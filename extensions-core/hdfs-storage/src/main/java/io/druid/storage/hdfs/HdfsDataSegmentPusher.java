/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.PropUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatters;
import io.druid.data.output.formatter.OrcFormatter;
import io.druid.query.ResultWriter;
import io.druid.query.TabularFormat;
import io.druid.query.select.EventHolder;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class HdfsDataSegmentPusher implements DataSegmentPusher, ResultWriter
{
  private static final Logger log = new Logger(HdfsDataSegmentPusher.class);

  private final HdfsDataSegmentPusherConfig config;
  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9 merger;

  @Inject
  public HdfsDataSegmentPusher(
      HdfsDataSegmentPusherConfig config,
      Configuration hadoopConfig,
      ObjectMapper jsonMapper,
      IndexMergerV9 merger
  )
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;
    this.merger = merger;

    log.info("Configured HDFS as deep storage");
  }

  @Override
  public String getPathForHadoop(String dataSource)
  {
    return new Path(config.getStorageDirectory()).toUri().toString();
  }

  @Override
  public DataSegment push(File inDir, DataSegment segment) throws IOException
  {
    final String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);

    log.info(
        "Copying segment[%s] to HDFS at location[%s/%s]",
        segment.getIdentifier(),
        config.getStorageDirectory(),
        storageDir
    );

    Path outFile = new Path(String.format("%s/%s/index.zip", config.getStorageDirectory(), storageDir));
    FileSystem fs = outFile.getFileSystem(hadoopConfig);

    fs.mkdirs(outFile.getParent());
    log.info("Compressing files from[%s] to [%s]", inDir, outFile);

    final long size;
    try (FSDataOutputStream out = fs.create(outFile)) {
      size = CompressionUtils.zip(inDir, out);
    }

    return createDescriptorFile(
        segment.withLoadSpec(makeLoadSpec(outFile))
               .withSize(size)
               .withBinaryVersion(SegmentUtils.getVersionFromDir(inDir)),
        outFile.getParent(),
        fs
    );
  }

  private DataSegment createDescriptorFile(DataSegment segment, Path outDir, final FileSystem fs) throws IOException
  {
    final Path descriptorFile = new Path(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    ByteSource
        .wrap(jsonMapper.writeValueAsBytes(segment))
        .copyTo(
            new ByteSink()
            {
              @Override
              public OutputStream openStream() throws IOException
              {
                return fs.create(descriptorFile);
              }
            }
        );
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(Path outFile)
  {
    return ImmutableMap.<String, Object>of("type", "hdfs", "path", outFile.toString());
  }

  @Override
  public Map<String, Object> write(URI location, TabularFormat result, Map<String, Object> context)
      throws IOException
  {
    log.info("Result will be forwarded to " + location + " with context " + context);
    Path parent = new Path(location);
    Path targetDirectory = parent;
    if (location.getScheme().equals(ResultWriter.FILE_SCHEME)) {
      targetDirectory = new Path(rewrite(location, null, -1));
    }
    FileSystem fileSystem = targetDirectory.getFileSystem(hadoopConfig);
    if (fileSystem instanceof LocalFileSystem) {
      // we don't need crc
      fileSystem = ((LocalFileSystem)fileSystem).getRawFileSystem();
    }

    boolean cleanup = PropUtils.parseBoolean(context, "cleanup", false);
    if (cleanup) {
      fileSystem.delete(targetDirectory, true);
    }
    if (fileSystem.isFile(targetDirectory)) {
      throw new IllegalStateException("'resultDirectory' should not be a file");
    }
    if (!fileSystem.exists(targetDirectory) && !fileSystem.mkdirs(targetDirectory)) {
      throw new IllegalStateException("failed to make target directory");
    }
    Map<String, Object> info = Maps.newLinkedHashMap();
    String format = Formatters.getFormat(context);
    CountingAccumulator exporter = toExporter(format, parent, context, fileSystem, targetDirectory);
    try {
      result.getSequence().accumulate(null, exporter.init());
    }
    finally {
      info.putAll(exporter.close());
    }

    if (format.equals("index") || !PropUtils.parseBoolean(context, "skipMetaFile", false)) {
      Map<String, Object> metaData = result.getMetaData();
      if (metaData != null && !metaData.isEmpty()) {
        Path metaFile = new Path(targetDirectory, PropUtils.parseString(context, "metaFileName", ".meta"));
        try (OutputStream output = fileSystem.create(metaFile)) {
          jsonMapper.writeValue(output, metaData);
        }
        Path metaLocation = new Path(parent, metaFile.getName());
        info.put("meta", ImmutableMap.of(metaLocation.toString(), fileSystem.getFileStatus(metaFile).getLen()));
      }
    }
    return info;
  }

  private URI rewrite(URI location, String host, int port)
  {
    try {
      return new URI(
          location.getScheme(),
          location.getUserInfo(),
          host,
          port,
          location.getPath(),
          location.getQuery(),
          location.getFragment()
      );
    }
    catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

  private CountingAccumulator toExporter(
      final String format,
      final Path parent,
      final Map<String, Object> context,
      final FileSystem fs,
      final Path targetDirectory
  )
      throws IOException
  {
    if ("index".equals(format)) {
      final String timestampColumn = PropUtils.parseString(context, "timestampColumn", EventHolder.timestampKey);
      final String dataSource = PropUtils.parseString(context, "dataSource", "___temporary_" + new DateTime());
      final long maxOccupation = PropUtils.parseLong(context, "maxOccupation", 256 << 20);
      final int maxRowCount = PropUtils.parseInt(context, "maxRowCount", 500000);
      final IncrementalIndexSchema schema = Preconditions.checkNotNull(
          jsonMapper.convertValue(context.get("schema"), IncrementalIndexSchema.class),
          "cannot find/create index schema"
      );
      final List<String> dimensions = schema.getDimensionsSpec().getDimensionNames();
      final List<String> metrics = schema.getMetricNames();

      final Path finalPath = new Path(targetDirectory, dataSource);
      fs.mkdirs(finalPath);

      final File temp = File.createTempFile("forward", "index");
      temp.delete();
      temp.mkdirs();

      return new CountingAccumulator() {

        private static final int OCCUPY_CHECK_INTERVAL = 5000;

        private int indexCount;
        private int rowCount;
        private IncrementalIndex index;
        private final List<File> files = Lists.newArrayList();

        @Override
        public CountingAccumulator init() throws IOException
        {
          index = newIndex();
          return this;
        }

        @Override
        public Void accumulate(Void accumulated, Map<String, Object> in)
        {
          rowCount++;
          try {
            if (isIndexFull()) {
              files.add(persist());
              index = newIndex();
            }
            Object timestamp = in.get(timestampColumn);
            if (timestamp instanceof DateTime) {
              index.add(new MapBasedRow((DateTime) timestamp, in));
            } else if (timestamp instanceof Long) {
              index.add(new MapBasedRow((Long) timestamp, in));
            } else {
              throw new IllegalStateException("null or invalid type timestamp column value " + timestamp);
            }
          }
          catch (Exception e) {
            Throwables.propagate(e);
          }
          return null;
        }

        private boolean isIndexFull()
        {
          return !index.canAppendRow() ||
                 rowCount % OCCUPY_CHECK_INTERVAL == 0 && index.estimatedOccupation() >= maxOccupation;
        }

        @Override
        public Map<String, Object> close() throws IOException
        {
          if (!index.isEmpty()) {
            files.add(persist());
          }
          File mergedBase;
          if (files.size() == 1) {
            mergedBase = files.get(0);
          } else {
            final List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(files.size());
            for (File file : files) {
              indexes.add(merger.getIndexIO().loadIndex(file));
            }
            mergedBase = merger.mergeQueryableIndexAndClose(
                indexes,
                schema.getMetrics(),
                new File(temp, "merged"),
                new IndexSpec(),
                new BaseProgressIndicator()
            );
          }
          int rowCount;
          Interval interval;
          try (QueryableIndex merged = merger.getIndexIO().loadIndex(mergedBase)) {
            rowCount = merged.getNumRows();
            interval = merged.getDataInterval();
          }

          String version = new DateTime().toString();
          int binaryVersion = SegmentUtils.getVersionFromDir(mergedBase);
          long length = FileUtils.sizeOfDirectory(mergedBase);

          for (File file : Preconditions.checkNotNull(mergedBase.listFiles())) {
            if (file.isFile() && !file.isHidden()) {
              fs.copyFromLocalFile(true, new Path("file://" + file.getAbsolutePath()), finalPath);
            }
          }
          FileUtils.deleteDirectory(mergedBase);

          Map<String, Object> loadSpec = toLoadSpec(finalPath.toUri());
          DataSegment segment = new DataSegment(
              dataSource,
              interval,
              version,
              loadSpec,
              dimensions,
              metrics,
              NoneShardSpec.instance(),
              binaryVersion,
              length
          );

          Map<String, Object> metaData = Maps.newLinkedHashMap();

          metaData.put("rowCount", rowCount);
          metaData.put(
              "data", ImmutableMap.of(
                  "location", new Path(parent, dataSource).toUri(),
                  "length", length,
                  "dataSource", dataSource,
                  "segment", segment
              )
          );

          return metaData;
        }

        private IncrementalIndex newIndex()
        {
          return new OnheapIncrementalIndex(schema, true, true, true, maxRowCount);
        }

        private File persist() throws IOException
        {
          return merger.persist(index, nextFile(), new IndexSpec());
        }

        private File nextFile()
        {
          return new File(temp, String.format("index-%,05d", indexCount++));
        }
      };
    }
    final Path dataFile = new Path(targetDirectory, PropUtils.parseString(context, "dataFileName", "data"));
    if ("orc".equals(format)) {
      String schema = Objects.toString(context.get("schema"), null);
      return Formatters.wrapToExporter(new OrcFormatter(dataFile, fs, schema, jsonMapper));
    }
    return Formatters.toBasicExporter(
        context, jsonMapper, new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return fs.create(dataFile);
          }

          @Override
          public String toString()
          {
            return new Path(parent, dataFile.getName()).toString();
          }
        }
    );
  }

  // copied from JobHelper.serializeOutIndex (in indexing-hadoop)
  private Map<String, Object> toLoadSpec(URI indexOutURI)
  {
    switch (indexOutURI.getScheme()) {
      case "hdfs":
      case "viewfs":
      case "gs":
        return ImmutableMap.<String, Object>of(
            "type", "hdfs",
            "path", indexOutURI.toString()
        );
      case "s3":
      case "s3n":
        return ImmutableMap.<String, Object>of(
            "type", "s3_zip",
            "bucket", indexOutURI.getHost(),
            "key", indexOutURI.getPath().substring(1) // remove the leading "/"
        );
      case "file":
        return ImmutableMap.<String, Object>of(
            "type", "local",
            "path", indexOutURI.getPath()
        );
      default:
        throw new IAE("Unknown file system scheme [%s]", indexOutURI.getScheme());
    }
  }
}
