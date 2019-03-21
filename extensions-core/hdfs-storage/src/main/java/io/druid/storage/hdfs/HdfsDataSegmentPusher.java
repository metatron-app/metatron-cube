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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatters;
import io.druid.data.output.formatter.OrcFormatter;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.ResultWriter;
import io.druid.query.TabularFormat;
import io.druid.query.select.EventHolder;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
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

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

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
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public String getPathForHadoop()
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
  public Sequence<Row> read(final List<URI> locations, final InputRowParser parser, final Map<String, Object> context)
      throws IOException
  {
    long total = 0;
    final float[] thresholds = new float[locations.size() + 1];
    for (int i = 0; i < locations.size(); i++) {
      thresholds[i] = total;
      Path path = new Path(locations.get(i));
      FileSystem fileSystem = path.getFileSystem(hadoopConfig);
      total += fileSystem.getFileStatus(path).getLen();
    }
    thresholds[locations.size()] = total;

    final Function<URI, Iterator<Row>> converter = new Function<URI, Iterator<Row>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Iterator<Row> apply(URI input)
      {
        final int index = locations.indexOf(input);
        try {
          final Path path = new Path(input);
          final FileSystem fileSystem = path.getFileSystem(hadoopConfig);
          final FSDataInputStream stream = fileSystem.open(path);
          Iterator<Row> iterator;
          if (parser instanceof InputRowParser.Streaming && ((InputRowParser.Streaming) parser).accept(stream)) {
            iterator = ((InputRowParser.Streaming) parser).parseStream(stream);
          } else {
            final String encoding = Objects.toString(context.get("encoding"), null);
            final boolean ignoreInvalidRows = PropUtils.parseBoolean(context, "ignoreInvalidRows");
            iterator = Iterators.transform(
                IOUtils.lineIterator(new InputStreamReader(stream, Charsets.toCharset(encoding))),
                InputRowParsers.asFunction(parser, ignoreInvalidRows)
            );
          }
          if (PropUtils.parseBoolean(context, "extractPartition")) {
            Rows.setPartition(new File(input));
          }
          return new GuavaUtils.DelegatedProgressing<Row>(GuavaUtils.withResource(iterator, stream))
          {
            @Override
            public float progress()
            {
              try {
                return (thresholds[index] - stream.available()) / thresholds[thresholds.length - 1];
              }
              catch (IOException e) {
                return thresholds[index] / thresholds[thresholds.length - 1];
              }
            }
          };
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
    return Sequences.once(GuavaUtils.concat(Iterators.transform(locations.iterator(), converter)));
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
      fileSystem = ((LocalFileSystem) fileSystem).getRawFileSystem();
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
      final long start = System.currentTimeMillis();
      final String timestampColumn = PropUtils.parseString(context, "timestampColumn", EventHolder.timestampKey);
      final String dataSource = PropUtils.parseString(context, "dataSource", "___temporary_" + new DateTime());
      final IncrementalIndexSchema schema = Preconditions.checkNotNull(
          jsonMapper.convertValue(context.get("schema"), IncrementalIndexSchema.class),
          "cannot find/create index schema"
      );
      final Granularity segmentGranularity = (Granularity) context.get("segmentGranularity");
      final Interval queryInterval = jsonMapper.convertValue(context.get("interval"), Interval.class);
      final BaseTuningConfig tuning = jsonMapper.convertValue(context.get("tuningConfig"), BaseTuningConfig.class);
      final List<String> dimensions = schema.getDimensionsSpec().getDimensionNames();
      final List<String> metrics = schema.getMetricNames();

      final Path finalPath = new Path(targetDirectory, dataSource);
      fs.mkdirs(finalPath);

      final File temp = File.createTempFile("forward", "index");
      temp.delete();
      temp.mkdirs();

      final int maxRowCount = tuning == null ? 500000 : tuning.getMaxRowsInMemory();
      final long maxOccupation = tuning == null ? 256 << 20 : tuning.getMaxOccupationInMemory();
      final IndexSpec indexSpec = tuning == null ? new IndexSpec() : tuning.getIndexSpec();

      return new CountingAccumulator()
      {
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
            throw Throwables.propagate(e);
          }
          return null;
        }

        private boolean isIndexFull()
        {
          return !index.canAppendRow() ||
                 maxOccupation > 0
                 && rowCount % OCCUPY_CHECK_INTERVAL == 0
                 && index.estimatedOccupation() >= maxOccupation;
        }

        @Override
        public Map<String, Object> close() throws IOException
        {
          if (index != null && !index.isEmpty()) {
            files.add(persist());
          }
          if (files.isEmpty()) {
            return ImmutableMap.<String, Object>of("rowCount", rowCount);
          }
          File mergedBase;
          if (files.size() == 1 && GuavaUtils.isNullOrEmpty(indexSpec.getSecondaryIndexing())) {
            mergedBase = files.get(0);
          } else {
            final List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(files.size());
            for (File file : files) {
              indexes.add(merger.getIndexIO().loadIndex(file));
            }
            log.info("Merging %d indices into one", indexes.size());
            File merge = new File(temp, "merged");
            mergedBase = merger.mergeQueryableIndexAndClose(
                indexes,
                schema.isRollup(),
                schema.getMetrics(),
                merge,
                indexSpec,
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

          // todo support multi-shard
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

          log.info("Took %,d msec to load %s", (System.currentTimeMillis() - start), dataSource);
          return metaData;
        }

        private IncrementalIndex newIndex()
        {
          return new OnheapIncrementalIndex(schema, true, true, true, false, maxRowCount)
          {
            @Override
            public Interval getInterval()
            {
              Interval dataInterval = new Interval(getMinTimeMillis(), getMaxTimeMillis());
              log.info("Interval of data [%s]", dataInterval);
              if (dataInterval.toPeriod().getMillis() == 0 && queryInterval != null) {
                dataInterval = queryInterval;
              }
              Granularity granularity =
                  segmentGranularity != null ? segmentGranularity : coveringGranularity(dataInterval);

              Interval interval = new Interval(
                  granularity.bucketStart(index.getMinTime()),
                  granularity.bucketEnd(index.getMaxTime())
              );
              log.info("Using segment interval [%s]", interval);
              return interval;
            }
          };
        }

        private Granularity coveringGranularity(Interval dataInterval)
        {
          for (Granularity granularity : Arrays.asList(
              QueryGranularities.HOUR,
              QueryGranularities.DAY,
              QueryGranularities.MONTH
          )) {
            if (Iterables.size(granularity.getIterable(dataInterval)) <= 1) {
              return granularity;
            }
          }
          return QueryGranularities.YEAR;
        }

        private File persist() throws IOException
        {
          log.info(
              "Flushing %,d rows with estimated size %,d bytes.. Heap usage %s",
              index.size(), index.estimatedOccupation(), memoryMXBean.getHeapMemoryUsage()
          );
          return merger.persist(index, nextFile(), indexSpec.withoutSecondaryIndexing());
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
