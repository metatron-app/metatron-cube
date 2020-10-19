/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatter;
import io.druid.data.output.Formatters;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.HadoopInputUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryResult;
import io.druid.query.StorageHandler;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
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

public class HdfsStorageHandler implements StorageHandler
{
  private static final Logger LOG = new Logger(HdfsStorageHandler.class);

  private final Configuration hadoopConfig;
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9 merger;

  private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

  @Inject
  public HdfsStorageHandler(
      Configuration hadoopConfig,
      ObjectMapper jsonMapper,
      IndexMergerV9 merger
  )
  {
    this.hadoopConfig = hadoopConfig;
    this.jsonMapper = jsonMapper;
    this.merger = merger;
  }

  @Override
  public Sequence<Row> read(final List<URI> locations, final InputRowParser parser, final Map<String, Object> context)
      throws IOException, InterruptedException
  {
    hadoopConfig.setClassLoader(Thread.currentThread().getContextClassLoader());
    final Object inputFormat = context.get(INPUT_FORMAT);
    final Class<?> inputFormatClass;
    if (inputFormat instanceof Class) {
      inputFormatClass = (Class) inputFormat;
    } else if (inputFormat instanceof String) {
      inputFormatClass = hadoopConfig.getClassByNameOrNull((String) inputFormat);
    } else {
      inputFormatClass = null;
    }
    if (inputFormatClass != null &&
        (InputFormat.class.isAssignableFrom(inputFormatClass) ||
         org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormatClass))) {
      final InputFormat format;
      if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormatClass)) {
        format = new InputFormatWrapper(
            (org.apache.hadoop.mapred.InputFormat) ReflectionUtils.newInstance(inputFormatClass, hadoopConfig)
        );
      } else {
        format = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, hadoopConfig);
      }
      Job job = Job.getInstance(hadoopConfig);
      FileInputFormat.setInputPaths(
          job, StringUtils.concat(",", Iterables.transform(locations, Functions.toStringFunction()))
      );
      final RecordReader<Object, InputRow> reader = HadoopInputUtils.toRecordReader(format, parser, job);
      return Sequences.once(HadoopInputUtils.<Row>toIterator(reader));
    }

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
          final Iterator<Row> iterator;
          if (parser instanceof InputRowParser.Streaming && ((InputRowParser.Streaming) parser).accept(stream)) {
            iterator = ((InputRowParser.Streaming) parser).parseStream(stream);
          } else {
            final String encoding = Objects.toString(context.get(ENCODING), null);
            final boolean ignoreInvalidRows = PropUtils.parseBoolean(context, IGNORE_INVALID_ROWS);
            iterator = Iterators.transform(
                IOUtils.lineIterator(new InputStreamReader(stream, Charsets.toCharset(encoding))),
                InputRowParsers.asFunction(parser, ignoreInvalidRows)
            );
          }
          if (PropUtils.parseBoolean(context, EXTRACT_PARTITION)) {
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
    return Sequences.once(Sequences.concat(Iterators.transform(locations.iterator(), converter)));
  }

  @Override
  public Map<String, Object> write(URI location, QueryResult result, Map<String, Object> context)
      throws IOException
  {
    LOG.info("Result will be forwarded to [%s] with context %s", location, context);
    hadoopConfig.setClassLoader(Thread.currentThread().getContextClassLoader());
    Path nominalPath = new Path(location);
    Path physicalPath = nominalPath;
    if (StorageHandler.FILE_SCHEME.equals(location.getScheme())) {
      physicalPath = new Path(rewrite(location, null, -1));
    }
    FileSystem fileSystem = physicalPath.getFileSystem(hadoopConfig);
    if (fileSystem instanceof LocalFileSystem) {
      // we don't need crc
      fileSystem = ((LocalFileSystem) fileSystem).getRawFileSystem();
    }

    boolean cleanup = PropUtils.parseBoolean(context, CLEANUP, false);
    if (cleanup) {
      fileSystem.delete(physicalPath, true);
    }
    if (fileSystem.isFile(physicalPath)) {
      throw new IAE("target location [%s] should not be a file", physicalPath);
    }
    if (!fileSystem.exists(physicalPath) && !fileSystem.mkdirs(physicalPath)) {
      throw new IAE("failed to make target directory");
    }
    Map<String, Object> info = Maps.newLinkedHashMap();
    CountingAccumulator exporter = toExporter(result, context, nominalPath, physicalPath, fileSystem);
    try {
      result.getSequence().accumulate(null, exporter.init());
    }
    catch (Exception ex) {
      LOG.warn(ex, "failed");
      throw Throwables.propagate(ex);
    }
    finally {
      info.putAll(exporter.close());
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
      final QueryResult result,
      final Map<String, Object> context,
      final Path nominalPath,
      final Path physicalPath,
      final FileSystem fs
  )
      throws IOException
  {
    final String format = Formatters.getFormat(context);

    if (INDEX_FORMAT.equals(format)) {
      final long start = System.currentTimeMillis();
      final String timestampColumn = PropUtils.parseString(context, TIMESTAMP_COLUMN, Row.TIME_COLUMN_NAME);
      final String dataSource = Preconditions.checkNotNull(PropUtils.parseString(context, DATASOURCE));
      final IncrementalIndexSchema schema = Preconditions.checkNotNull(
          jsonMapper.convertValue(context.get(SCHEMA), IncrementalIndexSchema.class),
          "cannot find/create index schema"
      );
      final Granularity segmentGranularity = schema.getSegmentGran();
      final Interval queryInterval = jsonMapper.convertValue(context.get("interval"), Interval.class);
      final BaseTuningConfig tuning = jsonMapper.convertValue(context.get(TUNING_CONFIG), BaseTuningConfig.class);
      final List<String> dimensions = schema.getDimensionsSpec().getDimensionNames();
      final List<String> metrics = schema.getMetricNames();

      final Path targetPath = new Path(physicalPath, dataSource);
      fs.mkdirs(targetPath);

      final int maxRowCount = tuning == null ? 200000 : tuning.getMaxRowsInMemory();
      final long maxOccupation = tuning == null ? 128 << 20 : tuning.getMaxOccupationInMemory();
      final long maxShardLength = tuning == null ? maxOccupation : tuning.getMaxShardLength();
      final IndexSpec indexSpec = tuning == null ? IndexSpec.DEFAULT : tuning.getIndexSpec();

      final File temp = GuavaUtils.createTemporaryDirectory("forward-", "-index");

      return new CountingAccumulator()
      {
        private static final int OCCUPY_CHECK_INTERVAL = 5000;

        private int indexCount;
        private int rowCount;
        private IncrementalIndex index;
        private final List<File> files = Lists.newArrayList();
        private final String version = new DateTime().toString();

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
            final Object timestamp = in.get(timestampColumn);
            if (timestamp instanceof DateTime) {
              index.add(new MapBasedRow((DateTime) timestamp, in));
            } else if (timestamp instanceof Number) {
              index.add(new MapBasedRow(((Number) timestamp).longValue(), in));
            } else {
              throw new ISE("null or invalid type timestamp column [%s] value [%s]", timestampColumn, timestamp);
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
          List<DataSegment> segments = Lists.newArrayList();
          if (files.size() == 1 && GuavaUtils.isNullOrEmpty(indexSpec.getSecondaryIndexing())) {
            segments.add(finalizeIndex(files.get(0), targetPath, NoneShardSpec.instance()));
          } else {
            int shardNum = 0;
            long queueSize = 0;
            final List<File> queue = Lists.newArrayList();
            for (File file : files) {
              long size = FileUtils.sizeOfDirectory(file);
              if (!queue.isEmpty() && queueSize + size > maxShardLength) {
                segments.add(mergeSegments(queue, shardNum++));
                queueSize = 0;
                queue.clear();
              }
              queueSize += size;
              queue.add(file);
            }
            if (!queue.isEmpty()) {
              segments.add(mergeSegments(queue, shardNum));
              queue.clear();
            }
          }
          int totalRowCount = 0;
          long totalLength = 0;
          List<Map<String, Object>> segmentMetas = Lists.newArrayList();
          for (DataSegment segment : segments) {
            Path segmentPath = new Path(targetPath, String.valueOf(segment.getShardSpec().getPartitionNum()));
            Map<String, Object> segmentMeta = ImmutableMap.of(
                "location", segmentPath.toUri(),
                "length", segment.getSize(),
                "segment", segment
            );
            segmentMetas.add(segmentMeta);
            totalRowCount += segment.getNumRows();
            totalLength += segment.getSize();
          }
          Map<String, Object> metaData = Maps.newLinkedHashMap();
          metaData.put("dataSource", dataSource);
          metaData.put("rowCount", rowCount);
          metaData.put("indexedRowCount", totalRowCount);
          metaData.put("indexedLength", totalLength);
          metaData.put("numSegments", segmentMetas.size());
          metaData.put("data", segmentMetas);

          LOG.info("Took %,d msec to load %s", (System.currentTimeMillis() - start), dataSource);
          return metaData;
        }

        private IncrementalIndex newIndex()
        {
          return new OnheapIncrementalIndex(schema, true, true, false, maxRowCount);
        }

        private Interval getSegmentInterval(IncrementalIndex index)
        {
          Interval timeMinMax = Preconditions.checkNotNull(index.getTimeMinMax(), "empty index");
          LOG.info("Interval of data [%s]", timeMinMax);
          Granularity granularity = segmentGranularity;
          if (granularity == null) {
            granularity = coveringGranularity(timeMinMax);
          }
          Interval interval;
          if (granularity == Granularities.ALL && queryInterval != null) {
            interval = queryInterval;
          } else {
            interval = Intervals.of(
                granularity.bucketStart(timeMinMax.getStart()),
                granularity.bucketEnd(timeMinMax.getEnd())
            );
          }
          LOG.info("Using segment interval [%s]", interval);
          return interval;
        }

        private Granularity coveringGranularity(Interval dataInterval)
        {
          for (Granularity granularity : Arrays.asList(
              Granularities.HOUR,
              Granularities.DAY,
              Granularities.MONTH,
              Granularities.QUARTER,
              Granularities.YEAR
          )) {
            if (Iterables.size(Iterables.limit(granularity.getIterable(dataInterval), 2)) == 1) {
              return granularity;
            }
          }
          return Granularities.ALL;
        }

        private File persist() throws IOException
        {
          LOG.info(
              "Flushing %,d rows with estimated size %,d bytes.. Heap usage %s",
              index.size(), index.estimatedOccupation(), memoryMXBean.getHeapMemoryUsage()
          );
          return merger.persist(index, getSegmentInterval(index), nextFile(), indexSpec.asIntermediarySpec());
        }

        private File nextFile()
        {
          return new File(temp, String.format("temp-%,05d", indexCount++));
        }

        private DataSegment mergeSegments(List<File> queue, int shardNum) throws IOException
        {
          Path finalPath = new Path(targetPath, String.valueOf(shardNum));
          fs.mkdirs(finalPath);

          File tempPath;
          if (queue.size() == 1 && GuavaUtils.isNullOrEmpty(indexSpec.getSecondaryIndexing())) {
            tempPath = queue.get(0);
          } else {
            if (queue.size() == 1) {
              LOG.info("Building seconday index & write to [%s]", finalPath);
            } else {
              LOG.info("Merging %d indices into [%s]", queue.size(), finalPath);
            }
            List<QueryableIndex> indices = Lists.newArrayList();
            for (File path : queue) {
              indices.add(merger.getIndexIO().loadIndex(path));
            }
            tempPath = merger.mergeQueryableIndexAndClose(
                indices,
                schema.isRollup(),
                schema.getMetrics(),
                new File(temp, String.valueOf(shardNum)),
                indexSpec,
                new BaseProgressIndicator()
            );
          }
          return finalizeIndex(tempPath, finalPath, LinearShardSpec.of(shardNum));
        }

        private DataSegment finalizeIndex(File tempPath, Path finalPath, ShardSpec shardSpec) throws IOException
        {
          int rowCount;
          Interval interval;
          try (QueryableIndex merged = merger.getIndexIO().loadIndex(tempPath, true)) {
            rowCount = merged.getNumRows();
            interval = merged.getInterval();
          }

          int binaryVersion = SegmentUtils.getVersionFromDir(tempPath);
          long length = FileUtils.sizeOfDirectory(tempPath);

          moveTo(tempPath, finalPath);

          final Map<String, Object> loadSpec = JobHelper.makeLoadSpec(finalPath.toUri());
          return new DataSegment(
              dataSource,
              interval,
              version,
              loadSpec,
              dimensions,
              metrics,
              shardSpec,
              binaryVersion,
              length,
              rowCount
          );
        }

        private void moveTo(File tempPath, Path finalPath) throws IOException
        {
          for (File file : Preconditions.checkNotNull(tempPath.listFiles())) {
            if (file.isFile() && !file.isHidden()) {
              fs.copyFromLocalFile(true, new Path("file://" + file.getAbsolutePath()), finalPath);
            }
          }
          FileUtils.deleteDirectory(tempPath);
        }
      };
    }
    final Path dataFile = new Path(physicalPath, PropUtils.parseString(context, DATA_FILENAME, "data"));
    final CountingAccumulator exporter = Formatters.toBasicExporter(
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
            return new Path(nominalPath, dataFile.getName()).toString();
          }
        }
    );
    if (exporter != null) {
      return exporter;
    }
    final Map<String, Object> parameter = Maps.newHashMap();
    parameter.put("format", format);
    parameter.put("outputPath", dataFile.toString());
    parameter.put("inputColumns", result.getInputColumns());
    parameter.put("typeString", result.getTypeString());
    parameter.put("geomColumn", context.get("geomColumn"));     // todo
    final Formatter formatter = jsonMapper.convertValue(parameter, Formatter.class);
    if (formatter != null) {
      return Formatters.wrapToExporter(formatter);
    }
    throw new IAE("Cannot find writer of format '%s'", format);
  }
}
