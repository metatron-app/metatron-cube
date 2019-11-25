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

package io.druid.indexer;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.CompressionUtils;
import io.druid.data.input.InputRow;
import io.druid.indexer.path.HadoopCombineInputFormat;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.AppendingGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 */
public class ReduceMergeIndexGeneratorJob implements HadoopDruidIndexerJob.IndexingStatsProvider
{
  private static final Logger log = new Logger(ReduceMergeIndexGeneratorJob.class);
  private static final int INCREMENTAL_INDEX_OVERHEAD = 64 << 10;
  private static final int MAX_LOG_INTERVAL = 10_0000;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB (from JobHelper)

  private final HadoopDruidIndexerConfig config;
  private final IndexGeneratorStats jobStats;

  public ReduceMergeIndexGeneratorJob(HadoopDruidIndexerConfig config)
  {
    this.config = config;
    this.jobStats = new IndexGeneratorStats();
  }

  public IndexGeneratorStats getJobStats()
  {
    return jobStats;
  }

  public boolean run()
  {
    log.info("Running ReduceMergeIndexGeneratorJob.. %s %s", config.getDataSource(), config.getIntervals());
    Configuration conf = new Configuration();
    try {
      Job job = Job.getInstance(
          conf,
          String.format("%s-index-generator-reducer-merge-%s", config.getDataSource(), config.getIntervals().get())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);
      config.addJobProperties(job);

      job.setMapperClass(ReducerMergingMapper.class);
      job.setReducerClass(ReducerMergingReducer.class);

      int numReducers = config.getNumReducer(-1);
      if (numReducers == 0) {
        throw new RuntimeException("No buckets?? seems there is no data to index.");
      }
      job.setNumReduceTasks(numReducers);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(IndexGeneratorJob.IndexGeneratorOutputFormat.class);
      FileOutputFormat.setOutputPath(job, config.makeIntermediatePath());

      config.addInputPaths(job);

      config.intoConfiguration(job);

      JobHelper.setupClasspath(
          JobHelper.distributedClassPath(config.getWorkingPath()),
          JobHelper.distributedClassPath(config.makeIntermediatePath()),
          job
      );

      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());

      boolean success = job.waitForCompletion(true);

      jobStats.setStats(job.getCounters());

      return success;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class ReducerMergingMapper extends HadoopDruidIndexerMapper<Text, Text>
  {
    private HadoopTuningConfig tuningConfig;

    private IndexMerger merger;

    private AggregatorFactory[] aggregators;
    private Counter flushedIndex;

    private int scatterParam;
    private Random random = new Random(System.nanoTime());

    private long maxOccupation;
    private int maxRowCount;
    private int occupationCheckInterval = 50000;

    private File baseFlushFile;
    private Path shufflingPath;
    private FileSystem shufflingFS;

    private MemoryMXBean memoryMXBean;
    private ProgressIndicator progressIndicator;

    private boolean dynamicDataSource;

    private int indexCount;
    private List<IntervalIndex> indices = Lists.newLinkedList();

    private String currentDataSource;
    private int nextLogging = 1;
    private int lineCount;

    @Override
    public void run(Context context) throws IOException, InterruptedException
    {
      setup(context);
      try {
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        persistAll(context);
      }
      finally {
        cleanup(context);
      }
    }

    @Override
    protected void setup(final Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      HadoopIngestionSpec schema = config.getSchema();
      DataSchema dataSchema = schema.getDataSchema();
      tuningConfig = schema.getTuningConfig();

      aggregators = dataSchema.getAggregators();

      dynamicDataSource = "hadoop".equals(schema.getIOConfig().getPathSpec().get("type"));
      if (!dynamicDataSource) {
        currentDataSource = dataSchema.getDataSource();
      }

      flushedIndex = context.getCounter("druid.internal", "index-flush-count");

      maxOccupation = tuningConfig.getMaxOccupationInMemory();
      maxRowCount = tuningConfig.getMaxRowsInMemory();

      scatterParam = Math.min(tuningConfig.getScatterParam(), context.getNumReduceTasks());

      merger = config.isBuildV9Directly()
               ? HadoopDruidIndexerConfig.INDEX_MERGER_V9
               : HadoopDruidIndexerConfig.INDEX_MERGER;

      Path path = config.makeShufflingDir(String.valueOf(context.getTaskAttemptID().getTaskID().getId()));

      shufflingFS = path.getFileSystem(context.getConfiguration());
      shufflingPath = JobHelper.prependFSIfNullScheme(shufflingFS, path);

      shufflingFS.mkdirs(shufflingPath);

      baseFlushFile = GuavaUtils.createTemporaryDirectory("base", "flush");

      progressIndicator = new BaseProgressIndicator()
      {
        @Override
        public void progress()
        {
          super.progress();
          context.progress();
        }
      };

      memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    @Override
    protected void innerMap(InputRow row, Context context)
        throws IOException, InterruptedException
    {
      // not null only with HadoopCombineInputFormat
      final String dataSource = dynamicDataSource ? HadoopCombineInputFormat.CURRENT_DATASOURCE.get() : currentDataSource;
      if (dynamicDataSource && !Objects.equals(currentDataSource, dataSource) && !indices.isEmpty()) {
        persistAll(context);
      }

      IntervalIndex target = findIndex(row.getTimestampFromEpoch());
      if (target != null) {
        final IncrementalIndex index = target.index();
        boolean flush = !index.canAppendRow();
        if (lineCount > 0 && lineCount % occupationCheckInterval == 0) {
          int rows = totalRows();
          long estimation = totalEstimation();
          log.info("... %,d rows in %d indices with estimated size %,d bytes", rows, indices.size(), estimation);
          if (flush) {
            log.info("Flushing index because row count in index exceeding maxRowsInMemory %,d", maxRowCount);
          } else if (maxOccupation > 0 && estimation >= maxOccupation) {
            log.info("Flushing index because estimated occupation is bigger than maxOccupation %,d B", maxOccupation);
            flush = true;
          }
        }
        if (flush) {
          log.info("Heap memory usage from mbean %s", memoryMXBean.getHeapMemoryUsage());
          target = persistOneIndex(context, target);
        }
      }
      if (target == null) {
        currentDataSource = dataSource;
        String currentDataSource = currentDataSource();
        Interval interval = getConfig().getTargetInterval(row).get();
        IncrementalIndex index = makeIncrementalIndex(interval);
        target = new IntervalIndex(currentDataSource, interval, index);
        indices.add(target);
        nextLogging = 1;
        log.info("Starting new index %s [%s]", currentDataSource, interval);
      }
      target.addRow(row);

      if (++lineCount % nextLogging == 0) {
        log.info("processing %,d lines..", lineCount);
        nextLogging = Math.min(nextLogging * 10, MAX_LOG_INTERVAL);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
      try {
        super.cleanup(context);
      }
      finally {
        FileUtils.deleteDirectory(baseFlushFile);
      }
    }

    private IncrementalIndex makeIncrementalIndex(Interval interval)
    {
      final HadoopIngestionSpec schema = config.getSchema();
      final GranularitySpec granularitySpec = schema.getDataSchema().getGranularitySpec();
      final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withMinTimestamp(interval.getStartMillis())
          .withDimensionsSpec(schema.getParser())
          .withDimensionFixed(schema.isDimensionFixed())
          .withQueryGranularity(granularitySpec.getQueryGranularity())
          .withSegmentGranularity(granularitySpec.getSegmentGranularity())
          .withMetrics(aggregators)
          .withRollup(granularitySpec.isRollup())
          .withNoQuery(true)
          .build();

      return new OnheapIncrementalIndex(
          indexSchema,
          true,
          !tuningConfig.isIgnoreInvalidRows(),
          true,
          maxRowCount
      );
    }

    private IntervalIndex findIndex(long timestamp)
    {
      for (IntervalIndex entry : indices) {
        if (entry.rhs.contains(timestamp)) {
          return entry;
        }
      }
      return null;
    }

    private int totalRows()
    {
      int total = 0;
      for (IntervalIndex entry : indices) {
        total += entry.index().size();
      }
      return total;
    }

    private long totalEstimation()
    {
      long total = 0;
      for (IntervalIndex entry : indices) {
        total += entry.index().estimatedOccupation();
        total += INCREMENTAL_INDEX_OVERHEAD;
      }
      return total;
    }

    private void persistAll(Context context) throws IOException, InterruptedException
    {
      for (IntervalIndex entry : indices) {
        persist(context, entry);
      }
      indices.clear();
    }

    private IntervalIndex persistOneIndex(Context context, IntervalIndex current)
        throws IOException, InterruptedException
    {
      IntervalIndex found = selectTarget(current);
      persist(context, found);
      return found == current ? null : current;
    }

    private IntervalIndex selectTarget(IntervalIndex current)
    {
      if (!current.index().canAppendRow()) {
        indices.remove(current);
        return current;
      }
      long leastAccessTime = -1;
      IntervalIndex found = null;
      for (IntervalIndex entry : indices) {
        if (entry == current) {
          continue;
        }
        if (found == null || entry.lastAccessTime < leastAccessTime) {
          leastAccessTime = entry.lastAccessTime;
          found = entry;
        }
      }
      if (System.currentTimeMillis() - leastAccessTime < 30 * 1000) {
        int maxRows = -1;
        for (IntervalIndex entry : indices) {
          if (entry == current) {
            continue;
          }
          int size = entry.index().size();
          if (maxRows < 0 || size > maxRows) {
            maxRows = size;
            found = entry;
          }
        }
      }
      IntervalIndex target = found == null ? current : found;
      int rows = target.index().size();
      log.info(
          "Flushing index of %s (%s) which has %d rows and was accessed %,d msec before",
          target.dataSource(), target.interval(), rows, System.currentTimeMillis() - target.lastAccessTime
      );
      indices.remove(target);
      return target;
    }

    private void persist(Context context, IntervalIndex entry)
        throws IOException, InterruptedException
    {
      Interval interval = entry.interval();
      IncrementalIndex index = entry.index();
      context.progress();

      final String dataSource = currentDataSource();
      log.info(
          "Flushing index of %s (%s).. %,d rows with estimated size %,d bytes accumulated during %,d msec",
          dataSource, interval, index.size(), index.estimatedOccupation(), entry.elapsed()
      );

      long prev = System.currentTimeMillis();
      File localFile = merger.persist(
          index,
          interval,
          nextFile(),
          config.getIndexSpec().withSecondaryIndexing(null),
          progressIndicator
      );

      final Path outFile = new Path(shufflingPath, localFile.getName() + ".zip");
      shufflingFS.mkdirs(outFile.getParent());

      log.info("Persisting local index in [%s] to temp storage [%s]", localFile, outFile);
      final long size;
      try (FSDataOutputStream out = shufflingFS.create(outFile, true, DEFAULT_FS_BUFFER_SIZE)) {
        size = CompressionUtils.store(localFile, out, DEFAULT_FS_BUFFER_SIZE);
      }
      log.info("Persisted [%,d] bytes.. elapsed %,d msec", size, System.currentTimeMillis() - prev);
      int scatterKey = 0;
      if (scatterParam > 1) {
        scatterKey = random.nextInt(scatterParam);
      }
      Text key = new Text(dataSource + ":" + interval.getStartMillis() + ":" + scatterKey);

      log.info(".. writing reduce key [%s]", key);
      context.write(key, new Text(outFile.toString()));

      FileUtils.deleteDirectory(localFile);

      flushedIndex.increment(1);
    }

    private File nextFile()
    {
      return new File(baseFlushFile, String.format("index-%,05d", indexCount++));
    }

    private String currentDataSource()
    {
      return currentDataSource != null ? currentDataSource : config.getDataSource();
    }
  }

  private static class IndexKey extends Pair<String, Interval>
  {
    public IndexKey(String dataSource, Interval interval)
    {
      super(dataSource, interval);
    }

    String dataSource() { return lhs; }

    Interval interval() { return rhs; }
  }

  private static class IntervalIndex extends IndexKey
  {
    private final IncrementalIndex index;
    private final long startTime = System.currentTimeMillis();

    private long lastAccessTime = -1;

    public IntervalIndex(String dataSource, Interval interval, IncrementalIndex index)
    {
      super(dataSource, interval);
      this.index = index;
    }

    IndexKey toKey() { return new IndexKey(dataSource(), interval()); }

    IncrementalIndex index() { return index; }

    long elapsed() { return System.currentTimeMillis() - startTime; }

    void addRow(InputRow row) throws IndexSizeExceededException
    {
      index.add(row);
      lastAccessTime = System.currentTimeMillis();
    }
  }

  public static class ReducerMergingReducer extends Reducer<Text, Text, BytesWritable, Text>
  {
    private HadoopDruidIndexerConfig config;
    private HadoopTuningConfig tuningConfig;

    private IndexMerger merger;
    private List<String> metricNames = Lists.newArrayList();

    private AggregatorFactory[] aggregators;

    private long maxShardLength;
    private int scatterParam;

    private FileSystem shufflingFS;

    private FileSystem outputFS;
    private File baseFlushFile;

    private ProgressIndicator progressIndicator;
    private int indexCount;

    private long startTime;

    @Override
    protected void setup(final Context context) throws IOException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      tuningConfig = config.getSchema().getTuningConfig();

      aggregators = config.getSchema().getDataSchema().getAggregators();
      for (AggregatorFactory aggregator : aggregators) {
        metricNames.add(aggregator.getName());
      }

      maxShardLength = tuningConfig.getMaxShardLength();
      scatterParam = Math.min(tuningConfig.getScatterParam(), context.getNumReduceTasks());

      merger = config.isBuildV9Directly()
               ? HadoopDruidIndexerConfig.INDEX_MERGER_V9
               : HadoopDruidIndexerConfig.INDEX_MERGER;

      // this is per datasource
      outputFS = new Path(config.getSchema().getIOConfig().getSegmentOutputPath())
          .getFileSystem(context.getConfiguration());

      baseFlushFile = GuavaUtils.createTemporaryDirectory("base", "flush");

      shufflingFS = config.makeShuffleDir().getFileSystem(context.getConfiguration());

      progressIndicator = new BaseProgressIndicator()
      {
        @Override
        public void progress()
        {
          super.progress();
          context.progress();
        }
      };

      startTime = System.currentTimeMillis();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException
    {
      log.info("reduce key [%s]", key);
      String[] split = key.toString().split(":");

      final String dataSource = split[0];
      final DateTime time = new DateTime(Long.valueOf(split[1]));
      final int scatterKey = Integer.valueOf(split[2]);

      final GranularitySpec granularitySpec = config.getGranularitySpec();
      final Interval interval = granularitySpec.bucketInterval(time).get();

      String version = tuningConfig.getVersion();
      IndexSpec indexSpec = tuningConfig.getIndexSpec();
      LinearShardSpec appendingSpec = LinearShardSpec.of(0);
      if (granularitySpec instanceof AppendingGranularitySpec) {
        AppendingGranularitySpec appending = (AppendingGranularitySpec) granularitySpec;
        SegmentDescriptor descriptor = appending.getSegmentDescriptor(time.getMillis());
        if (descriptor != null) {
          appendingSpec = LinearShardSpec.of(descriptor.getPartitionNumber());
          version = descriptor.getVersion();
        }
      }
      final List<String> files = Lists.newArrayList(Iterables.transform(values, Functions.toStringFunction()));
      final List<List<File>> groups = groupToShards(files, maxShardLength);

      log.info("Merging %d segments of %s [%s] into %d shards", files.size(), dataSource, interval, groups.size());

      final boolean singleShard = groups.size() == 1;

      for (int i = 0; i < groups.size(); i++) {
        Set<String> dimensions = Sets.newLinkedHashSet();
        List<File> shard = groups.get(i);
        File mergedBase;
        if (shard.size() == 1 && GuavaUtils.isNullOrEmpty(indexSpec.getSecondaryIndexing())) {
          mergedBase = shard.get(0);
          QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(mergedBase);
          dimensions.addAll(Lists.newArrayList(index.getAvailableDimensions()));
          index.close();
        } else {
          final List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(shard.size());
          for (File file : shard) {
            QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(file);
            dimensions.addAll(Lists.newArrayList(index.getAvailableDimensions()));
            indexes.add(index);
          }
          mergedBase = merger.mergeQueryableIndexAndClose(
              indexes,
              granularitySpec.isRollup(),
              aggregators,
              new File(baseFlushFile, singleShard ? "single" : "shard-" + i),
              config.getIndexSpec(),
              progressIndicator
          );
        }
        int increment = scatterParam > 1 ? (i * scatterParam + scatterKey) : i;
        ShardSpec shardSpec = LinearShardSpec.of(appendingSpec.getPartitionNum() + increment);
        writeShard(dataSource, mergedBase, version, interval, Lists.newArrayList(dimensions), shardSpec, context);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
      log.info("Completed in %,d msec", System.currentTimeMillis() - startTime);
      FileUtils.deleteDirectory(baseFlushFile);
    }

    private List<List<File>> groupToShards(List<String> shuffles, long limit) throws IOException
    {
      List<List<File>> groups = Lists.newArrayList();

      long current = 0;
      List<File> group = Lists.newArrayList();
      for (String shuffle : shuffles) {
        File local = new File(baseFlushFile, String.format("index-%,05d", indexCount++));
        local.mkdirs();
        log.info("Uncompressing files from [%s] to [%s]", shuffle, local);
        long length = CompressionUtils.unzip(shufflingFS.open(new Path(shuffle)), local, DEFAULT_FS_BUFFER_SIZE);
        log.info("Uncompressed into [%,d] bytes", length);
        if (!group.isEmpty() && current + length > limit) {
          log.info("group-%d : %s", groups.size(), group);
          groups.add(group);
          group = Lists.newArrayList();
          current = 0;
        }
        group.add(local);
        current += length;
      }
      if (!group.isEmpty()) {
        log.info("group-%d : %s", groups.size(), group);
        groups.add(group);
      }
      return groups;
    }

    private void writeShard(
        String dataSource,
        File directory,
        String version,
        Interval interval,
        List<String> dimensions,
        ShardSpec shardSpec,
        Context context
    )
        throws IOException
    {
      final DataSegment segmentTemplate = new DataSegment(
          dataSource,
          interval,
          version,
          null,
          dimensions,
          metricNames,
          shardSpec,
          -1,
          -1
      );

      Path basePath = new Path(config.getSchema().getIOConfig().getSegmentOutputPath());
      final Path segmentBasePath = JobHelper.makeSegmentOutputPath(
          basePath,
          outputFS,
          segmentTemplate
      );

      log.info("Zipping shard [%s] to path [%s]", shardSpec, segmentBasePath);
      final DataSegment segment = JobHelper.serializeOutIndex(
          segmentTemplate,
          context.getConfiguration(),
          context,
          context.getTaskAttemptID(),
          directory,
          segmentBasePath
      );

      Path descriptorPath = config.makeDescriptorInfoPath(segment);
      descriptorPath = JobHelper.prependFSIfNullScheme(
          FileSystem.get(
              descriptorPath.toUri(),
              context.getConfiguration()
          ), descriptorPath
      );

      log.info("Writing descriptor to path [%s]", descriptorPath);
      JobHelper.writeSegmentDescriptor(
          config.makeDescriptorInfoDir().getFileSystem(context.getConfiguration()),
          segment,
          descriptorPath,
          context
      );
    }
  }
}
