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

package io.druid.indexer;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.CompressionUtils;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.indexer.path.HynixCombineInputFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMerger;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
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
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class ReduceMergeIndexGeneratorJob implements HadoopDruidIndexerJob.IndexingStatsProvider
{
  private static final Logger log = new Logger(ReduceMergeIndexGeneratorJob.class);

  private final HadoopDruidIndexerConfig config;
  private final IndexGeneratorJob.IndexGeneratorStats jobStats;

  public ReduceMergeIndexGeneratorJob(HadoopDruidIndexerConfig config)
  {
    this.config = config;
    this.jobStats = new IndexGeneratorJob.IndexGeneratorStats();
  }

  public IndexGeneratorJob.IndexGeneratorStats getJobStats()
  {
    return jobStats;
  }

  public boolean run()
  {
    Configuration conf = new Configuration();
    try {
      Job job = Job.getInstance(
          conf,
          String.format("%s-index-generator-reducer-merge-%s", config.getDataSource(), config.getIntervals().get()));

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);
      config.addJobProperties(job);

      job.setMapperClass(ReducerMergingMapper.class);
      job.setReducerClass(ReducerMergingReducer.class);

      int numReducers = Iterables.size(config.getAllBuckets().get());
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

      Counter invalidRowCount = job.getCounters()
                                   .findCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER);
      jobStats.setInvalidRowCount(invalidRowCount.getValue());

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
    private List<String> metricNames = Lists.newArrayList();

    private AggregatorFactory[] aggregators;
    private Counter flushedIndex;

    private long maxOccupation;
    private int maxRowCount;
    private int occupationCheckInterval = 2000;

    private File baseFlushFile;
    private Path shufflingPath;
    private FileSystem shufflingFS;

    private ProgressIndicator progressIndicator;

    private int indexCount;
    private String currentDataSource;
    private Interval interval;
    private IncrementalIndex index;

    private int nextLogging = 1;
    private int lineCount;

    private long startTime;

    @Override
    public void run(Context context) throws IOException, InterruptedException
    {
      setup(context);
      try {
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
      }
      catch (Exception e) {
        log.warn(e, "Failed.. ");
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
      tuningConfig = config.getSchema().getTuningConfig();

      aggregators = config.getSchema().getDataSchema().getAggregators();
      for (int i = 0; i < aggregators.length; ++i) {
        metricNames.add(aggregators[i].getName());
      }

      flushedIndex = context.getCounter("navis", "index-flush-count");

      maxOccupation = tuningConfig.getMaxOccupationInMemory();
      maxRowCount = tuningConfig.getRowFlushBoundary();
      occupationCheckInterval = 2000;

      merger = config.isBuildV9Directly()
               ? HadoopDruidIndexerConfig.INDEX_MERGER_V9
               : HadoopDruidIndexerConfig.INDEX_MERGER;

      Path path = config.makeShufflingDir(String.valueOf(context.getTaskAttemptID().getTaskID().getId()));

      shufflingFS = path.getFileSystem(context.getConfiguration());
      shufflingPath = JobHelper.prependFSIfNullScheme(shufflingFS, path);

      shufflingFS.mkdirs(shufflingPath);

      baseFlushFile = File.createTempFile("base", "flush");
      baseFlushFile.delete();
      baseFlushFile.mkdirs();

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
    protected void innerMap(InputRow row, Object value, Context context)
        throws IOException, InterruptedException
    {
      String dataSource = HynixCombineInputFormat.CURRENT_DATASOURCE.get();
      if (interval != null &&
          (!Objects.equals(currentDataSource, dataSource) || !interval.contains(row.getTimestampFromEpoch()))) {
        persist(context);
      }
      if (index != null) {
        boolean flush = !index.canAppendRow();
        if (lineCount > 0 && lineCount % occupationCheckInterval == 0) {
          long estimation = index.estimatedOccupation();
          log.info("... %,d rows in index with estimated size %,d bytes", index.size(), estimation);
          if (flush) {
            log.info("Flushing index because row count in index exceeding maxRowsInMemory %,d", maxRowCount);
          } else if (maxOccupation > 0 && estimation >= maxOccupation) {
            log.info("Flushing index because estimated size is bigger than %,d", maxOccupation);
            flush = true;
          }
        }
        if (flush) {
          persist(context);
        }
      }
      if (index == null) {
        currentDataSource = dataSource;
        interval = getConfig().getTargetInterval(row).get();
        index = makeIncrementalIndex();
        lineCount = 0;
        nextLogging = 1;
        log.info("Starting new index %s [%s]", currentDataSource, interval);
      }
      index.add(row);

      if (++lineCount % nextLogging == 0) {
        log.info("processing %,d lines..", lineCount);
        nextLogging = Math.min(nextLogging * 10, 100000);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
      super.cleanup(context);
      try {
        if (index != null && !index.isEmpty()) {
          persist(context);
        }
      }
      finally {
        FileUtils.deleteDirectory(baseFlushFile);
      }
    }

    private IncrementalIndex makeIncrementalIndex()
    {
      final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withMinTimestamp(interval.getStartMillis())
          .withDimensionsSpec(config.getSchema().getDataSchema().getParser())
          .withQueryGranularity(config.getSchema().getDataSchema().getGranularitySpec().getQueryGranularity())
          .withMetrics(aggregators)
          .build();

      return new OnheapIncrementalIndex(
          indexSchema,
          true,
          !tuningConfig.isIgnoreInvalidRows(),
          false,
          maxRowCount
      );
    }

    private void persist(Context context) throws IOException, InterruptedException
    {
      context.progress();
      log.info(
          "flushing index.. %,d rows with estimated size %,d bytes (%s) in %,d msec",
          index.size(), index.estimatedOccupation(), interval, System.currentTimeMillis() - startTime
      );

      File localFile = merger.persist(index, interval, nextFile(), config.getIndexSpec(), progressIndicator);

      final String dataSource = currentDataSource != null ? currentDataSource : config.getDataSource();
      final Path outFile = new Path(shufflingPath, localFile.getName() + ".zip");
      shufflingFS.mkdirs(outFile.getParent());

      log.info("Compressing files from [%s] to [%s]", localFile, outFile);
      final long size;
      try (FSDataOutputStream out = shufflingFS.create(outFile)) {
        size = CompressionUtils.zip(localFile, out);
      }
      log.info("Compressed into [%,d] bytes", size);
      Text key = new Text(dataSource + ":" + index.getInterval().getStartMillis());
      context.write(key, new Text(outFile.toString()));

      FileUtils.deleteDirectory(localFile);

      flushedIndex.increment(1);
      startTime = System.currentTimeMillis();
      index = null;
    }

    private File nextFile()
    {
      return new File(baseFlushFile, String.format("index-%,05d", indexCount++));
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

    private FileSystem shufflingFS;

    private FileSystem outputFS;
    private File baseFlushFile;

    private ProgressIndicator progressIndicator;
    private int indexCount;

    private long startTime;

    @Override
    protected void setup(final Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      tuningConfig = config.getSchema().getTuningConfig();

      aggregators = config.getSchema().getDataSchema().getAggregators();
      for (int i = 0; i < aggregators.length; ++i) {
        metricNames.add(aggregators[i].getName());
      }

      maxShardLength = tuningConfig.getMaxShardLength();

      merger = config.isBuildV9Directly()
               ? HadoopDruidIndexerConfig.INDEX_MERGER_V9
               : HadoopDruidIndexerConfig.INDEX_MERGER;

      // this is per datasource
      outputFS = new Path(config.getSchema().getIOConfig().getSegmentOutputPath())
          .getFileSystem(context.getConfiguration());

      baseFlushFile = File.createTempFile("base", "flush");
      baseFlushFile.delete();
      baseFlushFile.mkdirs();

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
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
      String[] split = key.toString().split(":");

      final String dataSource = split[0];
      final DateTime time = new DateTime(Long.valueOf(split[1]));
      final Interval interval = config.getGranularitySpec().bucketInterval(time).get();

      log.info("Starting merge on %s [%s]", dataSource, interval);
      final List<List<File>> groups = groupToShards(
          Lists.newArrayList(Iterables.transform(values, Functions.toStringFunction())),
          maxShardLength
      );

      final boolean singleShard = groups.size() == 1;

      for (int i = 0; i < groups.size(); i++) {
        Set<String> dimensions = Sets.newLinkedHashSet();
        List<File> shard = groups.get(i);
        File mergedBase;
        if (shard.size() == 1) {
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
              aggregators,
              new File(baseFlushFile, singleShard ? "single" : "shard-" + i),
              config.getIndexSpec(),
              progressIndicator
          );
        }
        ShardSpec shardSpec = singleShard ? new NoneShardSpec() : new NumberedShardSpec(i, groups.size());
        writeShard(dataSource, mergedBase, interval, Lists.newArrayList(dimensions), shardSpec, context);
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
        long length = CompressionUtils.unzip(shufflingFS.open(new Path(shuffle)), local).size();
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
          tuningConfig.getVersion(),
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

      log.info("Writing descriptor to path[%s]", descriptorPath);
      JobHelper.writeSegmentDescriptor(
          config.makeDescriptorInfoDir().getFileSystem(context.getConfiguration()),
          segment,
          descriptorPath,
          context
      );
    }
  }
}
