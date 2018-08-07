package io.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.InputRow;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMerger;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.indexing.granularity.AppendingGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 */
public class MapOnlyIndexGeneratorJob implements HadoopDruidIndexerJob.IndexingStatsProvider
{
  private static final Logger log = new Logger(MapOnlyIndexGeneratorJob.class);

  private final HadoopDruidIndexerConfig config;
  private IndexGeneratorStats jobStats;

  public MapOnlyIndexGeneratorJob(
      HadoopDruidIndexerConfig config
  )
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
    log.info("Running MapOnlyIndexGeneratorJob.. %s %s", config.getDataSource(), config.getIntervals());
    try {
      Job job = Job.getInstance(
          new Configuration(),
          String.format("%s-index-generator-map-only-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);
      config.addJobProperties(job);

      job.setMapperClass(MapOnlyIndexGeneratorMapper.class);

      job.setNumReduceTasks(0);

      job.setOutputKeyClass(BytesWritable.class);
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

  public static class MapOnlyIndexGeneratorMapper extends HadoopDruidIndexerMapper<BytesWritable, Text>
  {
    private HadoopTuningConfig tuningConfig;

    private IndexMerger merger;
    private List<String> metricNames = Lists.newArrayList();

    private AggregatorFactory[] aggregators;
    private Counter flushedIndex;
    private Counter outOfRangeRows;

    private long maxOccupation;
    private long maxShardLength;

    private FileSystem outputFS;
    private File baseFlushFile;
    private int occupationCheckInterval = 2000;

    private Set<File> toMerge = Sets.newTreeSet();
    private Set<String> allDimensionNames = Sets.newLinkedHashSet();
    private ProgressIndicator progressIndicator;

    private int numRows;

    private int nextLogging = 1;
    private int lineCount;

    private int runningTotalLineCount;
    private long startTime;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      try {
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        persistAll(context);
      } finally {
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
      for (AggregatorFactory aggregator : aggregators) {
        metricNames.add(aggregator.getName());
      }

      flushedIndex = context.getCounter("navis", "index-flush-count");
      outOfRangeRows = context.getCounter("navis", "out-of-range-rows");

      maxOccupation = tuningConfig.getMaxOccupationInMemory();
      maxShardLength = tuningConfig.getMaxShardLength();
      occupationCheckInterval = 2000;

      merger = config.isBuildV9Directly()
               ? HadoopDruidIndexerConfig.INDEX_MERGER_V9
               : HadoopDruidIndexerConfig.INDEX_MERGER;

      // this is per datasource
      outputFS = new Path(config.getSchema().getIOConfig().getSegmentOutputPath())
          .getFileSystem(context.getConfiguration());

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

      startTime = System.currentTimeMillis();
    }

    private Interval interval;
    private IncrementalIndex<?> index;

    @Override
    protected void innerMap(
        InputRow row, Object value, Context context
    ) throws IOException, InterruptedException
    {
      if (interval == null) {
        interval = getConfig().getTargetInterval(row).get();
        index = makeIncrementalIndex();
      } else if (!interval.contains(row.getTimestampFromEpoch())) {
        outOfRangeRows.increment(1);
      }
      final InputRow inputRow = index.formatRow(row);

      boolean flush = !index.canAppendRow();

      if (lineCount > 0 && lineCount % occupationCheckInterval == 0) {
        long estimation = index.estimatedOccupation();
        log.info("... %,d rows in index with estimated size %,d bytes", index.size(), estimation);
        if (!flush && maxOccupation > 0 && estimation >= maxOccupation) {
          log.info("flushing index because estimated size is bigger than %,d", maxOccupation);
          flush = true;
        }
      }
      if (flush) {
        allDimensionNames.addAll(index.getDimensionOrder());

        if (index.getOutOfRowsReason() != null) {
          log.info(index.getOutOfRowsReason());
        }
        log.info(
            "%,d lines to %,d rows in %,d millis",
            lineCount - runningTotalLineCount,
            numRows,
            System.currentTimeMillis() - startTime
        );
        runningTotalLineCount = lineCount;

        final File file = new File(baseFlushFile, String.format("index-%,05d", toMerge.size()));
        toMerge.add(file);

        context.progress();
        persist(index, interval, file, progressIndicator);
        index = makeIncrementalIndex();
        startTime = System.currentTimeMillis();
      }
      numRows = index.add(inputRow);
      if (++lineCount % nextLogging == 0) {
        log.info("processing %,d lines..", lineCount);
        nextLogging = Math.min(nextLogging * 10, 1000000);
      }
    }

    @Override
    protected void cleanup(
        Context context
    ) throws IOException, InterruptedException
    {
      for (File file : toMerge) {
        FileUtils.deleteDirectory(file);
      }
      toMerge.clear();
    }

    private void persistAll(Context context) throws IOException
    {
      if (index != null) {
        allDimensionNames.addAll(index.getDimensionOrder());
      }
      log.info("%,d lines completed.", lineCount);

      if (toMerge.isEmpty()) {
        if (index == null || index.isEmpty()) {
          throw new IAE("If you try to persist empty indexes you are going to have a bad time");
        }
        File mergedBase = new File(baseFlushFile, "merged");
        persist(index, interval, mergedBase, progressIndicator);
        toMerge.add(mergedBase);
      } else {
        if (!index.isEmpty()) {
          File finalFile = new File(baseFlushFile, "final");
          persist(index, interval, finalFile, progressIndicator);
          toMerge.add(finalFile);
        }
      }
      final GranularitySpec granularitySpec = config.getGranularitySpec();
      LinearShardSpec appendingSpec = LinearShardSpec.of(0);
      String version = tuningConfig.getVersion();
      if (granularitySpec instanceof AppendingGranularitySpec) {
        SegmentDescriptor descriptor = ((AppendingGranularitySpec) granularitySpec).getSegmentDescriptor(interval);
        if (descriptor != null) {
          appendingSpec = LinearShardSpec.of(descriptor.getPartitionNumber());
          version = descriptor.getVersion();
        }
      }

      if (toMerge.size() == 1) {
        writeShard(Iterables.getOnlyElement(toMerge), version, appendingSpec, context);
      } else {
        List<List<File>> groups = groupToShards(toMerge, maxShardLength);

        final boolean singleShard = groups.size() == 1;

        for (int i = 0; i < groups.size(); i++) {
          List<File> shard = groups.get(i);
          File mergedBase;
          if (shard.size() == 1) {
            mergedBase = shard.get(0);
          } else {
            final List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(shard.size());
            for (File file : shard) {
              indexes.add(HadoopDruidIndexerConfig.INDEX_IO.loadIndex(file));
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
          ShardSpec shardSpec = LinearShardSpec.of(appendingSpec.getPartitionNum() + i);
          writeShard(mergedBase, version, shardSpec, context);
        }
      }
    }

    private List<List<File>> groupToShards(Set<File> toMerge, long limit)
    {
      List<List<File>> groups = Lists.newArrayList();
      List<File> group = Lists.newArrayList();
      long current = 0;
      for (File file : toMerge) {
        long length = FileUtils.sizeOfDirectory(file);
        log.info("file [%s] : %,d bytes", file, length);
        if (!group.isEmpty() && current + length > limit) {
          log.info("group-%d : %s", groups.size(), group);
          groups.add(group);
          group = Lists.newArrayList();
          current = 0;
        }
        group.add(file);
        current += length;
      }
      if (!group.isEmpty()) {
        log.info("group-%d : %s", groups.size(), group);
        groups.add(group);
      }
      return groups;
    }

    private void writeShard(File directory, String version, ShardSpec shardSpec, Context context) throws IOException
    {
      final DataSegment segmentTemplate = new DataSegment(
          config.getDataSource(),
          interval,
          version,
          null,
          ImmutableList.copyOf(allDimensionNames),
          metricNames,
          shardSpec,
          -1,
          -1
      );

      final Path segmentBasePath = JobHelper.makeSegmentOutputPath(
          new Path(config.getSchema().getIOConfig().getSegmentOutputPath()),
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

    private IncrementalIndex makeIncrementalIndex()
    {
      final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withMinTimestamp(interval.getStartMillis())
          .withDimensionsSpec(config.getSchema().getDataSchema().getParser())
          .withQueryGranularity(config.getSchema().getDataSchema().getGranularitySpec().getQueryGranularity())
          .withMetrics(aggregators)
          .withRollup(config.getSchema().getDataSchema().getGranularitySpec().isRollup())
          .build();

      OnheapIncrementalIndex newIndex = new OnheapIncrementalIndex(
          indexSchema,
          true,
          !tuningConfig.isIgnoreInvalidRows(),
          true,
          true,
          tuningConfig.getRowFlushBoundary()
      );

      if (allDimensionNames != null && !indexSchema.getDimensionsSpec().hasCustomDimensions()) {
        newIndex.loadDimensionIterable(allDimensionNames);
      }

      return newIndex;
    }

    private File persist(IncrementalIndex index, Interval interval, File file, ProgressIndicator progress)
        throws IOException
    {
      flushedIndex.increment(1);
      log.info(
          "flushing index.. %,d rows with estimated size %,d bytes (%s)",
          index.size(), index.estimatedOccupation(), interval
      );
      return merger.persist(index, interval, file, config.getIndexSpec(), progress);
    }
  }
}
