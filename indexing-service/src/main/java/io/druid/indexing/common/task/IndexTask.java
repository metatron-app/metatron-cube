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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.collections.Stats;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Murmur3;
import io.druid.data.ParserInitializationFail;
import io.druid.data.ParsingFail;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularity;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentAppendingAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.index.YeOldePlumberSchool;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IOConfig;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.AppendingGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(IndexTask.class);

  /**
   * Should we index this inputRow? Decision is based on our interval and shardSpec.
   *
   * @param inputRow the row to check
   *
   * @return true or false
   */
  private static boolean shouldIndex(
      final ShardSpec shardSpec,
      final Interval interval,
      final InputRow inputRow,
      final Granularity rollupGran
  )
  {
    return interval.contains(inputRow.getTimestampFromEpoch())
           && shardSpec.isInChunk(rollupGran.bucketStart(inputRow.getTimestampFromEpoch()), inputRow);
  }

  private static String makeId(String id, IndexIngestionSpec ingestionSchema)
  {
    if (id == null) {
      return String.format("index_%s_%s", makeDataSource(ingestionSchema), new DateTime());
    }

    return id;
  }

  private static String makeDataSource(IndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  private static Interval makeInterval(IndexIngestionSpec ingestionSchema)
  {
    GranularitySpec spec = ingestionSchema.getDataSchema().getGranularitySpec();
    return spec.umbrellaInterval();
  }

  static RealtimeTuningConfig convertTuningConfig(
      ShardSpec shardSpec,
      Integer rowFlushBoundary,
      IndexSpec indexSpec,
      Boolean buildV9Directly
  )
  {
    return new RealtimeTuningConfig(
        rowFlushBoundary,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        shardSpec,
        indexSpec,
        buildV9Directly,
        0,
        0,
        true,
        null,
        false
    );
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  private final List<String> hadoopDependencyCoordinates;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") IndexIngestionSpec ingestionSchema,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        // _not_ the version, just something uniqueish
        makeId(id, ingestionSchema),
        taskResource,
        makeDataSource(ingestionSchema),
        makeInterval(ingestionSchema),
        context
    );

    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    this.ingestionSchema = ingestionSchema;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @JsonProperty("hadoopDependencyCoordinates")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();

    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));
    final Set<DataSegment> segments = Sets.newHashSet();

    final Set<Interval> validIntervals = Sets.intersection(
        granularitySpec.bucketIntervals().get(), getDataIntervals(ingestionSchema)
    );
    if (validIntervals.isEmpty()) {
      throw new ISE("No valid data intervals found. Check your configs!");
    }

    IndexIngestionSpec ingestionSpec = ingestionSchema;
    if (granularitySpec.isAppending()) {
      TaskActionClient actionClient = toolbox.getTaskActionClient();
      List<Interval> intervals = JodaUtils.condenseIntervals(validIntervals);
      List<SegmentDescriptor> descriptors = actionClient.submit(
          new SegmentAppendingAction(getDataSource(), intervals, granularitySpec.getSegmentGranularity())
      );
      AppendingGranularitySpec appendingSpec = new AppendingGranularitySpec(granularitySpec, descriptors);
      ingestionSpec = ingestionSpec.withDataSchema(dataSchema.withGranularitySpec(appendingSpec));
      log.info("Granularity spec is rewritten as %s", appendingSpec);
    }

    String version = myLock.getVersion();
    log.info("Setting version to: %s", version);

    final Stats stats = new Stats();
    for (final Interval bucket : validIntervals) {
      final int numShards;
      if (tuningConfig.getTargetPartitionSize() != null) {
        numShards = determineNumShards(ingestionSpec, bucket, tuningConfig.getTargetPartitionSize(), stats);
      } else {
        numShards = tuningConfig.getNumShards();
      }
      final List<DataSegment> generated = generateSegment(
          ingestionSpec,
          toolbox,
          bucket,
          version,
          numShards,
          stats
      );
      segments.addAll(generated);
    }
    toolbox.publishSegments(segments);

    return TaskStatus.success(getId(), stats.stats());
  }

  @Override
  public String getClasspathPostfix()
  {
    if (!GuavaUtils.isNullOrEmpty(hadoopDependencyCoordinates)) {
      log.info("Using hadoopDependencyCoordinates %s", hadoopDependencyCoordinates);
      List<String> files = Lists.newArrayList();
      ExtensionsConfig extensionsConfig = GuiceInjectors.makeStartupInjector().getInstance(ExtensionsConfig.class);
      File hadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());
      for (File hadoopDependency :
          Initialization.getHadoopDependencyFilesToLoad(hadoopDependencyCoordinates, extensionsConfig)) {
        for (File file : FileUtils.listFiles(hadoopDependency, new String[]{"jar"}, false)) {
          if (!file.isHidden() && file.isFile()) {
            files.add(file.getAbsolutePath());
          }
        }
      }
      if (!files.isEmpty()) {
        return StringUtils.join(files, File.pathSeparator);
      }
    }
    return null;
  }

  private SortedSet<Interval> getDataIntervals(IndexIngestionSpec ingestionSchema) throws IOException
  {
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    int unparsed = 0;
    SortedSet<Interval> retVal = Sets.newTreeSet(JodaUtils.intervalsByStartThenEnd());
    InputRowParser parser = ingestionSchema.getParser(jsonMapper);
    try (Firehose firehose = Firehose.wrap(firehoseFactory.connect(parser), parser)) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          continue;
        }
        DateTime dt = new DateTime(inputRow.getTimestampFromEpoch());
        Optional<Interval> interval = granularitySpec.bucketInterval(dt);
        if (interval.isPresent()) {
          retVal.add(interval.get());
        } else {
          unparsed++;
        }
      }
    }
    if (unparsed > 0) {
      log.warn("Unable to to find a matching interval for [%,d] events", unparsed);
    }

    return retVal;
  }

  private int determineNumShards(
      final IndexIngestionSpec ingestionSpec,
      final Interval interval,
      final int targetPartitionSize,
      final Stats stats
  ) throws IOException
  {
    log.info("Determining partitions for interval[%s] with targetPartitionSize[%d]", interval, targetPartitionSize);

    final FirehoseFactory firehoseFactory = ingestionSpec.getIOConfig().getFirehoseFactory();
    final Granularity queryGranularity = ingestionSpec.getDataSchema().getGranularitySpec().getQueryGranularity();

    // The implementation of this determine partitions stuff is less than optimal.  Should be done better.
    // Use HLL to estimate number of rows
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    // Load data
    int totalRows = 0;
    InputRowParser parser = ingestionSpec.getParser(jsonMapper);
    try (Firehose firehose = Firehose.wrap(firehoseFactory.connect(parser), parser)) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          continue;
        }
        if (interval.contains(inputRow.getTimestampFromEpoch())) {
          final List<Object> groupKey = Rows.toGroupKey(
              queryGranularity.bucketStart(inputRow.getTimestampFromEpoch()),
              inputRow
          );
          collector.add(Murmur3.hash128(jsonMapper.writeValueAsBytes(groupKey)));
        }
        totalRows++;
      }
    }
    stats.ofLong("totalRows").set(totalRows);

    final double numRows = collector.estimateCardinality();
    log.info("Estimated approximately [%,f] rows of data.", numRows);

    int numberOfShards = (int) Math.ceil(numRows / targetPartitionSize);
    if ((double) numberOfShards > numRows) {
      numberOfShards = (int) numRows;
    }
    log.info("Will require [%,d] shard(s).", numberOfShards);
    return numberOfShards;
  }

  private List<DataSegment> generateSegment(
      final IndexIngestionSpec ingestionSpec,
      final TaskToolbox toolbox,
      final Interval interval,
      final String version,
      final int numShards,
      final Stats stats
  ) throws IOException
  {
    List<DataSegment> segments = Lists.newArrayList();
    for (int i = 0; i < numShards; i++) {
      ShardSpec shardSpec = numShards == 1 ?
                            NoneShardSpec.instance() : new HashBasedNumberedShardSpec(i, numShards, null, jsonMapper);
      segments.add(generateSegment(ingestionSpec, toolbox, interval, version, shardSpec, stats));
    }
    return segments;
  }

  private DataSegment generateSegment(
      final IndexIngestionSpec ingestionSpec,
      final TaskToolbox toolbox,
      final Interval interval,
      final String version,
      final ShardSpec shardSpec,
      final Stats stats
  ) throws IOException
  {
    // Set up temporary directory.
    final File tmpDir = new File(
        toolbox.getTaskWorkDir(),
        String.format(
            "%s_%s_%s_%s_%s",
            getDataSource(),
            interval.getStart(),
            interval.getEnd(),
            version,
            shardSpec.getPartitionNum()
        )
    );

    final DataSchema schema = ingestionSpec.getDataSchema();
    final GranularitySpec granularity = schema.getGranularitySpec();
    final FirehoseFactory firehoseFactory = ingestionSpec.getIOConfig().getFirehoseFactory();
    final IndexTuningConfig tuningConfig = ingestionSpec.getTuningConfig();
    final int maxRowsInMemory = tuningConfig.getMaxRowsInMemory();

    // We need to track published segments.
    final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<DataSegment>();
    final DataSegmentPusher wrappedDataSegmentPusher = new DataSegmentPusher()
    {
      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public String getPathForHadoop()
      {
        return toolbox.getSegmentPusher().getPathForHadoop();
      }

      @Override
      public DataSegment push(File file, DataSegment segment) throws IOException
      {
        if (granularity instanceof AppendingGranularitySpec) {
          AppendingGranularitySpec appending = (AppendingGranularitySpec) granularity;
          int appendingPartitionNum = 0;
          String appendingVersion = version;
          SegmentDescriptor descriptor = appending.getSegmentDescriptor(interval.getStartMillis());
          if (descriptor != null) {
            appendingPartitionNum = descriptor.getPartitionNumber();
            appendingVersion = descriptor.getVersion();
          }
          segment = segment.withVersion(appendingVersion)
                           .withShardSpec(LinearShardSpec.of(appendingPartitionNum + shardSpec.getPartitionNum()));
        }
        DataSegment pushed = toolbox.getSegmentPusher().push(file, segment);
        pushedSegments.add(pushed);
        return pushed;
      }
    };

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = maxRowsInMemory > 0
                                   ? maxRowsInMemory
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();

    // Create firehose + plumber
    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final InputRowParser parser = ingestionSpec.getParser(jsonMapper);
    final Firehose firehose = Firehose.wrap(firehoseFactory.connect(parser), parser);
    final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);
    final Plumber plumber = new YeOldePlumberSchool(
        interval,
        version,
        wrappedDataSegmentPusher,
        tmpDir,
        toolbox.getIndexMerger(),
        toolbox.getIndexMergerV9(),
        toolbox.getIndexIO()
    ).findPlumber(
        schema,
        convertTuningConfig(
            shardSpec,
            myRowFlushBoundary,
            tuningConfig.getIndexSpec(),
            true
        ),
        metrics
    );

    final Granularity rollupGran = schema.getGranularitySpec().getQueryGranularity();
    try {
      plumber.startJob();

      firehose.start();
      while (firehose.hasMore()) {
        final InputRow inputRow;
        try {
          inputRow = firehose.nextRow();
        }
        catch (Throwable e) {
          if (tuningConfig.isIgnoreInvalidRows()) {
            metrics.incrementUnparseable();
            handelInvalidRow(e);
            continue;
          }
          throw Throwables.propagate(e);
        }
        if (inputRow == null) {
          metrics.incrementUnparseable();
          continue;
        }

        if (shouldIndex(shardSpec, interval, inputRow, rollupGran)) {
          int numRows = plumber.add(inputRow, committerSupplier);
          if (numRows == -1) {
            throw new ISE(
                String.format(
                    "Was expecting non-null sink for timestamp[%s]",
                    new DateTime(inputRow.getTimestampFromEpoch())
                )
            );
          }
          metrics.incrementProcessed();
        } else {
          metrics.incrementThrownAway();
        }
      }
    }
    finally {
      firehose.close();
    }

    plumber.persist(committerSupplier.get());

    try {
      plumber.finishJob();
    }
    finally {
      log.info(
          "Task[%s] interval[%s] partition[%d] took in %,d rows (%,d processed, %,d unparseable, %,d thrown away)"
          + " and output %,d rows",
          getId(),
          interval,
          shardSpec.getPartitionNum(),
          metrics.totalRows(),
          metrics.processed(),
          metrics.unparseable(),
          metrics.thrownAway(),
          metrics.rowOutput()
      );
    }
    if (!stats.hasStats("totalRows")) {
      stats.ofLong("totalRows").set(metrics.totalRows());
    }
    stats.ofLong("indexRows").increase(metrics.processed());
    stats.ofLong("numSegments").increase(1);

    firehose.success();

    // We expect a single segment to have been created.
    return Iterables.getOnlyElement(pushedSegments);
  }

  private static final int INVALID_LOG_THRESHOLD = 3;

  private int invalidRows;
  private int nextLog = 1000;

  private void handelInvalidRow(Throwable e)
  {
    invalidRows++;
    if (e instanceof ParserInitializationFail) {
      throw (ParserInitializationFail) e;   // invalid configuration, etc.. fail early
    }
    if (invalidRows <= INVALID_LOG_THRESHOLD) {
      Object value = null;
      if (e instanceof ParsingFail) {
        value = ((ParsingFail) e).getInput();
        e = e.getCause() == null ? e : e.getCause();
      }
      log.info(
          e,
          "Ignoring invalid row [%s] due to parsing error.. %s", value,
          invalidRows == INVALID_LOG_THRESHOLD ? "will not be logged further" : ""
      );
    }
    if (invalidRows % nextLog == 0) {
      log.info("%,d invalid rows..", invalidRows);
      nextLog = Math.min(10_000_000, nextLog * 10);
    }
  }

  public static class IndexIngestionSpec extends IngestionSpec<IndexIOConfig, IndexTuningConfig>
  {
    @JsonCreator
    public IndexIngestionSpec(
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("ioConfig") IndexIOConfig ioConfig,
        @JsonProperty("tuningConfig") IndexTuningConfig tuningConfig
    )
    {
      super(dataSchema, ioConfig, tuningConfig == null ? IndexTuningConfig.DEFAULT : tuningConfig);
    }

    @Override
    @JsonProperty("dataSchema")
    public DataSchema getDataSchema()
    {
      return dataSchema;
    }

    @Override
    @JsonProperty("ioConfig")
    public IndexIOConfig getIOConfig()
    {
      return ioConfig;
    }

    @Override
    @JsonProperty("tuningConfig")
    public IndexTuningConfig getTuningConfig()
    {
      return tuningConfig;
    }

    @Override
    public IndexIngestionSpec withDataSchema(DataSchema dataSchema)
    {
      return new IndexIngestionSpec(dataSchema, ioConfig, tuningConfig);
    }
  }

  @JsonTypeName("index")
  public static class IndexIOConfig implements IOConfig
  {
    private final FirehoseFactory firehoseFactory;

    @JsonCreator
    public IndexIOConfig(
        @JsonProperty("firehose") FirehoseFactory firehoseFactory
    )
    {
      this.firehoseFactory = firehoseFactory;
    }

    @JsonProperty("firehose")
    public FirehoseFactory getFirehoseFactory()
    {
      return firehoseFactory;
    }
  }

  @JsonTypeName("index")
  public static class IndexTuningConfig extends BaseTuningConfig
  {
    public static final IndexTuningConfig DEFAULT = new IndexTuningConfig(null, null, null, null, false, null, null);

    private static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;

    private final Integer targetPartitionSize;
    private final Integer numShards;

    public IndexTuningConfig(
        @JsonProperty("indexSpec") IndexSpec indexSpec,
        @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
        @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
        @JsonProperty("buildV9Directly") Boolean buildV9Directly,
        @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
        @JsonProperty("targetPartitionSize") Integer targetPartitionSize,
        @JsonProperty("numShards") Integer numShards
    )
    {
      super(
          indexSpec,
          maxRowsInMemory,
          maxOccupationInMemory,
          null,
          buildV9Directly,
          ignoreInvalidRows
      );
      if (!isSet(targetPartitionSize) && !isSet(numShards)) {
        targetPartitionSize = DEFAULT_TARGET_PARTITION_SIZE;
      }
      Preconditions.checkArgument(
          isSet(targetPartitionSize) ^ isSet(numShards),
          "Must have a valid, non-null targetPartitionSize or numShards"
      );
      this.targetPartitionSize = targetPartitionSize;
      this.numShards = numShards;
    }

    public IndexTuningConfig(
        IndexSpec indexSpec,
        int maxRowsInMemory,
        Boolean buildV9Directly,
        boolean ignoreInvalidRows,
        int targetPartitionSize,
        Integer numShards
    )
    {
      this(indexSpec, maxRowsInMemory, null, buildV9Directly, ignoreInvalidRows, targetPartitionSize, numShards);
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getTargetPartitionSize()
    {
      return targetPartitionSize;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNumShards()
    {
      return numShards;
    }

    private boolean isSet(Integer value)
    {
      return value != null && value > 0;
    }
  }
}
