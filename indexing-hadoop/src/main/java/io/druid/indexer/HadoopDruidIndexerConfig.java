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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularity;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.path.PartitionPathSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.server.DruidNode;
import io.druid.server.ServiceTypes;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.ShardSpecLookup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 */
public class HadoopDruidIndexerConfig
{
  private static final Logger log = new Logger(HadoopDruidIndexerConfig.class);
  private static final Injector injector;

  public static final String CONFIG_PROPERTY = "druid.indexer.config";
  public static final Charset JAVA_NATIVE_CHARSET = Charset.forName("Unicode");
  public static final Splitter TAB_SPLITTER = Splitter.on("\t");
  public static final Joiner TAB_JOINER = Joiner.on("\t");
  public static final ObjectMapper JSON_MAPPER;
  public static final IndexIO INDEX_IO;
  public static final IndexMerger INDEX_MERGER;
  public static final IndexMergerV9 INDEX_MERGER_V9;

  private static final String DEFAULT_WORKING_PATH = "/tmp/druid-indexing";

  static {
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("hadoop-indexer", null, null)
                );
                binder.bindConstant().annotatedWith(Names.named("type")).to(ServiceTypes.HADOOP_INDEXING);
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(-1);
              }
            },
            new IndexingHadoopModule()
        )
    );
    JSON_MAPPER = injector.getInstance(ObjectMapper.class);
    INDEX_IO = injector.getInstance(IndexIO.class);
    INDEX_MERGER = injector.getInstance(IndexMerger.class);
    INDEX_MERGER_V9 = injector.getInstance(IndexMergerV9.class);
  }

  public enum IndexJobCounters
  {
    INVALID_ROW_COUNTER
  }

  public static HadoopDruidIndexerConfig fromSpec(HadoopIngestionSpec spec)
  {
    return new HadoopDruidIndexerConfig(spec);
  }

  public static HadoopDruidIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    // Eventually PathSpec needs to get rid of its Hadoop dependency, then maybe this can be ingested directly without
    // the Map<> intermediary

    if (argSpec.containsKey("spec")) {
      return HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
          argSpec,
          HadoopDruidIndexerConfig.class
      );
    }
    return new HadoopDruidIndexerConfig(
        HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
            argSpec,
            HadoopIngestionSpec.class
        )
    );
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromFile(File file)
  {
    try {
      return fromMap(
          (Map<String, Object>) HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
              file, new TypeReference<Map<String, Object>>()
              {
              }
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromString(String str)
  {
    // This is a map to try and prevent dependency screwbally-ness
    try {
      return fromMap(
          (Map<String, Object>) HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
              str, new TypeReference<Map<String, Object>>()
              {
              }
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromDistributedFileSystem(String path)
  {
    try {
      Path pt = new Path(path);
      FileSystem fs = pt.getFileSystem(new Configuration());
      Reader reader = new InputStreamReader(fs.open(pt));

      return fromMap(
          (Map<String, Object>) HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
              reader, new TypeReference<Map<String, Object>>()
              {
              }
          )
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static HadoopDruidIndexerConfig fromConfiguration(Configuration conf)
  {
    final HadoopDruidIndexerConfig retVal = fromString(conf.get(CONFIG_PROPERTY));
    retVal.verify();
    return retVal;
  }

  private volatile HadoopIngestionSpec schema;
  private volatile PathSpec pathSpec;
  private final Map<Long, ShardSpecLookup> shardSpecLookups = Maps.newHashMap();
  private final Map<Long, Map<ShardSpec, HadoopyShardSpec>> hadoopShardSpecLookup = Maps.newHashMap();
  private final Granularity rollupGran;

  @JsonCreator
  @SuppressWarnings("unchecked")
  public HadoopDruidIndexerConfig(
      @JsonProperty("spec") HadoopIngestionSpec spec
  )
  {
    PathSpec pathSpec = JSON_MAPPER.convertValue(spec.getIOConfig().getPathSpec(), PathSpec.class);
    if (pathSpec instanceof PathSpec.Resolving) {
      pathSpec = ((PathSpec.Resolving) pathSpec).resolve();
      spec = spec.withIOConfig(
          spec.getIOConfig().withPathSpec(
              DefaultObjectMapper.excludeNulls(JSON_MAPPER).convertValue(pathSpec, Map.class)
          )
      );
    }
    this.schema = spec;
    this.pathSpec = pathSpec;
    for (Map.Entry<Long, List<HadoopyShardSpec>> entry : spec.getTuningConfig().getShardSpecs().entrySet()) {
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        continue;
      }
      final ShardSpec actualSpec = entry.getValue().get(0).getActualSpec();
      shardSpecLookups.put(
          entry.getKey(), actualSpec.getLookup(
              Lists.transform(
                  entry.getValue(), new Function<HadoopyShardSpec, ShardSpec>()
                  {
                    @Override
                    public ShardSpec apply(HadoopyShardSpec input)
                    {
                      return input.getActualSpec();
                    }
                  }
              )
          )
      );

      Map<ShardSpec, HadoopyShardSpec> innerHadoopShardSpecLookup = Maps.newHashMap();
      for (HadoopyShardSpec hadoopyShardSpec : entry.getValue()) {
        innerHadoopShardSpecLookup.put(hadoopyShardSpec.getActualSpec(), hadoopyShardSpec);
      }
      hadoopShardSpecLookup.put(entry.getKey(), innerHadoopShardSpecLookup);

    }
    this.rollupGran = spec.getDataSchema().getGranularitySpec().getQueryGranularity();
  }

  @JsonProperty(value = "spec")
  public HadoopIngestionSpec getSchema()
  {
    return schema;
  }

  @JsonIgnore
  public PathSpec getPathSpec()
  {
    return pathSpec;
  }

  public String getDataSource()
  {
    return schema.getDataSchema().getDataSource();
  }

  public GranularitySpec getGranularitySpec()
  {
    return schema.getDataSchema().getGranularitySpec();
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    log.info("Replacing %s with %s", schema.getDataSchema().getGranularitySpec(), granularitySpec);
    this.schema = schema.withDataSchema(schema.getDataSchema().withGranularitySpec(granularitySpec));
    this.pathSpec = JSON_MAPPER.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public PartitionsSpec getPartitionsSpec()
  {
    return schema.getTuningConfig().getPartitionsSpec();
  }

  public IndexSpec getIndexSpec()
  {
    return schema.getTuningConfig().getIndexSpec();
  }

  public boolean isOverwriteFiles()
  {
    return schema.getTuningConfig().isOverwriteFiles();
  }

  public boolean isIgnoreInvalidRows()
  {
    return schema.getTuningConfig().isIgnoreInvalidRows();
  }

  public int getNumReducer()
  {
    Optional<Iterable<Bucket>> buckets = getAllBuckets();
    if (!buckets.isPresent()) {
      return 0;
    }
    int bucketSize = Iterables.size(buckets.get());
    HadoopTuningConfig config = schema.getTuningConfig();
    return Math.min(Math.max(bucketSize, config.getMinReducer()), config.getMaxReducer());
  }

  public int getMaxReducer()
  {
    return schema.getTuningConfig().getMaxReducer();
  }

  public void setVersion(String version)
  {
    log.info("Replacing %s with %s", schema.getTuningConfig().getVersion(), version);
    this.schema = schema.withTuningConfig(schema.getTuningConfig().withVersion(version));
    this.pathSpec = JSON_MAPPER.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public void setShardSpecs(Map<Long, List<HadoopyShardSpec>> shardSpecs)
  {
    log.info("Replacing %s with %s", schema.getTuningConfig().getShardSpecs(), shardSpecs);
    this.schema = schema.withTuningConfig(schema.getTuningConfig().withShardSpecs(shardSpecs));
    this.pathSpec = JSON_MAPPER.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public Optional<List<Interval>> getIntervals()
  {
    Optional<SortedSet<Interval>> setOptional = schema.getDataSchema().getGranularitySpec().bucketIntervals();
    if (setOptional.isPresent()) {
      return Optional.of((List<Interval>) JodaUtils.condenseIntervals(setOptional.get()));
    } else {
      return Optional.absent();
    }
  }

  public boolean isDeterminingPartitions()
  {
    return schema.getTuningConfig().getPartitionsSpec().isDeterminingPartitions();
  }

  public Long getTargetPartitionSize()
  {
    return schema.getTuningConfig().getPartitionsSpec().getTargetPartitionSize();
  }

  public long getMaxPartitionSize()
  {
    return schema.getTuningConfig().getPartitionsSpec().getMaxPartitionSize();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (schema.getIOConfig().getMetadataUpdateSpec() != null);
  }

  public boolean isCombineText()
  {
    return schema.getTuningConfig().isCombineText();
  }

  public InputRowParser getParser()
  {
    return schema.getDataSchema().getParser();
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return schema.getTuningConfig().getShardSpecs().get(bucket.time.getMillis()).get(bucket.partitionNum);
  }

  public boolean isBuildV9Directly()
  {
    return schema.getTuningConfig().getBuildV9Directly();
  }

  /**
   * Job instance should have Configuration set (by calling {@link #addJobProperties(Job)}
   * or via injected system properties) before this method is called.  The {@link PathSpec} may
   * create objects which depend on the values of these configurations.
   *
   * @param job
   *
   * @return
   *
   * @throws IOException
   */
  public Job addInputPaths(Job job) throws IOException
  {
    return pathSpec.addInputPaths(this, job);
  }

  /********************************************
   Granularity/Bucket Helper Methods
   ********************************************/

  /**
   * Get the proper bucket for some input row.
   *
   * @param inputRow an InputRow
   *
   * @return the Bucket that this row belongs to
   */
  public Optional<Bucket> getBucket(InputRow inputRow)
  {
    final Optional<Interval> timeBucket = getTargetInterval(inputRow);
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }
    final DateTime bucketStart = timeBucket.get().getStart();
    final ShardSpec actualSpec = shardSpecLookups.get(bucketStart.getMillis())
                                                 .getShardSpec(
                                                     rollupGran.truncate(inputRow.getTimestampFromEpoch()),
                                                     inputRow
                                                 );
    final HadoopyShardSpec hadoopyShardSpec = hadoopShardSpecLookup.get(bucketStart.getMillis()).get(actualSpec);

    return Optional.of(
        new Bucket(
            hadoopyShardSpec.getShardNum(),
            bucketStart,
            actualSpec.getPartitionNum()
        )
    );

  }

  public Optional<Interval> getTargetInterval(InputRow inputRow)
  {
    return schema.getDataSchema().getGranularitySpec().bucketInterval(
        new DateTime(inputRow.getTimestampFromEpoch())
    );
  }

  public Optional<Set<Interval>> getSegmentGranularIntervals()
  {
    return Optional.fromNullable(
        (Set<Interval>) schema.getDataSchema()
                              .getGranularitySpec()
                              .bucketIntervals()
                              .orNull()
    );
  }

  public Optional<Iterable<Bucket>> getAllBuckets()
  {
    Optional<Set<Interval>> intervals = getSegmentGranularIntervals();
    if (intervals.isPresent()) {
      return Optional.of(
          (Iterable<Bucket>) FunctionalIterable
              .create(intervals.get())
              .transformCat(
                  new Function<Interval, Iterable<Bucket>>()
                  {
                    @Override
                    public Iterable<Bucket> apply(Interval input)
                    {
                      final DateTime bucketTime = input.getStart();
                      final List<HadoopyShardSpec> specs = schema.getTuningConfig().getShardSpecs().get(bucketTime.getMillis());
                      if (specs == null) {
                        return ImmutableList.of();
                      }

                      return FunctionalIterable
                          .create(specs)
                          .transform(
                              new Function<HadoopyShardSpec, Bucket>()
                              {
                                int i = 0;

                                @Override
                                public Bucket apply(HadoopyShardSpec input)
                                {
                                  return new Bucket(input.getShardNum(), bucketTime, i++);
                                }
                              }
                          );
                    }
                  }
              )
      );
    } else {
      return Optional.absent();
    }
  }

  public String getWorkingPath()
  {
    final String workingPath = schema.getTuningConfig().getWorkingPath();
    return workingPath == null ? DEFAULT_WORKING_PATH : workingPath;
  }

  /******************************************
   Path helper logic
   ******************************************/

  /**
   * Make the intermediate path for this job run.
   *
   * @return the intermediate path for this job run.
   */

  public Path makeIntermediatePath()
  {
    return new Path(
        String.format(
            "%s/%s/%s/%s",
            getWorkingPath(),
            schema.getDataSchema().getDataSource(),
            schema.getTuningConfig().getVersion().replace(":", ""),
            schema.getUniqueId()
        )
    );
  }

  public Path makeSegmentPartitionInfoPath(Interval bucketInterval)
  {
    return new Path(
        String.format(
            "%s/%s_%s/partitions.json",
            makeIntermediatePath(),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getStart()),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getEnd())
        )
    );
  }

  public Path makeIntervalInfoPath()
  {
    return new Path(
        String.format(
            "%s/intervals.json",
            makeIntermediatePath()
        )
    );
  }

  public Path makeShuffleDir()
  {
    return new Path(makeIntermediatePath(), "shuffle");
  }

  public Path makeShufflingDir(String path)
  {
    return new Path(makeShuffleDir(), path);
  }

  public Path makeDescriptorInfoDir()
  {
    return new Path(makeIntermediatePath(), "segmentDescriptorInfo");
  }

  public Path makeGroupedDataDir()
  {
    return new Path(makeIntermediatePath(), "groupedData");
  }

  public Path makeDescriptorInfoPath(DataSegment segment)
  {
    return new Path(makeDescriptorInfoDir(), String.format("%s.json", segment.getIdentifier().replace(":", "")));
  }

  public void addJobProperties(Job job)
  {
    Configuration conf = job.getConfiguration();

    for (final Map.Entry<String, String> entry : schema.getTuningConfig().getJobProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public void intoConfiguration(Job job)
  {
    Configuration conf = job.getConfiguration();

    try {
      conf.set(CONFIG_PROPERTY, JSON_MAPPER.writeValueAsString(this));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void verify()
  {
    final ObjectMapper mapper = DefaultObjectMapper.excludeNulls(JSON_MAPPER);

    try {
      log.info("Running with config:%n%s", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    DataSchema dataSchema = schema.getDataSchema();
    Preconditions.checkNotNull(dataSchema.getDataSource(), "dataSource");
    Preconditions.checkNotNull(dataSchema.getGranularitySpec(), "granularitySpec");

    InputRowParser parser = dataSchema.getParser();
    Preconditions.checkNotNull(parser.getDimensionsSpec(), "dimensionsSpec");
    Preconditions.checkNotNull(parser.getTimestampSpec(), "timestampSpec");

    HadoopTuningConfig tuningConfig = schema.getTuningConfig();
    Preconditions.checkNotNull(tuningConfig.getVersion(), "version");
    Preconditions.checkNotNull(tuningConfig.getWorkingPath(), "workingPath");

    Preconditions.checkNotNull(pathSpec, "inputSpec");
    Preconditions.checkNotNull(schema.getIOConfig().getSegmentOutputPath(), "segmentOutputPath");
  }

  public List<String> extractForwardingColumns()
  {
    List<String> dimensions = extractCommonDimensions();
    SettlingConfig settlingConfig = schema.getSettlingConfig();
    if (settlingConfig != null) {
      if (!dimensions.contains(settlingConfig.getParamNameColumn())) {
        dimensions.add(settlingConfig.getParamNameColumn());
      }
      if (!dimensions.contains(settlingConfig.getParamValueColumn())) {
        dimensions.add(settlingConfig.getParamValueColumn());
      }
    }
    return dimensions;
  }

  public List<String> extractFinalDimensions()
  {
    List<String> dimensions = extractCommonDimensions();
    SettlingConfig settlingConfig = schema.getSettlingConfig();
    if (settlingConfig != null) {
      if (!dimensions.contains(settlingConfig.getParamNameColumn())) {
        dimensions.add(settlingConfig.getParamNameColumn());
      }
    }
    return dimensions;
  }

  private List<String> extractCommonDimensions()
  {
    List<String> dimensions = Lists.newArrayList(getParser().getDimensionsSpec().getDimensionNames());
    if (pathSpec instanceof PartitionPathSpec) {
      PartitionPathSpec partitionPathSpec = (PartitionPathSpec) pathSpec;
      for (String partitionColumn : partitionPathSpec.getPartitionColumns()) {
        if (!dimensions.contains(partitionColumn)) {
          dimensions.add(partitionColumn);
        }
      }
    }
    return dimensions;
  }
}
