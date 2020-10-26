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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Evaluation;
import io.druid.data.input.InputRow;
import io.druid.data.input.Validation;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.granularity.Granularity;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.partitions.DetermineSizeBasedPartitionsJob;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.path.PartitionPathSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.jackson.FunctionModule;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
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
import java.util.Arrays;
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
  public static final HadoopKerberosConfig HADOOP_KERBEROS_CONFIG;

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
                JsonConfigProvider.bind(binder, "druid.hadoop.security.kerberos", HadoopKerberosConfig.class);
              }
            },
            new IndexingHadoopModule(),
            new FunctionModule()
        )
    );
    JSON_MAPPER = injector.getInstance(ObjectMapper.class);
    INDEX_IO = injector.getInstance(IndexIO.class);
    INDEX_MERGER = injector.getInstance(IndexMerger.class);
    INDEX_MERGER_V9 = injector.getInstance(IndexMergerV9.class);
    HADOOP_KERBEROS_CONFIG = injector.getInstance(HadoopKerberosConfig.class);
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
      return fromMap(HadoopDruidIndexerConfig.JSON_MAPPER.readValue(file, ObjectMappers.MAP_REF));
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
      return fromMap(HadoopDruidIndexerConfig.JSON_MAPPER.readValue(str,ObjectMappers.MAP_REF));
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

      return fromMap(HadoopDruidIndexerConfig.JSON_MAPPER.readValue(reader, ObjectMappers.MAP_REF));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static HadoopDruidIndexerConfig fromConfiguration(Configuration conf)
  {
    return fromConfiguration(conf, true);
  }

  public static HadoopDruidIndexerConfig fromConfiguration(Configuration conf, boolean verify)
  {
    final HadoopDruidIndexerConfig retVal = fromString(conf.get(CONFIG_PROPERTY));
    if (verify) {
      retVal.verify();
    }
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
              ObjectMappers.excludeNulls(JSON_MAPPER).convertValue(pathSpec, Map.class)
          )
      );
    }
    this.schema = spec;
    this.pathSpec = pathSpec;
    for (Map.Entry<Long, List<HadoopyShardSpec>> entry : spec.getTuningConfig().getShardSpecs().entrySet()) {
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        continue;
      }
      final ShardSpec actualSpec = entry.getValue().get(0).getActualSpecWithDefault();
      shardSpecLookups.put(
          entry.getKey(), actualSpec.getLookup(
              Lists.transform(
                  entry.getValue(), new Function<HadoopyShardSpec, ShardSpec>()
                  {
                    @Override
                    public ShardSpec apply(HadoopyShardSpec input)
                    {
                      return input.getActualSpecWithDefault();
                    }
                  }
              )
          )
      );

      Map<ShardSpec, HadoopyShardSpec> innerHadoopShardSpecLookup = Maps.newHashMap();
      for (HadoopyShardSpec hadoopyShardSpec : entry.getValue()) {
        innerHadoopShardSpecLookup.put(hadoopyShardSpec.getActualSpecWithDefault(), hadoopyShardSpec);
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

  public int getNumReducer(int numSegments) throws IOException
  {
    HadoopTuningConfig config = schema.getTuningConfig();
    if (config.getMinReducer() >= config.getMaxReducer()) {
      return config.getMinReducer();  // forced
    }
    if (numSegments < 0) {
      numSegments = Iterables.size(getAllBuckets());
    }
    if (numSegments == 0) {
      return 0;   //no segments?
    }
    int minReducer = config.getMinReducer();
    int maxReducer = Math.min(numSegments, config.getMaxReducer());
    if (minReducer >= maxReducer) {
      return minReducer;
    }
    if (config.getBytesPerReducer() > 0) {
      long totalSize = DetermineSizeBasedPartitionsJob.getTotalSize(this);
      if (totalSize > 0) {
        int numReducer = (int) Math.ceil((double) totalSize / config.getBytesPerReducer());
        return Math.max(Math.min(numReducer, maxReducer), minReducer);
      }
    }
    return maxReducer;
  }

  public void setVersion(String version)
  {
    setTuningConfig(schema.getTuningConfig().withVersion(version));
  }

  public void setShardSpecs(Map<Long, List<HadoopyShardSpec>> shardSpecs)
  {
    setTuningConfig(schema.getTuningConfig().withShardSpecs(shardSpecs));
  }

  public void setTuningConfig(HadoopTuningConfig config)
  {
    this.schema = schema.withTuningConfig(config);
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

  public HadoopTuningConfig getTuningConfig()
  {
    return schema.getTuningConfig();
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
    return schema.getParser(JSON_MAPPER);
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
                                                     rollupGran.bucketStart(inputRow.getTimestampFromEpoch()),
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

  public Iterable<Bucket> getAllBuckets()
  {
    Optional<Set<Interval>> intervals = getSegmentGranularIntervals();
    if (intervals.isPresent()) {
      return FunctionalIterable
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
      );
    } else {
      return Arrays.asList();
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
    final ObjectMapper mapper = ObjectMappers.excludeNulls(JSON_MAPPER);

    try {
      log.info("Running with config:%n%s", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    DataSchema dataSchema = schema.getDataSchema();
    Preconditions.checkNotNull(dataSchema.getDataSource(), "dataSource");
    Preconditions.checkNotNull(dataSchema.getGranularitySpec(), "granularitySpec");

    InputRowParser parser = schema.getParser(mapper);
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
    return extractCommonDimensions();
  }

  public List<String> extractFinalDimensions()
  {
    return extractCommonDimensions();
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

  // for projection pushdown
  public Set<String> getRequiredColumnNames()
  {
    Set<String> required = Sets.newHashSet();

    InputRowParser parser = getParser();
    List<DimensionSchema> dimensionSchema = parser.getDimensionsSpec().getDimensions();
    if (dimensionSchema.isEmpty()) {
      return null;
    }
    required.addAll(parser.getTimestampSpec().getRequiredColumns());

    for (DimensionSchema dimension : dimensionSchema) {
      required.add(dimension.getName());
    }
    for (AggregatorFactory agg : schema.getDataSchema().getAggregators()) {
      required.addAll(agg.requiredFields());
    }
    for (Evaluation evaluation : schema.getDataSchema().getEvaluations()) {
      for (String expression : evaluation.getExpressions()) {
        required.addAll(Parser.findRequiredBindings(expression));
      }
    }
    for (Validation validation : schema.getDataSchema().getValidations()) {
      for (String expression : validation.getExclusions()) {
        required.addAll(Parser.findRequiredBindings(expression));
      }
    }
    return required;
  }
}
