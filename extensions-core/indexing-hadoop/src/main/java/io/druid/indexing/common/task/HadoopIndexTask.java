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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.java.util.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.indexer.HadoopDruidDetermineConfigurationJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopDruidIndexerJob;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.IngestionMode;
import io.druid.indexer.Jobby;
import io.druid.indexer.MetadataStorageUpdaterJobHandler;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexing.common.TaskLock;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAppendingAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.hadoop.OverlordActionBasedUsedSegmentLister;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.AppendingGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

@JsonTypeName("index_hadoop")
public class HadoopIndexTask extends HadoopTask
{
  private static final Logger log = new Logger(HadoopIndexTask.class);

  static String createNewId(String prefix, HadoopIngestionSpec spec)
  {
    return String.format("%s_%s_%s", prefix, getTheDataSource(spec), new DateTime());
  }

  private static String getTheDataSource(HadoopIngestionSpec spec)
  {
    return spec.getDataSchema().getDataSource();
  }

  @SuppressWarnings("unchecked")
  private static String extractRequiredLockName(HadoopIngestionSpec spec)
  {
    DataSchema dataSchema = spec.getDataSchema();
    String schemaDataSource = dataSchema.getDataSource();
    HadoopTuningConfig tuningConfig = spec.getTuningConfig();
    Map<String, Object> pathSpec = spec.getIOConfig().getPathSpec();
    if (pathSpec.containsKey("elements") && !pathSpec.containsKey("type") || "hadoop".equals(pathSpec.get("type"))) {
      // simple validation
      Set<String> dataSources = Sets.newLinkedHashSet();
      for (Map elementSpec : (List<Map>) pathSpec.get("elements")) {
        String dataSourceName = Objects.toString(elementSpec.get("dataSource"), schemaDataSource);
        if (Strings.isNullOrEmpty(dataSourceName) || dataSourceName.indexOf(';') >= 0) {
          throw new IllegalArgumentException("Datasource name should not be empty or contain ';'");
        }
        if (!dataSources.contains(dataSourceName)) {
          dataSources.add(dataSourceName);
        }
      }
      if (dataSchema.getGranularitySpec().isAppending() || dataSources.size() > 1) {
        Preconditions.checkArgument(
            tuningConfig.getIngestionMode() == IngestionMode.REDUCE_MERGE,
            "generic type input spec only can be used with REDUCE_MERGE mode");
      }
      if (dataSchema.getGranularitySpec().isAppending()) {
        Preconditions.checkArgument(dataSources.size() == 1, "cannot append on multi datasources, for now");
      }
      return StringUtils.join(dataSources, ';');
    }
    return schemaDataSource;
  }

  @JsonIgnore
  private HadoopIngestionSpec spec;

  @JsonIgnore
  private final String classpathPrefix;

  @JsonIgnore
  private final ObjectMapper jsonMapper;

  @JsonIgnore
  private final String requiredLockName;

  /**
   * @param spec is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *             for creating Druid index segments. It may be modified.
   *             <p/>
   *             Here, we will ensure that the DbConnectorConfig field of the spec is set to null, such that the
   *             job does not push a list of published segments the database. Instead, we will use the method
   *             IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *             segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") HadoopIngestionSpec spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id != null ? id : createNewId("index_hadoop", spec),
        getTheDataSource(spec),
        hadoopDependencyCoordinates == null
        ? (hadoopCoordinates == null ? null : ImmutableList.of(hadoopCoordinates))
        : hadoopDependencyCoordinates,
        context
    );

    HadoopTuningConfig tuning = spec.getTuningConfig();
    PartitionsSpec partitionsSpec = tuning.getPartitionsSpec();
    if (tuning.getIngestionMode() == IngestionMode.REDUCE_MERGE && partitionsSpec.isDeterminingPartitions()) {
      // REDUCE_MERGE does not use partitions spec
      spec = spec.withTuningConfig(tuning.withPartitionsSpec(HashedPartitionsSpec.makeDefaultHashedPartitionsSpec()));
    }

    // Some HadoopIngestionSpec stuff doesn't make sense in the context of the indexing service
    Preconditions.checkArgument(
        spec.getIOConfig().getSegmentOutputPath() == null,
        "segmentOutputPath must be absent"
    );
    Preconditions.checkArgument(
        spec.getTuningConfig().getWorkingPath() == null,
        "workingPath must be absent"
    );
    Preconditions.checkArgument(
        spec.getIOConfig().getMetadataUpdateSpec() == null,
        "metadataUpdateSpec must be absent"
    );

    this.spec = spec;
    this.classpathPrefix = classpathPrefix;
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "null ObjectMapper");
    this.requiredLockName = extractRequiredLockName(spec);
  }

  @Override
  public String getRequiredLockName()
  {
    return requiredLockName;
  }

  @Override
  public String getType()
  {
    return "index_hadoop";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    Optional<SortedSet<Interval>> intervals = spec.getDataSchema().getGranularitySpec().bucketIntervals();
    if (intervals.isPresent()) {
      SortedSet<Interval> buckets = intervals.get();
      Interval interval = JodaUtils.umbrellaInterval(JodaUtils.condenseIntervals(buckets));
      log.info("Checking lock of task %s on interval.. %s (%d buckets)", getId(), interval, buckets.size());
      return taskActionClient.submit(new LockTryAcquireAction(interval)) != null;
    } else {
      return true;
    }
  }

  @JsonProperty("spec")
  public HadoopIngestionSpec getSpec()
  {
    return spec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getHadoopDependencyCoordinates()
  {
    return super.getHadoopDependencyCoordinates();
  }

  @JsonProperty
  @Override
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final ClassLoader loader = buildClassLoader(toolbox);
    boolean determineIntervals = !spec.getDataSchema().getGranularitySpec().bucketIntervals().isPresent();

    final TaskConfig taskConfig = toolbox.getConfig();
    if (!GuavaUtils.isNullOrEmpty(taskConfig.getDefaultHadoopJobProperties())) {
      HadoopTuningConfig tunningConfig = spec.getTuningConfig();
      spec = spec.withTuningConfig(
          tunningConfig.withDefaultJobProeprties(taskConfig.getDefaultHadoopJobProperties())
      );
    }
    spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
        spec,
        jsonMapper,
        new OverlordActionBasedUsedSegmentLister(toolbox)
    );

    final String dataSource = getDataSource();
    final String config = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopDetermineConfigInnerProcessing",
        new String[]{
            toolbox.getObjectMapper().writeValueAsString(spec),
            taskConfig.getHadoopWorkingPath(),
            toolbox.getSegmentPusher().getPathForHadoop()
        },
        loader
    );

    HadoopIngestionSpec indexerSchema = toolbox
        .getObjectMapper()
        .readValue(config, HadoopIngestionSpec.class);

    DataSchema dataSchema = indexerSchema.getDataSchema();
    GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    List<Interval> intervals = JodaUtils.condenseIntervals(granularitySpec.bucketIntervals().get());

    TaskActionClient actionClient = toolbox.getTaskActionClient();

    // We should have a lock from before we started running only if interval was specified
    TaskLock lock;
    if (determineIntervals) {
      lock = actionClient.submit(new LockAcquireAction(JodaUtils.umbrellaInterval(intervals)));
    } else {
      lock = Iterables.get(getTaskLocks(toolbox), 0);
    }
    if (granularitySpec.isAppending()) {
      List<SegmentDescriptor> descriptors = actionClient.submit(
          new SegmentAppendingAction(dataSource, intervals, granularitySpec.getSegmentGranularity())
      );
      AppendingGranularitySpec appendingSpec = new AppendingGranularitySpec(granularitySpec, descriptors);
      indexerSchema = indexerSchema.withDataSchema(dataSchema.withGranularitySpec(appendingSpec));
      log.info("Granularity spec is rewritten as %s", appendingSpec);
    }

    String version = lock.getVersion();
    log.info("Setting version to: %s", version);

    final String segments = invokeForeignLoader(
        "io.druid.indexing.common.task.HadoopIndexTask$HadoopIndexGeneratorInnerProcessing",
        new String[]{
            toolbox.getObjectMapper().writeValueAsString(indexerSchema),
            version
        },
        loader
    );

    if (segments != null) {
      List<DataSegment> publishedSegments = toolbox.getObjectMapper().readValue(
          segments,
          new TypeReference<List<DataSegment>>()
          {
          }
      );

      toolbox.publishSegments(publishedSegments);
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId(), "No segments found");
    }
  }

  public static class HadoopIndexGeneratorInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      String version = args[1];

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.JSON_MAPPER
          .readValue(
              schema,
              HadoopIngestionSpec.class
          );
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(
          theSchema
              .withTuningConfig(theSchema.getTuningConfig().withVersion(version))
      );

      // MetadataStorageUpdaterJobHandler is only needed when running standalone without indexing service
      // In that case the whatever runs the Hadoop Index Task must ensure MetadataStorageUpdaterJobHandler
      // can be injected based on the configuration given in config.getSchema().getIOConfig().getMetadataUpdateSpec()
      final MetadataStorageUpdaterJobHandler maybeHandler;
      if (config.isUpdaterJobSpecSet()) {
        maybeHandler = HadoopTask.injector.getInstance(MetadataStorageUpdaterJobHandler.class);
      } else {
        maybeHandler = null;
      }
      HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(config, maybeHandler);

      log.info("Starting a hadoop index generator job...");
      if (job.run()) {
        List<DataSegment> publishedSegments = job.getPublishedSegments();
        if (GuavaUtils.isNullOrEmpty(publishedSegments)) {
          String stats = HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(job.getIndexJobStats());
          throw new IllegalStateException("No segments found.. " + stats);
        }
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(publishedSegments);
      }

      return null;
    }
  }

  public static class HadoopDetermineConfigInnerProcessing
  {
    public static String runTask(String[] args) throws Exception
    {
      final String schema = args[0];
      final String workingPath = args[1];
      final String segmentOutputPath = args[2];

      final HadoopIngestionSpec theSchema = HadoopDruidIndexerConfig.JSON_MAPPER
          .readValue(
              schema,
              HadoopIngestionSpec.class
          );
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(
          theSchema
              .withIOConfig(theSchema.getIOConfig().withSegmentOutputPath(segmentOutputPath))
              .withTuningConfig(theSchema.getTuningConfig().withWorkingPath(workingPath))
      );

      Jobby job = new HadoopDruidDetermineConfigurationJob(config);

      log.info("Starting a hadoop determine configuration job...");
      if (job.run()) {
        return HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(config.getSchema());
      }

      return null;
    }
  }
}
