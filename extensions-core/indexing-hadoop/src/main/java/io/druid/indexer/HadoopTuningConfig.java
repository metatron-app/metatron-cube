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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.IndexSpec;
import io.druid.segment.incremental.BaseTuningConfig;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig extends BaseTuningConfig
{
  private static final PartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.makeDefaultHashedPartitionsSpec();
  private static final Map<Long, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.of();
  private static final boolean DEFAULT_USE_COMBINER = false;
  private static final int DEFAULT_MIN_REDUCER = 1;
  private static final int DEFAULT_MAX_REDUCER = 100;
  private static final int DEFAULT_SCATTER_PARAM = -1;
  private static final long DEFAULT_BYTES_PER_REDUCER = -1;
  private static final int DEFAULT_NUM_BACKGROUND_PERSIST_THREADS = 0;

  public static HadoopTuningConfig makeDefaultTuningConfig()
  {
    return new HadoopTuningConfig(
        null,
        new DateTime().toString(),
        DEFAULT_PARTITIONS_SPEC,
        DEFAULT_SHARD_SPECS,
        null,
        null,
        null,
        null,
        false,
        true,
        false,
        false,
        null,
        null,
        false,
        false,
        DEFAULT_MIN_REDUCER,
        DEFAULT_MAX_REDUCER,
        DEFAULT_SCATTER_PARAM,
        DEFAULT_BYTES_PER_REDUCER,
        null,
        DEFAULT_NUM_BACKGROUND_PERSIST_THREADS
    );
  }

  private final String workingPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final Map<Long, List<HadoopyShardSpec>> shardSpecs;
  private final boolean leaveIntermediate;
  private final Boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final Map<String, String> jobProperties;
  private final IngestionMode ingestionMode;
  private final boolean combineText;
  private final boolean useCombiner;
  private final int minReducer;
  private final int maxReducer;
  private final int scatterParam;
  private final long bytesPerReducer;
  private final int numBackgroundPersistThreads;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") Map<Long, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      final @JsonProperty("maxShardLength") Long maxShardLength,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") Map<String, String> jobProperties,
      final @JsonProperty("ingestionMode") IngestionMode ingestionMode,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("useCombiner") Boolean useCombiner,
      final @JsonProperty("minReducer") Integer minReducer,
      final @JsonProperty("maxReducer") Integer maxReducer,
      final @JsonProperty("scatterParam") Integer scatterParam,
      final @JsonProperty("bytesPerReducer") Long bytesPerReducer,
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("numBackgroundPersistThreads") Integer numBackgroundPersistThreads
  )
  {
    super(indexSpec, maxRowsInMemory, maxOccupationInMemory, maxShardLength, buildV9Directly, ignoreInvalidRows);
    this.workingPath = workingPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec == null ? DEFAULT_PARTITIONS_SPEC : partitionsSpec;
    this.shardSpecs = shardSpecs == null ? DEFAULT_SHARD_SPECS : shardSpecs;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure == null || cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.jobProperties = jobProperties == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(jobProperties);
    this.ingestionMode = ingestionMode == null ? IngestionMode.MAPRED : ingestionMode;
    this.combineText = combineText;
    this.useCombiner = useCombiner == null ? DEFAULT_USE_COMBINER : useCombiner;
    this.minReducer = minReducer == null ? DEFAULT_MIN_REDUCER : Math.max(1, minReducer);
    this.maxReducer = maxReducer == null ? DEFAULT_MAX_REDUCER : Math.max(1, maxReducer);
    this.bytesPerReducer = bytesPerReducer == null ? DEFAULT_BYTES_PER_REDUCER : bytesPerReducer;
    this.scatterParam = scatterParam == null ? DEFAULT_SCATTER_PARAM : scatterParam;
    this.numBackgroundPersistThreads = numBackgroundPersistThreads == null ? DEFAULT_NUM_BACKGROUND_PERSIST_THREADS : numBackgroundPersistThreads;
    Preconditions.checkArgument(this.numBackgroundPersistThreads >= 0, "Not support persistBackgroundCount < 0");
    Preconditions.checkArgument(
        this.scatterParam <= 1 || this.ingestionMode == IngestionMode.REDUCE_MERGE,
        "scatter is supported only with REDUCE_MERGE"
    );
  }

  public HadoopTuningConfig(
      String workingPath,
      String version,
      PartitionsSpec partitionsSpec,
      Map<Long, List<HadoopyShardSpec>> shardSpecs,
      IndexSpec indexSpec,
      Integer maxRowsInMemory,
      boolean leaveIntermediate,
      Boolean cleanupOnFailure,
      boolean overwriteFiles,
      boolean ignoreInvalidRows,
      Map<String, String> jobProperties,
      boolean combineText,
      Boolean useCombiner,
      Integer maxRowsInMemoryCOMPAT,
      Boolean buildV9Directly,
      Integer numBackgroundPersistThreads
  )
  {
    this(
        workingPath,
        version,
        partitionsSpec,
        shardSpecs,
        indexSpec,
        maxRowsInMemory,
        null,
        null,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        null,
        combineText,
        useCombiner,
        null,
        null,
        null,
        null,
        buildV9Directly,
        numBackgroundPersistThreads
    );
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getWorkingPath()
  {
    return workingPath;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<Long, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  @JsonProperty
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  @JsonProperty
  public Boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, String> getJobProperties()
  {
    return jobProperties;
  }

  @JsonProperty
  public IngestionMode getIngestionMode()
  {
    return ingestionMode;
  }

  @JsonProperty
  public boolean isCombineText()
  {
    return combineText;
  }

  @JsonProperty
  public boolean getUseCombiner()
  {
    return useCombiner;
  }

  @JsonProperty
  public int getNumBackgroundPersistThreads()
  {
    return numBackgroundPersistThreads;
  }

  @JsonProperty
  public int getMinReducer()
  {
    return minReducer;
  }

  @JsonProperty
  public int getMaxReducer()
  {
    return maxReducer;
  }

  @JsonProperty
  public int getScatterParam()
  {
    return scatterParam;
  }

  @JsonProperty
  public long getBytesPerReducer()
  {
    return bytesPerReducer;
  }

  public HadoopTuningConfig withPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        shardSpecs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        minReducer,
        maxReducer,
        scatterParam,
        bytesPerReducer,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withWorkingPath(String path)
  {
    return new HadoopTuningConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        minReducer,
        maxReducer,
        scatterParam,
        bytesPerReducer,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withVersion(String ver)
  {
    return new HadoopTuningConfig(
        workingPath,
        ver,
        partitionsSpec,
        shardSpecs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        minReducer,
        maxReducer,
        scatterParam,
        bytesPerReducer,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withShardSpecs(Map<Long, List<HadoopyShardSpec>> specs)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        specs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        minReducer,
        maxReducer,
        scatterParam,
        bytesPerReducer,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withNumReducer(int numReducer)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        shardSpecs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        numReducer,
        numReducer,
        scatterParam,
        DEFAULT_BYTES_PER_REDUCER,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withDefaultJobProeprties(Map<String, String> defaultHadoopJobProperties)
  {
    Map<String, String> mergedProeprties = Maps.newHashMap(defaultHadoopJobProperties);
    mergedProeprties.putAll(jobProperties);

    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        shardSpecs,
        getIndexSpec(),
        getMaxRowsInMemory(),
        getMaxOccupationInMemory(),
        getMaxShardLength(),
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        isIgnoreInvalidRows(),
        mergedProeprties,
        ingestionMode,
        combineText,
        useCombiner,
        minReducer,
        maxReducer,
        scatterParam,
        DEFAULT_BYTES_PER_REDUCER,
        getBuildV9Directly(),
        numBackgroundPersistThreads
    );
  }
}
