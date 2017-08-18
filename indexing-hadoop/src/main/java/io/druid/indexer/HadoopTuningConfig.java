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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.TuningConfig;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig implements TuningConfig
{
  private static final PartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.makeDefaultHashedPartitionsSpec();
  private static final Map<DateTime, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.of();
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_ROW_FLUSH_BOUNDARY = 75000;
  private static final boolean DEFAULT_USE_COMBINER = false;
  private static final int DEFAULT_MAX_REDUCER = 100;
  private static final Boolean DEFAULT_BUILD_V9_DIRECTLY = Boolean.FALSE;
  private static final int DEFAULT_NUM_BACKGROUND_PERSIST_THREADS = 0;

  public static HadoopTuningConfig makeDefaultTuningConfig()
  {
    return new HadoopTuningConfig(
        null,
        new DateTime().toString(),
        DEFAULT_PARTITIONS_SPEC,
        DEFAULT_SHARD_SPECS,
        DEFAULT_INDEX_SPEC,
        DEFAULT_ROW_FLUSH_BOUNDARY,
        null,
        null,
        false,
        true,
        false,
        false,
        false,
        null,
        null,
        false,
        false,
        DEFAULT_MAX_REDUCER,
        null,
        DEFAULT_BUILD_V9_DIRECTLY,
        DEFAULT_NUM_BACKGROUND_PERSIST_THREADS
    );
  }

  private final String workingPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final Map<DateTime, List<HadoopyShardSpec>> shardSpecs;
  private final IndexSpec indexSpec;
  private final int rowFlushBoundary;
  private final long maxOccupationInMemory;
  private final long maxShardLength;
  private final boolean leaveIntermediate;
  private final Boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;
  private final boolean assumeTimeSorted;
  private final Map<String, String> jobProperties;
  private final IngestionMode ingestionMode;
  private final boolean combineText;
  private final boolean useCombiner;
  private final int maxReducer;
  private final Boolean buildV9Directly;
  private final int numBackgroundPersistThreads;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      final @JsonProperty("maxShardLength") Long maxShardLength,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("assumeTimeSorted") boolean assumeTimeSorted,
      final @JsonProperty("jobProperties") Map<String, String> jobProperties,
      final @JsonProperty("ingestionMode") IngestionMode ingestionMode,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("useCombiner") Boolean useCombiner,
      final @JsonProperty("maxReducer") Integer maxReducer,
      // See https://github.com/druid-io/druid/pull/1922
      final @JsonProperty("rowFlushBoundary") Integer maxRowsInMemoryCOMPAT,
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("numBackgroundPersistThreads") Integer numBackgroundPersistThreads
  )
  {
    this.workingPath = workingPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec == null ? DEFAULT_PARTITIONS_SPEC : partitionsSpec;
    this.shardSpecs = shardSpecs == null ? DEFAULT_SHARD_SPECS : shardSpecs;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.rowFlushBoundary = maxRowsInMemory == null ? maxRowsInMemoryCOMPAT == null ?  DEFAULT_ROW_FLUSH_BOUNDARY : maxRowsInMemoryCOMPAT : maxRowsInMemory;
    this.maxOccupationInMemory = maxOccupationInMemory == null ? -1 : maxOccupationInMemory;
    this.maxShardLength = maxShardLength == null ? 2 << 30L : maxShardLength;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure == null ? true : cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.assumeTimeSorted = assumeTimeSorted;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.<String, String>of()
                          : ImmutableMap.copyOf(jobProperties));
    this.ingestionMode = ingestionMode == null ? IngestionMode.MAPRED : ingestionMode;
    this.combineText = combineText;
    this.useCombiner = useCombiner == null ? DEFAULT_USE_COMBINER : useCombiner;
    this.buildV9Directly = buildV9Directly == null ? DEFAULT_BUILD_V9_DIRECTLY : buildV9Directly;
    this.maxReducer = maxReducer == null ? DEFAULT_MAX_REDUCER : maxReducer;
    this.numBackgroundPersistThreads = numBackgroundPersistThreads == null ? DEFAULT_NUM_BACKGROUND_PERSIST_THREADS : numBackgroundPersistThreads;
    Preconditions.checkArgument(this.numBackgroundPersistThreads >= 0, "Not support persistBackgroundCount < 0");
  }

  public HadoopTuningConfig(
      String workingPath,
      String version,
      PartitionsSpec partitionsSpec,
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
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
        false,
        jobProperties,
        null,
        combineText,
        useCombiner,
        null,
        maxRowsInMemoryCOMPAT,
        buildV9Directly,
        numBackgroundPersistThreads
    );
  }

  @JsonProperty
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
  public Map<DateTime, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty("maxRowsInMemory")
  public int getRowFlushBoundary()
  {
    return rowFlushBoundary;
  }

  @JsonProperty("maxOccupationInMemory")
  public long getMaxOccupationInMemory()
  {
    return maxOccupationInMemory;
  }

  @JsonProperty("maxShardLength")
  public long getMaxShardLength()
  {
    return maxShardLength;
  }

  @JsonProperty
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  @JsonProperty
  public Boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  @JsonProperty
  public boolean isAssumeTimeSorted()
  {
    return assumeTimeSorted;
  }

  @JsonProperty
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
  public Boolean getBuildV9Directly() {
    return buildV9Directly;
  }

  @JsonProperty
  public int getNumBackgroundPersistThreads()
  {
    return numBackgroundPersistThreads;
  }

  @JsonProperty
  public int getMaxReducer()
  {
    return maxReducer;
  }

  public HadoopTuningConfig withWorkingPath(String path)
  {
    return new HadoopTuningConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        indexSpec,
        rowFlushBoundary,
        maxOccupationInMemory,
        maxShardLength,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        assumeTimeSorted,
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        maxReducer,
        null,
        buildV9Directly,
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
        indexSpec,
        rowFlushBoundary,
        maxOccupationInMemory,
        maxShardLength,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        assumeTimeSorted,
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        maxReducer,
        null,
        buildV9Directly,
        numBackgroundPersistThreads
    );
  }

  public HadoopTuningConfig withShardSpecs(Map<DateTime, List<HadoopyShardSpec>> specs)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        specs,
        indexSpec,
        rowFlushBoundary,
        maxOccupationInMemory,
        maxShardLength,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        assumeTimeSorted,
        jobProperties,
        ingestionMode,
        combineText,
        useCombiner,
        maxReducer,
        null,
        buildV9Directly,
        numBackgroundPersistThreads
    );
  }
}
