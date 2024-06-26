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

package io.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.TuningConfig;

/**
 */
public class BaseTuningConfig implements TuningConfig
{
  public static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75000;
  public static final long DEFAULT_MAX_OCCUPATION_IN_MEMORY = -1;
  public static final Boolean DEFAULT_BUILD_V9_DIRECTLY = Boolean.TRUE;

  public static final BaseTuningConfig DEFAULT = new BaseTuningConfig(null, null, null, null, null, false);

  private final IndexSpec indexSpec;
  private final int maxRowsInMemory;
  private final long maxOccupationInMemory;
  private final long maxShardLength;
  private final boolean ignoreInvalidRows;
  private final boolean buildV9Directly;

  @JsonCreator
  public BaseTuningConfig(
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      final @JsonProperty("maxShardLength") Long maxShardLength,
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.indexSpec = indexSpec == null ? IndexSpec.DEFAULT : indexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null || maxRowsInMemory == 0 ?
                           DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    this.maxOccupationInMemory = maxOccupationInMemory == null || maxOccupationInMemory == 0 ?
                                 DEFAULT_MAX_OCCUPATION_IN_MEMORY : maxOccupationInMemory;
    this.maxShardLength = maxShardLength == null ? this.maxOccupationInMemory : maxShardLength;
    this.buildV9Directly = buildV9Directly == null ? DEFAULT_BUILD_V9_DIRECTLY : buildV9Directly;
    this.ignoreInvalidRows = ignoreInvalidRows;
  }

  @Override
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
  @JsonProperty
  public long getMaxOccupationInMemory()
  {
    return maxOccupationInMemory;
  }

  @JsonProperty
  public long getMaxShardLength()
  {
    return maxShardLength;
  }

  @Override
  @JsonProperty
  public boolean getBuildV9Directly()
  {
    return buildV9Directly;
  }

  @Override
  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  public BaseTuningConfig withIndexSpec(IndexSpec indexSpec)
  {
    return new BaseTuningConfig(
        indexSpec,
        maxRowsInMemory,
        maxOccupationInMemory,
        maxShardLength,
        buildV9Directly,
        ignoreInvalidRows
    );
  }
}
