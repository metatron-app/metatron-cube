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
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75000;
  private static final long DEFAULT_MAX_OCCUPATION_IN_MEMORY = -1;
  private static final Boolean DEFAULT_BUILD_V9_DIRECTLY = Boolean.TRUE;

  private final IndexSpec indexSpec;
  private final int maxRowsInMemory;
  private final long maxOccupationInMemory;
  private final boolean ignoreInvalidRows;
  private final boolean buildV9Directly;

  @JsonCreator
  public BaseTuningConfig(
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null || maxRowsInMemory == 0 ?
                           DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    this.maxOccupationInMemory = maxOccupationInMemory == null || maxOccupationInMemory == 0 ?
                                 DEFAULT_MAX_OCCUPATION_IN_MEMORY : maxOccupationInMemory;
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
}
