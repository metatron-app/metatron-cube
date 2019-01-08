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

package io.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.TuningConfig;

/**
 */
public class BaseTuningConfig implements TuningConfig
{
  protected static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  protected static final int DEFAULT_ROW_FLUSH_BOUNDARY = 75000;

  private final IndexSpec indexSpec;
  private final int maxRowsInMemory;
  private final long maxOccupationInMemory;
  private final long maxShardLength;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;

  @JsonCreator
  public BaseTuningConfig(
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("maxOccupationInMemory") Long maxOccupationInMemory,
      final @JsonProperty("maxShardLength") Long maxShardLength,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_ROW_FLUSH_BOUNDARY : maxRowsInMemory;
    this.maxOccupationInMemory = maxOccupationInMemory == null ? -1 : maxOccupationInMemory;
    this.maxShardLength = maxShardLength == null ? 1L << 31 : maxShardLength;
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

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

  @Override
  @JsonIgnore
  public boolean isReportParseExceptions()
  {
    return !isIgnoreInvalidRows();
  }
}
