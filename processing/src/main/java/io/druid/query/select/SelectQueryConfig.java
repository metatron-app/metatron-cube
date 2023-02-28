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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class SelectQueryConfig
{
  @JsonProperty
  private boolean useDateTime = false;

  @JsonProperty
  private int optimizeSegmentThreshold = 2;

  @JsonProperty
  private int maxThreshold = 1000000;

  @JsonProperty
  private boolean useBulkRow = true;

  @JsonProperty
  private boolean useRawUTF8 = true;

  public boolean isUseDateTime()
  {
    return useDateTime;
  }

  public int getOptimizeSegmentThreshold()
  {
    return optimizeSegmentThreshold;
  }

  public int getMaxThreshold()
  {
    return maxThreshold;
  }

  public boolean isUseBulkRow()
  {
    return useBulkRow;
  }

  public void setUseBulkRow(boolean useBulkRow)
  {
    this.useBulkRow = useBulkRow;
  }

  public boolean isUseRawUTF8()
  {
    return useRawUTF8;
  }

  public void setUseRawUTF8(boolean useRawUTF8)
  {
    this.useRawUTF8 = useRawUTF8;
  }
}
