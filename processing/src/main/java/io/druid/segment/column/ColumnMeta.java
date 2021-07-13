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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;

import java.util.Map;

public class ColumnMeta
{
  final ValueDesc valueType;
  final boolean hasMultipleValues;
  final Map<String, String> descs;
  final Map<String, Object> stats;

  @JsonCreator
  public ColumnMeta(
      @JsonProperty("valueType") ValueDesc valueType,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("descs") Map<String, String> descs,
      @JsonProperty("stats") Map<String, Object> stats
  )
  {
    this.valueType = Preconditions.checkNotNull(valueType);
    this.hasMultipleValues = hasMultipleValues;
    this.descs = descs;
    this.stats = stats;
  }

  @JsonProperty
  public ValueDesc getValueType()
  {
    return valueType;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getDescs()
  {
    return descs;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getStats()
  {
    return stats;
  }
}
