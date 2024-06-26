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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.ValueDesc;

/**
 */
@JsonTypeName("listFold")
public class ListFoldingAggregatorFactory extends ListAggregatorFactory
{
  public ListFoldingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("inputType") ValueDesc inputType,
      @JsonProperty("limit") int limit,
      @JsonProperty("dedup") boolean dedup,
      @JsonProperty("sort") boolean sort
  )
  {
    super(
        name == null ? fieldName : name,
        fieldName == null ? name : fieldName,
        inputType == null ? ValueDesc.STRING_ARRAY : inputType.isArray() ? inputType : ValueDesc.ofArray(inputType),
        limit,
        dedup,
        sort
    );
  }
}
