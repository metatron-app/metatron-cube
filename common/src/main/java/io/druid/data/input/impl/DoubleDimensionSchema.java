/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;

public class DoubleDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public DoubleDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("multiValueHandling") MultiValueHandling multiValueHandling
  )
  {
    super(name, fieldName, multiValueHandling);
  }

  public DoubleDimensionSchema(String name)
  {
    this(name, null, null);
  }

  @Override
  public String getTypeName()
  {
    return ValueDesc.DOUBLE_TYPE;
  }

  @Override
  @JsonIgnore
  public ValueType getValueType()
  {
    return ValueType.DOUBLE;
  }
}
