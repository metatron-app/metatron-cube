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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;

public class StringDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public static StringDimensionSchema create(String name)
  {
    int index = name.indexOf('?');
    if (index < 0) {
      return new StringDimensionSchema(name, null);
    }
    return new StringDimensionSchema(
        name.substring(0, index),
        MultiValueHandling.fromString(name.substring(index + 1))
    );
  }

  @JsonCreator
  public StringDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("multiValueHandling") MultiValueHandling multiValueHandling
  )
  {
    super(name, multiValueHandling);
  }

  public StringDimensionSchema(String name)
  {
    this(name, null);
  }

  @Override
  public String getTypeName()
  {
    return ValueDesc.STRING_TYPE;
  }

  @Override
  @JsonIgnore
  public ValueType getValueType()
  {
    return ValueType.STRING;
  }
}
