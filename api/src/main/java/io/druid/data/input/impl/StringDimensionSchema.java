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
import com.google.common.collect.Maps;
import io.druid.data.ValueType;

import java.util.Map;

public class StringDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public static StringDimensionSchema create(String name) {
    int index = name.indexOf('?');
    int compareCacheEntry = -1;
    MultiValueHandling multiValueHandling = null;
    if (index > 0) {
      Map<String, String> parsed = parse(name.substring(index + 1));
      String cache = parsed.get("cache");
      try {
        compareCacheEntry = cache == null ? -1 : Integer.parseInt(cache);
      }
      catch (NumberFormatException e) {
        // ignore
      }
      String multiValue = parsed.get("multivalue");
      try {
        multiValueHandling = multiValue == null ? null : MultiValueHandling.valueOf(multiValue.toUpperCase());
      }
      catch (NumberFormatException e) {
        // ignore
      }
      name = name.substring(0, index);
    }
    return new StringDimensionSchema(name, multiValueHandling, compareCacheEntry);
  }

  private static Map<String, String> parse(String args)
  {
    Map<String, String> parsed = Maps.newHashMap();
    for (String split : args.split("&")) {
      int index = split.indexOf('=');
      parsed.put(split.substring(0, index).trim().toLowerCase(), split.substring(index + 1).trim());
    }
    return parsed;
  }

  private final int compareCacheEntry;

  @JsonCreator
  public StringDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("multiValueHandling") MultiValueHandling multiValueHandling,
      @JsonProperty("compareCacheEntry") int compareCacheEntry
  )
  {
    super(name, multiValueHandling);
    this.compareCacheEntry = compareCacheEntry;
  }

  public StringDimensionSchema(String name)
  {
    this(name, null, -1);
  }

  @Override
  public String getTypeName()
  {
    return DimensionSchema.STRING_TYPE_NAME;
  }

  @Override
  @JsonIgnore
  public ValueType getValueType()
  {
    return ValueType.STRING;
  }

  @JsonProperty
  public int getCompareCacheEntry()
  {
    return compareCacheEntry;
  }
}
