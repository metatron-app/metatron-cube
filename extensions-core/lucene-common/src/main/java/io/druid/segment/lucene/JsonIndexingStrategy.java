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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;

import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("json")
public class JsonIndexingStrategy implements LuceneIndexingStrategy
{
  public static final String TYPE_NAME = "json";

  private final String fieldName;
  private final List<String> patterns;

  @JsonCreator
  public JsonIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("patterns") List<String> patterns
  )
  {
    this.fieldName = fieldName;
    this.patterns = patterns;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getPatterns()
  {
    return patterns;
  }

  @Override
  public String getFieldDescriptor()
  {
    return TYPE_NAME;
  }

  @Override
  public LuceneIndexingStrategy withFieldName(String fieldName)
  {
    return new JsonIndexingStrategy(fieldName, patterns);
  }

  @Override
  public LuceneFieldGenerator createIndexableField(ValueDesc type, Iterable<Object> values)
  {
    return Lucenes.makeJsonFieldGenerator(new DefaultObjectMapper(), fieldName, patterns);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JsonIndexingStrategy that = (JsonIndexingStrategy) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(patterns, that.patterns)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, patterns);
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
