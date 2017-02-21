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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.guava.nary.BinaryFn;

import java.util.Map;

/**
 */
public class ColumnDesc
{
  static final BinaryFn<ColumnDesc, ColumnDesc, ColumnDesc> MERGER = new BinaryFn<ColumnDesc, ColumnDesc, ColumnDesc>() {
    @Override
    public ColumnDesc apply(ColumnDesc arg1, ColumnDesc arg2)
    {
      return arg1.update(arg2);
    }
  };

  private final String comment;
  private final Map<String, String> properties;
  private final Map<String, String> valueComments;

  @JsonCreator
  public ColumnDesc(
      @JsonProperty("comment") String comment,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("valueComments") Map<String, String> valueComments
  )
  {
    this.comment = comment;
    this.properties = properties;
    this.valueComments = valueComments;
  }

  @JsonProperty
  public String getComment()
  {
    return comment;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @JsonProperty
  public Map<String, String> getValueComments()
  {
    return valueComments;
  }

  @Override
  public String toString()
  {
    return "{" +
           "comment='" + comment + '\'' +
           ", properties=" + properties +
           ", valueComments=" + valueComments +
           '}';
  }

  public ColumnDesc update(ColumnDesc other)
  {
    Map<String, String> newProperties = TableDesc.merge(properties, other.properties, null);
    Map<String, String> newValues = TableDesc.merge(valueComments, other.valueComments, null);
    return new ColumnDesc(other.comment == null ? comment : other.comment, newProperties, newValues);
  }
}
