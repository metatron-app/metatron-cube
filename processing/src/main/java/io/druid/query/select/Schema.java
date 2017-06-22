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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 */
public class Schema
{
  private final String[] columnNames;
  private final String[] columnTypes;

  @JsonCreator
  public Schema(
      @JsonProperty("columnNames") String[] columnNames,
      @JsonProperty("columnTypes") String[] columnTypes
  )
  {
    this.columnNames = Preconditions.checkNotNull(columnNames);
    this.columnTypes = Preconditions.checkNotNull(columnTypes);
    Preconditions.checkArgument(columnNames.length == columnTypes.length);
  }

  @JsonProperty
  public String[] getColumnNames()
  {
    return columnNames;
  }

  @JsonProperty
  public String[] getColumnTypes()
  {
    return columnTypes;
  }

  public int size()
  {
    return columnNames.length;
  }

  @Override
  public String toString()
  {
    return "Schema{" +
           "columnNames=" + Arrays.toString(columnNames) +
           ", columnTypes=" + Arrays.toString(columnTypes) +
           '}';
  }
}
