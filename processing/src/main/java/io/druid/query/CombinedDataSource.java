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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.List;

public class CombinedDataSource implements DataSource
{
  public static CombinedDataSource of(DataSource... dataSources)
  {
    return new CombinedDataSource(Arrays.asList(dataSources));
  }

  private final List<DataSource> dataSources;

  @JsonCreator
  public CombinedDataSource(@JsonProperty("dataSources") List<DataSource> dataSources)
  {
    this.dataSources = Preconditions.checkNotNull(dataSources, "'dataSources' cannot be null");
  }

  @Override
  public List<String> getNames()
  {
    return ImmutableList.copyOf(Iterables.concat(Iterables.transform(dataSources, DataSource::getNames)));
  }

  @JsonProperty
  public List<DataSource> getDataSources()
  {
    return dataSources;
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

    CombinedDataSource that = (CombinedDataSource) o;

    if (!dataSources.equals(that.dataSources)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return dataSources.hashCode();
  }

  @Override
  public String toString()
  {
    return dataSources.toString();
  }
}