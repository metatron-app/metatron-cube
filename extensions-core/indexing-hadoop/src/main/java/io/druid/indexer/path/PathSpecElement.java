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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.InputFormat;
import org.joda.time.Interval;

import java.util.Objects;

/**
 */
public class PathSpecElement
{
  private final String dataSource;
  private final String paths;

   // not yet
  private final Class<? extends InputFormat> inputFormat;
  private final Interval interval;

  @JsonCreator
  public PathSpecElement(
      @JsonProperty("paths") String paths,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("inputFormat") Class<? extends InputFormat> inputFormat,
      @JsonProperty("interval") Interval interval
  )
  {
    this.paths = Preconditions.checkNotNull(paths, "path should not be null");
    this.dataSource = dataSource;
    this.inputFormat = inputFormat;
    this.interval = interval;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getPaths()
  {
    return paths;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Interval getInterval()
  {
    return interval;
  }

  public PathSpecElement withPaths(String paths)
  {
    return new PathSpecElement(paths, dataSource, inputFormat, interval);
  }

  public PathSpecElement withDataSource(String dataSource)
  {
    return new PathSpecElement(paths, dataSource, inputFormat, interval);
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

    PathSpecElement that = (PathSpecElement) o;

    if (!Objects.equals(dataSource, that.dataSource)) {
      return false;
    }
    if (!Objects.equals(paths, that.paths)) {
      return false;
    }
    if (!Objects.equals(inputFormat, that.inputFormat)) {
      return false;
    }
    if (!Objects.equals(interval, that.interval)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, paths, inputFormat, interval);
  }

  @Override
  public String toString()
  {
    return "{" +
           "dataSource='" + dataSource + '\'' +
           ", paths='" + paths + '\'' +
           (inputFormat == null ? "" : ", inputFormat=" + inputFormat) +
           (interval == null ? "" : ", interval=" + interval) +
           '}';
  }
}
