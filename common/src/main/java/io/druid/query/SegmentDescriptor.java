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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import java.util.Objects;

/**
*/
public class SegmentDescriptor
{
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final int partitionNumber;

  @JsonCreator
  public SegmentDescriptor(
      @JsonProperty("ds") String dataSource,
      @JsonProperty("itvl") Interval interval,
      @JsonProperty("ver") String version,
      @JsonProperty("part") int partitionNumber
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
    this.partitionNumber = partitionNumber;
  }

  @JsonProperty("ds")
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("itvl")
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty("ver")
  public String getVersion()
  {
    return version;
  }

  @JsonProperty("part")
  public int getPartitionNumber()
  {
    return partitionNumber;
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

    SegmentDescriptor that = (SegmentDescriptor) o;

    if (partitionNumber != that.partitionNumber) {
      return false;
    }
    if (!Objects.equals(dataSource, that.dataSource)) {
      return false;
    }
    if (!Objects.equals(interval, that.interval)) {
      return false;
    }
    if (!Objects.equals(version, that.version)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, interval, version, partitionNumber);
  }

  @Override
  public String toString()
  {
    return "SegmentDescriptor{" +
           "dataSource=" + dataSource +
           ", interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionNumber=" + partitionNumber +
           '}';
  }
}
