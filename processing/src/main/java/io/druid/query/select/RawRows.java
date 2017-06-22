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
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class RawRows implements Comparable<RawRows>
{
  private final DateTime timestamp;
  private final Schema schema;
  private final List<Object[]> rows;

  @JsonCreator
  public RawRows(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("schema") Schema schema,
      @JsonProperty("rows") List<Object[]> rows
  )
  {
    this.timestamp = timestamp;
    this.schema = schema;
    this.rows = rows;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public List<Object[]> getRows()
  {
    return rows;
  }

  @Override
  public int compareTo(RawRows o)
  {
    return timestamp.compareTo(o.timestamp);
  }

  @Override
  public String toString()
  {
    return "RawRow{" +
           "timestamp=" + timestamp +
           ", schema=" + schema +
           ", rows=" + rows +
           '}';
  }
}
