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

package io.druid.client;

import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;

import java.util.Map;

/**
 */
public class ImmutableDruidDataSource
{
  private final String name;
  private final ImmutableMap<String, String> properties;
  private final ImmutableMap<String, DataSegment> segmentsMap;

  public ImmutableDruidDataSource(
      String name,
      ImmutableMap<String, String> properties,
      ImmutableMap<String, DataSegment> segmentsMap
  )
  {
    this.name = name;
    this.properties = properties;
    this.segmentsMap = segmentsMap;
  }

  public String getName()
  {
    return name;
  }

  public Map<String, String> getProperties()
  {
    return properties;
  }

  public boolean isEmpty()
  {
    return segmentsMap.isEmpty();
  }

  public int size()
  {
    return segmentsMap.size();
  }

  public Iterable<DataSegment> getSegments()
  {
    return segmentsMap.values();
  }

  @Override
  public boolean equals(Object obj)
  {
    ImmutableDruidDataSource other = (ImmutableDruidDataSource) obj;
    return name.equals(other.name) &&
           properties.equals(other.properties) &&
           segmentsMap.equals(other.segmentsMap);
  }
}
