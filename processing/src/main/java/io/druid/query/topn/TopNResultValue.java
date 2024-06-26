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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNResultValue implements Iterable<Map<String, Object>>
{
  private final List<Map<String, Object>> value;

  @JsonCreator
  public TopNResultValue(List<Map<String, Object>> value)
  {
    this.value = value;
  }

  @JsonValue
  public List<Map<String, Object>> getValue()
  {
    return value;
  }

  @Override
  public Iterator<Map<String, Object>> iterator()
  {
    return value.iterator();
  }

  public int size()
  {
    return value.size();
  }

  public TopNResultValue limit(int limit)
  {
    return value.size() <= limit ? this : new TopNResultValue(Lists.newArrayList(value.subList(0, limit)));
  }

  @Override
  public String toString()
  {
    return "TopNResultValue{" +
           "value=" + value +
           '}';
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

    TopNResultValue that = (TopNResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
