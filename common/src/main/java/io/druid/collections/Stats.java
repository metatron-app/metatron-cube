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

package io.druid.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.function.Function;

public class Stats
{
  private final Map<String, Counter> counterMap = Maps.newConcurrentMap();

  public Map<String, Object> stats()
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder();
    for (Map.Entry<String, Counter> entry : counterMap.entrySet()) {
      builder.put(entry.getKey(), entry.getValue().value());
    }
    return builder.build();
  }

  public boolean hasStats(String name)
  {
    return counterMap.containsKey(name);
  }

  public L ofLong(String name)
  {
    return (L) counterMap.computeIfAbsent(name, new Function<String, L>()
    {
      @Override
      public L apply(String name)
      {
        return new L();
      }
    });
  }

  public D ofDouble(String name)
  {
    return (D) counterMap.computeIfAbsent(name, new Function<String, D>()
    {
      @Override
      public D apply(String name)
      {
        return new D();
      }
    });
  }

  public static interface Counter
  {
    Number value();
  }

  public static class L implements Counter
  {
    private long counter;

    public void increase(long v)
    {
      counter += v;
    }

    public void set(long v)
    {
      counter = v;
    }

    public long get()
    {
      return counter;
    }

    @Override
    public Number value()
    {
      return counter;
    }
  }

  public static class D implements Counter
  {
    private double counter;

    public void increase(double v)
    {
      counter += v;
    }

    public void set(double v)
    {
      counter = v;
    }

    public double get()
    {
      return counter;
    }

    @Override
    public Number value()
    {
      return counter;
    }
  }
}
