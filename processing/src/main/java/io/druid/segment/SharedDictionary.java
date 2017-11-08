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

package io.druid.segment;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Provider;
import io.druid.segment.data.DictionaryLoader;

import java.util.List;
import java.util.Map;

/**
 */
public class SharedDictionary
{
  private final Map<String, Mapping> columns = Maps.newHashMap();

  public synchronized Mapping forColumn(String column)
  {
    Mapping mapping = columns.get(column);
    if (mapping == null) {
      columns.put(column, mapping = new Mapping());
    }
    return mapping;
  }

  public Provider<Mapping> asProvider(final String column)
  {
    return new Provider<Mapping>()
    {
      @Override
      public Mapping get()
      {
        return forColumn(column);
      }
    };
  }

  public static class Mapping<T>
  {
    private final Map<T, Integer> valueToId = Maps.newHashMap();
    private final List<T> idToValue = Lists.newArrayList();

    public synchronized int getId(T value)
    {
      Integer id = valueToId.get(value);
      if (id == null) {
        valueToId.put(value, id = valueToId.size());
        idToValue.add(value);
      }
      return id;
    }

    public synchronized T getValue(int id)
    {
      return idToValue.get(id);
    }

    public synchronized List<T> values()
    {
      return Lists.<T>newArrayList(idToValue);
    }

    public MappingCollector<T> collector(int size)
    {
      return new MappingCollector<>(this, size);
    }
  }

  public static class MappingCollector<T> implements DictionaryLoader.Collector<T>
  {
    private final Mapping<T> mapping;
    private final int[] map;

    private MappingCollector(Mapping<T> mapping, int size)
    {
      this.mapping = mapping;
      this.map = new int[size];
    }

    @Override
    public void collect(int id, T value)
    {
      map[id] = mapping.getId(value);
    }

    public int[] mapping()
    {
      return map;
    }
  }
}
