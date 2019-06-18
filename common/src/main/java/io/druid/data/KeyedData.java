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

package io.druid.data;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

// depends only on key for equals/hashCode
public class KeyedData<K, V>
{
  public static <K, V> KeyedData<K, V> of(K lhs, V rhs)
  {
    return new KeyedData<>(lhs, rhs);
  }

  public final K key;
  public final V value;

  public KeyedData(K key, V value)
  {
    this.key = key;
    this.value = value;
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
    return Objects.equals(key, ((KeyedData) o).key);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(key);
  }

  @Override
  public String toString()
  {
    return "KeyedData{" +
           "key=" + key +
           ", value=" + value +
           '}';
  }

  public static class StringKeyed<V> extends KeyedData<String, V>
  {
    public static <V> StringKeyed<V> of(V rhs)
    {
      return of(rhs.toString(), rhs);
    }

    public static <V> StringKeyed<V> of(String lhs, V rhs)
    {
      return new StringKeyed<>(lhs, rhs);
    }

    public static <V> List<StringKeyed<V>> of(List<V> rhs)
    {
      List<StringKeyed<V>> list = Lists.newArrayList();
      for (V v : rhs) {
        list.add(of(v));
      }
      return list;
    }

    public static <V> List<V> values(List<StringKeyed<V>> keyedList)
    {
      List<V> values = Lists.newArrayList();
      for (StringKeyed<V> keyed : keyedList) {
        values.add(keyed.value());
      }
      return values;
    }

    private StringKeyed(String lhs, V rhs)
    {
      super(lhs, rhs);
    }

    public V value()
    {
      return value;
    }
  }
}
