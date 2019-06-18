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

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import io.druid.common.guava.GuavaUtils;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class Pair<K, V> extends com.metamx.common.Pair<K, V> implements Map.Entry<K, V>
{
  public static <T1, T2> Pair<T1, T2> of(T1 lhs, T2 rhs)
  {
    return new Pair<>(lhs, rhs);
  }

  public Pair(K lhs, V rhs)
  {
    super(lhs, rhs);
  }

  @Override
  public K getKey()
  {
    return lhs;
  }

  @Override
  public V getValue()
  {
    return rhs;
  }

  @Override
  public V setValue(V value)
  {
    throw new UnsupportedOperationException("setValue");
  }

  public static <K extends Comparable, V> Comparator<Map.Entry<K, V>> KEY_COMP()
  {
    return KEY_COMP(GuavaUtils.<K>noNullableNatural());
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Comparator<Map.Entry<K, V>> KEY_COMP(final Comparator<K> comparator)
  {
    return new Comparator<Map.Entry<K, V>>()
    {
      @Override
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
      {
        return comparator.compare(o1.getKey(), o2.getKey());
      }
    };
  }

  public static <L> Function<Pair<L, ?>, L> lhs()
  {
    return new Function<Pair<L, ?>, L>()
    {
      @Override
      public L apply(Pair<L, ?> input)
      {
        return input.lhs;
      }
    };
  }

  public static <R> Function<Pair<?, R>, R> rhs()
  {
    return new Function<Pair<?, R>, R>()
    {
      @Override
      public R apply(Pair<?, R> input)
      {
        return input.rhs;
      }
    };
  }
}
