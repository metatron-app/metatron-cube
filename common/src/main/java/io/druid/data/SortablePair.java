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

package io.druid.data;

import java.util.Comparator;
import java.util.Map;

/**
 */
public final class SortablePair<K extends Comparable, V> extends Pair<K, V>
    implements Comparable<SortablePair<K, V>>
{
  public SortablePair(K lhs, V rhs)
  {
    super(lhs, rhs);
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(SortablePair<K, V> o)
  {
    return lhs.compareTo(o.lhs);
  }

  @SuppressWarnings("unchecked")
  public static <K extends Comparable, V> Comparator<Map.Entry<K, V>> KEY_COMP(boolean descending)
  {
    Comparator<Map.Entry<K, V>> comparator;
    if (descending) {
      comparator = new Comparator<Map.Entry<K, V>>()
      {
        @Override
        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
        {
          return o1.getKey().compareTo(o2.getKey());
        }
      };
    } else {
      comparator = new Comparator<Map.Entry<K, V>>()
      {
        @Override
        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
        {
          return o2.getKey().compareTo(o1.getKey());
        }
      };
    }
    return comparator;
  }
}
