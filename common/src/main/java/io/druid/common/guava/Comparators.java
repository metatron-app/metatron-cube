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

package io.druid.common.guava;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;
import io.druid.data.ValueDesc;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Comparator;
import java.util.List;

/**
 */
public class Comparators
{
  public static Comparator toComparator(ValueDesc valueDesc)
  {
    if (valueDesc == null) {
      return GuavaUtils.NULL_FIRST_NATURAL;
    }
    switch (valueDesc.type()) {
      case FLOAT:
        return NULL_FIRST(new Comparator<Number>()
        {
          @Override
          public int compare(Number o1, Number o2)
          {
            return Float.compare(o1.floatValue(), o2.floatValue());
          }
        });
      case DOUBLE:
        return NULL_FIRST(new Comparator<Number>()
        {
          @Override
          public int compare(Number o1, Number o2)
          {
            return Double.compare(o1.doubleValue(), o2.doubleValue());
          }
        });
      case LONG:
        return NULL_FIRST(new Comparator<Number>()
        {
          @Override
          public int compare(Number o1, Number o2)
          {
            return Long.compare(o1.longValue(), o2.longValue());
          }
        });
      case BOOLEAN:
      case STRING:
      case DATETIME:
        return GuavaUtils.NULL_FIRST_NATURAL;
    }
    if (valueDesc.isArray()) {
      return NULL_FIRST(toListComparator(toComparator(valueDesc.subElement(null))));
    }
    if (valueDesc.isStruct()) {
      // todo
    }
    return GuavaUtils.NULL_FIRST_NATURAL;
  }

  @SuppressWarnings("unchecked")
  public static Comparator<List<Object>> toListComparator(Comparator elementComp)
  {
    return (List<Object> o1, List<Object> o2) ->
    {
      final int min = Math.min(o1.size(), o2.size());
      for (int i = 0; i < min; i++) {
        int ret = elementComp.compare(o1.get(i), o2.get(i));
        if (ret != 0) {
          return ret;
        }
      }
      return o1.size() - o2.size();
    };
  }

  public static Comparator<Object[]> NF_ARRAY = new Comparator<Object[]>()
  {
    @Override
    public int compare(Object[] o1, Object[] o2)
    {
      return compareNF(o1, o2);
    }
  };

  public static Comparator<Object[]> toArrayComparator(List<Comparator> comparators)
  {
    return toArrayComparator(comparators.toArray(new Comparator[0]));
  }

  public static Comparator<Object[]> toArrayComparator(final Comparator[] cx)
  {
    return toArrayComparator(cx, 0);
  }

  public static Comparator<Object[]> toArrayComparator(final Comparator[] cx, final int from)
  {
    return new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(Object[] o1, Object[] o2)
      {
        for (int i = from; i < cx.length; i++) {
          final int compare = cx[i].compare(o1[i], o2[i]);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
  }

  public static Comparator<Object[]> toArrayComparator(final int[] indices)
  {
    return new Comparator<Object[]>()
    {
      @Override
      public int compare(Object[] o1, Object[] o2)
      {
        int compare = 0;
        for (int i = 0; compare == 0 && i < indices.length; i++) {
          compare = compareNF((Comparable) o1[indices[i]], (Comparable) o2[indices[i]]);
        }
        return compare;
      }
    };
  }

  // from com.google.common.collect.NullsFirstOrdering
  private static final int RIGHT_IS_GREATER = -1;
  private static final int LEFT_IS_GREATER = 1;

  public static <T> Comparator<T> NULL_FIRST(final Comparator<T> comparator)
  {
    return (left, right) -> {
      if (left == right) {
        return 0;
      }
      if (left == null) {
        return RIGHT_IS_GREATER;
      }
      if (right == null) {
        return LEFT_IS_GREATER;
      }
      return comparator.compare(left, right);
    };
  }

  public static <T> Comparator<T> REVERT(final Comparator<T> comparator)
  {
    return (left, right) -> comparator.compare(right, left);
  }

  public static int compareNF(final Object[] d1, final Object[] d2)
  {
    int compare = 0;
    for (int i = 0; i < d1.length && compare == 0; i++) {
      compare = compareNF((Comparable) d1[i], (Comparable) d2[i]);
    }
    return compare;
  }

  @SuppressWarnings("unchecked")
  public static int compareNF(Comparable d1, Comparable d2)
  {
    if (d1 == d2) {
      return 0;
    }
    if (d1 == null) {
      return RIGHT_IS_GREATER;
    }
    if (d2 == null) {
      return LEFT_IS_GREATER;
    }
    return d1.compareTo(d2);
  }

  public static <T> Comparator<T> compound(final List<Comparator<T>> comparators)
  {
    if (comparators.isEmpty()) {
      return null;
    }
    if (comparators.size() == 1) {
      return comparators.get(0);
    }
    if (comparators.size() == 2) {
      final Comparator<T> comp1 = comparators.get(0);
      final Comparator<T> comp2 = comparators.get(1);
      return (left, right) -> {
        int compare = comp1.compare(left, right);
        if (compare == 0) {
          compare = comp2.compare(left, right);
        }
        return compare;
      };
    }
    return new Comparator<T>()
    {
      @Override
      public int compare(T o1, T o2)
      {
        for (Comparator<T> comparator : comparators) {
          int compare = comparator.compare(o1, o2);
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
  }

  @SafeVarargs
  public static <T, V> Comparator<T> explicit(Function<T, V> converter, V... values)
  {
    final Object2IntMap<V> ranks = new Object2IntOpenHashMap<V>();
    for (int i = 0; i < values.length; i++) {
      ranks.put(values[i], i);
    }
    return (l, r) -> Ints.compare(
        ranks.getOrDefault(converter.apply(l), values.length),
        ranks.getOrDefault(converter.apply(r), values.length)
    );
  }
}
