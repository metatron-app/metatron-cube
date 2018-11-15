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

package io.druid.query.ordering;

import com.google.common.collect.Ordering;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;

import java.util.Comparator;
import java.util.List;

/**
 */
public class Comparators
{
  public static Comparator createGeneric(String name, Comparator defaultValue)
  {
    if (StringUtils.isNullOrEmpty(name)) {
      return defaultValue;
    }
    boolean descending = false;
    String lowerCased = name.toLowerCase();
    if (lowerCased.endsWith(":asc")) {
      name = name.substring(0, name.length() - 4);
    } else if (lowerCased.endsWith(":desc")) {
      name = name.substring(0, name.length() - 5);
      descending = true;
    }
    Comparator comparator = createString(name, defaultValue);
    return descending ? Ordering.from(defaultValue).reverse() : comparator;
  }

  private static Comparator createString(String name, Comparator defaultValue)
  {
    if (StringUtils.isNullOrEmpty(name)) {
      return defaultValue;
    }
    ValueDesc type = ValueDesc.of(name);
    if (type.isPrimitive()) {
      return type.comparator();
    }
    Comparator comparator = StringComparators.tryMakeComparator(name, null);
    if (comparator == null) {
      return defaultValue;
    }
    return comparator;
  }

  public static Comparator<Object[]> toArrayComparator(List<Comparator> comparators)
  {
    return toArrayComparator(comparators.toArray(new Comparator[comparators.size()]));
  }

  public static Comparator<Object[]> toArrayComparator(final Comparator[] cx)
  {
    return new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public int compare(Object[] o1, Object[] o2)
      {
        int compare = 0;
        for (int i = 0; compare == 0 && i < cx.length; i++) {
          compare = cx[i].compare(o1[i], o2[i]);
        }
        return compare;
      }
    };
  }

  public static Comparator<Object[]> toArrayComparator(final int[] indices)
  {
    return new Comparator<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
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

  public static int compareNF(Comparable[] d1, Comparable[] d2)
  {
    int compare = 0;
    for (int i = 0; i < d1.length && compare == 0; i++) {
      compare = compareNF(d1[i], d2[i]);
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

  public static <T> Ordering<T> compound(final List<Comparator<T>> comparators)
  {
    if (comparators.size() == 1) {
      return Ordering.from(comparators.get(0));
    }
    return Ordering.from(new Comparator<T>()
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
    });
  }
}
