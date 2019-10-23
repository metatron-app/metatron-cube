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

package io.druid.query.search.search;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.ordering.StringComparators;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class SearchHitSort
{
  public static SearchHitSort valueOf(List<String> ordering)
  {
    return ordering == null || ordering.isEmpty() ? null : new SearchHitSort(ordering);
  }

  private final Comparator<SearchHit> comparator;
  private final boolean referencesCount;

  public SearchHitSort(List<String> ordering)
  {
    ordering = Preconditions.checkNotNull(ordering, "'ordering' cannot be null");
    if (ordering.size() == 1 && !ordering.get(0).startsWith("$")) {
      final Comparator stringComp = StringComparators.makeComparator(ordering.get(0));
      this.comparator = new Comparator<SearchHit>()
      {
        @Override
        public int compare(SearchHit o1, SearchHit o2)
        {
          String v1 = o1.getValue();
          String v2 = o2.getValue();
          int ret = stringComp.compare(v1, v2);
          if (ret == 0) {
            ret = o1.getDimension().compareTo(o2.getDimension());
          }
          return ret;
        }
      };
      this.referencesCount = false;
    } else {
      boolean referencesCount = false;
      Ordering<SearchHit> complex = null;
      for (String spec : ordering) {
        referencesCount |= spec.startsWith(SearchHit.COUNT);
        Comparator<SearchHit> comparator = toSearchHitComparator(spec.split(":"));
        complex = complex == null ? Ordering.from(comparator) : complex.compound(comparator);
      }
      this.comparator = Preconditions.checkNotNull(complex, "'ordering' is empty or invalid");
      this.referencesCount = referencesCount;
    }
  }

  @SuppressWarnings("unchecked")
  private Comparator<SearchHit> toSearchHitComparator(String[] specs)
  {
    boolean invert = false;
    Comparator<SearchHit> comparator;
    switch (specs[0]) {
      case SearchHit.VALUE: {
        final Comparator stringComp = makeBaseComparator(specs);
        comparator = new Comparator<SearchHit>()
        {
          @Override
          public int compare(SearchHit o1, SearchHit o2)
          {
            return stringComp.compare(o1.getValue(), o2.getValue());
          }
        };
        break;
      }
      case SearchHit.DIMENSION: {
        final Comparator stringComp = makeBaseComparator(specs);
        comparator = new Comparator<SearchHit>()
        {
          @Override
          public int compare(SearchHit o1, SearchHit o2)
          {
            return stringComp.compare(o1.getDimension(), o2.getDimension());
          }
        };
        break;
      }
      case SearchHit.COUNT:
        comparator = new Comparator<SearchHit>()
        {
          @Override
          public int compare(SearchHit o1, SearchHit o2)
          {
            final Integer count1 = o1.getCount();
            final Integer count2 = o2.getCount();
            if (Objects.equals(count1, count2)) {
              return 0;
            }
            if (count1 == null) {
              return -1;
            }
            if (count2 == null) {
              return 1;
            }
            return Ints.compare(count1, count2);
          }
        };
        break;
      default:
        throw new IllegalArgumentException("invalid target " + specs[0]);
    }
    for (int i = 1; i < specs.length; i++) {
      invert |= specs[i].equalsIgnoreCase("desc");
    }
    return invert ? Comparators.inverse(comparator) : comparator;
  }

  private Comparator makeBaseComparator(String[] specs)
  {
    Comparator stringComp = null;
    for (int i = 1; i < specs.length && stringComp == null; i++) {
      stringComp = StringComparators.tryMakeComparator(specs[i], StringComparators.LEXICOGRAPHIC);
    }
    if (stringComp == null) {
      stringComp = StringComparators.LEXICOGRAPHIC;
    }
    return stringComp;
  }

  public Comparator<SearchHit> getComparator()
  {
    return comparator;
  }

  public boolean sortOnCount()
  {
    return referencesCount;
  }
}
