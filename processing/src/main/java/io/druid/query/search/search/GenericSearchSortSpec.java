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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Comparators;
import io.druid.query.ordering.StringComparators;
import io.druid.query.ordering.StringComparators.StringComparator;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class GenericSearchSortSpec implements SearchSortSpec
{
  private final String ordering;
  private final List<String> orderingSpecs;
  private final Comparator<SearchHit> comparator;

  @JsonCreator
  public GenericSearchSortSpec(
      @JsonProperty("ordering") String ordering,
      @JsonProperty("orderingSpecs") List<String> orderingSpecs
  )
  {
    if (ordering != null) {
      final StringComparator stringComp = StringComparators.makeComparator(ordering);
      this.ordering = ordering;
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
      this.orderingSpecs = null;
    } else {
      Ordering<SearchHit> complex = null;
      for (String spec : Preconditions.checkNotNull(orderingSpecs)) {
        Comparator<SearchHit> comparator = toSearchHitComparator(spec.split(":"));
        complex = complex == null ? Ordering.from(comparator) : complex.compound(comparator);
      }
      this.comparator = Preconditions.checkNotNull(complex);
      this.orderingSpecs = orderingSpecs;
      this.ordering = null;
    }
  }

  public GenericSearchSortSpec(String ordering)
  {
    this(ordering, null);
  }

  public GenericSearchSortSpec(List<String> orderingSpecs)
  {
    this(null, orderingSpecs);
  }

  private Comparator<SearchHit> toSearchHitComparator(String[] specs)
  {
    Comparator<SearchHit> comparator;
    switch (specs[0]) {
      case "$value": {
        final StringComparator stringComp = specs.length == 1
                                            ? StringComparators.LEXICOGRAPHIC
                                            : StringComparators.makeComparator(specs[1]);
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
      case "$dimension": {
        final StringComparator stringComp = specs.length == 1
                                            ? StringComparators.LEXICOGRAPHIC
                                            : StringComparators.makeComparator(specs[1]);
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
      case "$count":
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
        if (specs.length == 1 || specs[1].equalsIgnoreCase("desc")) {
          comparator = Comparators.inverse(comparator);
        }
        break;
      default:
        throw new IllegalArgumentException("invalid target " + specs[0]);
    }
    return comparator;
  }

  @JsonProperty("ordering")
  public String getOrdering()
  {
    return ordering;
  }

  @JsonProperty("orderingSpecs")
  public List<String> getOrderingSpecs()
  {
    return orderingSpecs;
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return comparator;
  }

  @Override
  public Comparator<SearchHit> getMergeComparator()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return toString().getBytes();
  }

  @Override
  public String toString()
  {
    return "generic(" + (ordering != null ? ordering : orderingSpecs) + ")";
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof GenericSearchSortSpec &&
           Objects.equals(ordering, (((GenericSearchSortSpec) other).ordering)) &&
           Objects.equals(orderingSpecs, (((GenericSearchSortSpec) other).orderingSpecs));
  }

  @Override
  public int hashCode()
  {
    return (ordering != null ? ordering : orderingSpecs).hashCode();
  }
}
