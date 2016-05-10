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
import io.druid.query.ordering.StringComparators;

import java.util.Comparator;

/**
 */
public class GenericSearchSortSpec implements SearchSortSpec
{
  private final String ordering;
  private final StringComparators.StringComparator comparator;

  @JsonCreator
  public GenericSearchSortSpec(
      @JsonProperty("ordering") String ordering
  )
  {
    this.ordering = ordering;
    this.comparator = StringComparators.makeComparator(ordering);
  }

  @JsonProperty("ordering")
  public String getOrdering()
  {
    return ordering;
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit o1, SearchHit o2)
      {
        String v1 = o1.getValue();
        String v2 = o2.getValue();
        int ret = comparator.compare(v1, v2);
        if (ret == 0) {
          ret = o1.getDimension().compareTo(o2.getDimension());
        }
        return ret;
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    return toString().getBytes();
  }

  @Override
  public String toString()
  {
    return "generic(" + ordering + ")";
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof GenericSearchSortSpec &&
           ordering.equals(((GenericSearchSortSpec) other).ordering);
  }

  @Override
  public int hashCode()
  {
    return ordering.hashCode();
  }
}
