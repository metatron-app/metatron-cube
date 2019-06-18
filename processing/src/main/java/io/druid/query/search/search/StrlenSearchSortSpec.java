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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import io.druid.query.QueryCacheHelper;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class StrlenSearchSortSpec implements SearchSortSpec
{
  private final List<String> ordering;
  private final SearchHitSort comparator;

  @JsonCreator
  public StrlenSearchSortSpec(
      @JsonProperty("ordering") List<String> ordering
  )
  {
    this.ordering = ordering;
    this.comparator = SearchHitSort.valueOf(ordering);
  }

  public StrlenSearchSortSpec()
  {
    this(null);
  }

  @JsonProperty
  public List<String> getOrdering()
  {
    return ordering;
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit s, SearchHit s1)
      {
        final String v1 = s.getValue();
        final String v2 = s1.getValue();
        if (v1 == null && v2 == null) {
          return 0;
        }
        if (v1 == null) {
          return -1;
        }
        if (v2 == null) {
          return 1;
        }
        int res = Ints.compare(v1.length(), v2.length());
        if (res == 0) {
          res = v1.compareTo(v2);
        }
        if (res == 0) {
          res = s.getDimension().compareTo(s1.getDimension());
        }
        return res;
      }
    };
  }

  @Override
  public Comparator<SearchHit> getResultComparator()
  {
    return comparator == null ? null : comparator.getComparator();
  }

  @Override
  public boolean sortOnCount()
  {
    return comparator != null && comparator.sortOnCount();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] key = QueryCacheHelper.computeCacheBytes(ordering);
    return ByteBuffer.allocate(1 + key.length)
                     .put((byte) 0x01)
                     .put(key)
                     .array();
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
    StrlenSearchSortSpec that = (StrlenSearchSortSpec) o;
    return Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(ordering);
  }

  @Override
  public String toString()
  {
    return "StrlenSearchSortSpec{" +
           "ordering=" + ordering +
           '}';
  }
}
