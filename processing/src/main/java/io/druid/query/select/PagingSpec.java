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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;

import java.util.Map;

/**
 */
public class PagingSpec implements Cacheable
{
  public static final PagingSpec GET_ALL = newSpec(-1);

  public static PagingSpec newSpec(int threshold)
  {
    return new PagingSpec(null, threshold);
  }

  public static Map<String, Integer> merge(Iterable<Map<String, Integer>> cursors)
  {
    Map<String, Integer> next = Maps.newHashMap();
    for (Map<String, Integer> cursor : cursors) {
      for (Map.Entry<String, Integer> entry : cursor.entrySet()) {
        next.put(entry.getKey(), entry.getValue());
      }
    }
    return next;
  }

  public static Map<String, Integer> next(Map<String, Integer> cursor, boolean descending)
  {
    for (Map.Entry<String, Integer> entry : cursor.entrySet()) {
      entry.setValue(descending ? entry.getValue() - 1 : entry.getValue() + 1);
    }
    return cursor;
  }

  private final Map<String, Integer> pagingIdentifiers;
  private final int threshold;
  private final boolean fromNext;

  @JsonCreator
  public PagingSpec(
      @JsonProperty("pagingIdentifiers") Map<String, Integer> pagingIdentifiers,
      @JsonProperty("threshold") int threshold,
      @JsonProperty("fromNext") boolean fromNext
  )
  {
    this.pagingIdentifiers = pagingIdentifiers == null ? Maps.<String, Integer>newHashMap() : pagingIdentifiers;
    this.threshold = threshold;
    this.fromNext = fromNext;
  }

  public PagingSpec(Map<String, Integer> pagingIdentifiers, int threshold)
  {
    this(pagingIdentifiers, threshold, false);
  }

  public PagingSpec withThreshold(int threshold)
  {
    return new PagingSpec(pagingIdentifiers, threshold, fromNext);
  }

  @JsonProperty
  public Map<String, Integer> getPagingIdentifiers()
  {
    return pagingIdentifiers;
  }

  @JsonProperty
  public int getThreshold()
  {
    return threshold;
  }

  @JsonProperty
  public boolean isFromNext()
  {
    return fromNext;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    for (Map.Entry<String, Integer> entry : pagingIdentifiers.entrySet()) {
      builder.append(entry.getKey()).append(entry.getValue());
    }
    return builder.append(threshold)
                  .append(isFromNext());
  }

  public PagingOffset getOffset(String identifier, boolean descending)
  {
    Integer offset = pagingIdentifiers.get(identifier);
    if (offset == null) {
      offset = PagingOffset.toOffset(0, descending);
    } else if (fromNext) {
      offset = descending ? offset - 1 : offset + 1;
    }
    return PagingOffset.of(offset, threshold);
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

    PagingSpec that = (PagingSpec) o;

    if (fromNext != that.fromNext) {
      return false;
    }
    if (threshold != that.threshold) {
      return false;
    }
    if (!pagingIdentifiers.equals(that.pagingIdentifiers)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = pagingIdentifiers.hashCode();
    result = 31 * result + threshold;
    result = 31 * result + (fromNext ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "PagingSpec{" +
           "pagingIdentifiers=" + pagingIdentifiers +
           ", threshold=" + threshold +
           ", fromNext=" + fromNext +
           '}';
  }
}
