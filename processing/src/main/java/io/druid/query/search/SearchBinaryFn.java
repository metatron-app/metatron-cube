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

package io.druid.query.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.Result;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchSortSpec;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class SearchBinaryFn implements BinaryFn.Identical<Result<SearchResultValue>>
{
  private final Comparator<SearchHit> comparator;
  private final Granularity gran;
  private final int limit;

  public SearchBinaryFn(
      SearchSortSpec searchSortSpec,
      Granularity granularity,
      int limit
  )
  {
    this.comparator = searchSortSpec.getComparator();
    this.gran = granularity;
    this.limit = gran instanceof AllGranularity ? limit : -1;
  }

  @Override
  public Result<SearchResultValue> apply(Result<SearchResultValue> arg1, Result<SearchResultValue> arg2)
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    final DateTime timestamp = gran instanceof AllGranularity
                               ? arg1.getTimestamp()
                               : gran.bucketStart(arg1.getTimestamp());

    SearchResultValue arg1Vals = arg1.getValue();
    SearchResultValue arg2Vals = arg2.getValue();

    int maxSize = arg1Vals.getValue().size() + arg2Vals.getValue().size();
    if (limit > 0) {
      maxSize = Math.min(limit, maxSize);
    }

    if (maxSize == 0) {
      return new Result<SearchResultValue>(timestamp, new SearchResultValue(ImmutableList.<SearchHit>of()));
    }

    Iterable<SearchHit> sorted = Iterables.mergeSorted(Arrays.asList(arg1Vals, arg2Vals), comparator);
    List<SearchHit> merged = mergeSearchHits(sorted.iterator(), maxSize);

    return new Result<SearchResultValue>(timestamp, new SearchResultValue(merged));
  }

  protected List<SearchHit> mergeSearchHits(Iterator<SearchHit> iterator, final int limit)
  {
    final List<SearchHit> results = Lists.newArrayListWithExpectedSize(limit);

    SearchHit prev = iterator.next();
    while (iterator.hasNext()) {
      SearchHit searchHit = iterator.next();
      if (!prev.equals(searchHit)) {
        results.add(prev);
        if (limit > 0 && results.size() >= limit) {
          return results;
        }
        prev = searchHit;
      }
    }
    if (limit < 0 || results.size() < limit) {
      results.add(prev);
    }
    return results;
  }

  public static class WithCount extends SearchBinaryFn
  {
    public WithCount(SearchSortSpec searchSortSpec, Granularity granularity, int limit)
    {
      super(searchSortSpec, granularity, limit);
    }

    @Override
    protected final List<SearchHit> mergeSearchHits(Iterator<SearchHit> iterator, final int limit)
    {
      final List<SearchHit> results = Lists.newArrayListWithExpectedSize(limit);

      SearchHit prev = iterator.next();
      int sum = prev.getCount();

      while (iterator.hasNext()) {
        SearchHit searchHit = iterator.next();
        if (prev.equals(searchHit)) {
          sum += searchHit.getCount();
        } else {
          results.add(toSearchHit(prev, sum));
          if (limit > 0 && results.size() >= limit) {
            return results;
          }
          prev = searchHit;
          sum = prev.getCount();
        }
      }
      if (limit < 0 || results.size() < limit) {
        results.add(toSearchHit(prev, sum));
      }
      return results;
    }

    private SearchHit toSearchHit(SearchHit searchHit, int sum)
    {
      if (searchHit.getCount() != sum) {
        return new SearchHit(searchHit.getDimension(), searchHit.getValue(), sum);
      } else {
        return searchHit;
      }
    }
  }
}