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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 */
public class SelectMetaQueryEngine
{
  public Sequence<Result<SelectMetaResultValue>> process(
      final SelectMetaQuery query,
      final Segment segment,
      final Cache cache
  )
  {
    final StorageAdapter adapter = segment.asStorageAdapter(false);

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final Interval interval = Iterables.getOnlyElement(intervals);
    final DimFilter filter = query.getFilter();
    final Granularity granularity = query.getGranularity();

    final String segmentId = segment.getIdentifier();

    final PagingOffset offset = query.getPagingOffset(segmentId);
    final float averageSize = calculateAverageSize(query, adapter);

    // minor optimization.. todo: we can do this even with filters set
    if (filter == null && offset.startDelta() == 0 &&
        QueryGranularities.ALL.equals(granularity) &&
        interval.contains(segment.getDataInterval())) {
      int row = adapter.getNumRows();
      return Sequences.of(
          new Result<>(
              granularity.toDateTime(interval.getStartMillis()),
              new SelectMetaResultValue(
                  ImmutableMap.of(segmentId, row), (long) (row * averageSize)
              )
          )
      );
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query,
        cache,
        new Function<Cursor, Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(Cursor cursor)
          {
            int row = 0;
            for (cursor.advanceTo(offset.startDelta()); !cursor.isDone(); cursor.advance()) {
              row++;
            }
            return new Result<>(
                cursor.getTime(),
                new SelectMetaResultValue(ImmutableMap.of(segmentId, row), (long) (row * averageSize))
            );
          }
        }
    );
  }

  // not include virtual columns & not consider extract, lookup, etc.
  private float calculateAverageSize(SelectMetaQuery query, StorageAdapter adapter)
  {
    float averageSize = 0;
    final Set<String> dimensions = Sets.newHashSet(DimensionSpecs.toOutputNames(query.getDimensions()));
    for (String dimension : adapter.getAvailableDimensions()) {
      if (dimensions.contains(dimension)) {
        averageSize += adapter.getAverageSize(dimension);
      }
    }
    final Set<String> metrics = Sets.newHashSet(query.getMetrics());
    for (String metric : adapter.getAvailableMetrics()) {
      if (metrics == null || metrics.contains(metric)) {
        averageSize += adapter.getAverageSize(metric);
      }
    }
    return averageSize;
  }
}
