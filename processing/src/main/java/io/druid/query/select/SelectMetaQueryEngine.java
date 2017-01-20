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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 */
public class SelectMetaQueryEngine
{
  public Sequence<Result<SelectMetaResultValue>> process(final SelectMetaQuery baseQuery, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final SelectMetaQuery query = (SelectMetaQuery) ViewSupportHelper.rewrite(baseQuery, adapter);
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final Interval interval = Iterables.getOnlyElement(intervals);
    final QueryGranularity granularity = query.getGranularity();
    final String identifier = segment.getIdentifier();

    StorageAdapter storageAdapter = segment.asStorageAdapter();
    Metadata metadata = storageAdapter.getMetadata();
    final List<String> dimensions = Lists.newArrayList(storageAdapter.getAvailableDimensions());
    final List<String> metrics = Lists.newArrayList(storageAdapter.getAvailableMetrics());
    final AggregatorFactory[] aggregators;
    if (metadata != null && metadata.getAggregators() != null) {
      aggregators = metadata.getAggregators();
    } else {
      aggregators = null;
    }

    final float averageSize = calculateAverageSize(query, adapter);

    // minor optimization.. todo: we can do this even with filters set
    if (query.getDimensionsFilter() == null &&
        QueryGranularities.ALL.equals(granularity) &&
        interval.equals(segment.getDataInterval())) {
      int row = storageAdapter.getNumRows();
      return BaseSequence.simple(
          Arrays.asList(
              new Result<>(
                  granularity.toDateTime(interval.getStartMillis()),
                  new SelectMetaResultValue(
                      dimensions, metrics, aggregators, ImmutableMap.of(identifier, row), (long) (row * averageSize)
                  )
              )
          )
      );
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        intervals,
        VirtualColumns.EMPTY,
        query.getDimensionsFilter(),
        null,
        query.isDescending(),
        granularity,
        new Function<Cursor, Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(Cursor cursor)
          {
            int i = 0;
            for (; !cursor.isDone(); cursor.advance()) {
              i++;
            }
            return new Result<>(
                cursor.getTime(),
                new SelectMetaResultValue(dimensions, metrics, aggregators, ImmutableMap.of(identifier, i), (long) (i * averageSize))
            );
          }
        }
    );
  }

  private float calculateAverageSize(SelectMetaQuery query, StorageAdapter adapter)
  {
    float averageSize = 0;
    final Set<String> retain = query.getColumns() == null ? null : Sets.newHashSet(query.getColumns());
    for (String dimension : adapter.getAvailableDimensions()) {
      if (retain == null || retain.contains(dimension)) {
        averageSize += adapter.getAverageSize(dimension);
      }
    }
    for (String metric : adapter.getAvailableMetrics()) {
      if (retain == null || retain.contains(metric)) {
        averageSize += adapter.getAverageSize(metric);
      }
    }
    return averageSize;
  }
}
