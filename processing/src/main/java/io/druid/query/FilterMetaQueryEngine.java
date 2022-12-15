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

package io.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Segment;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;
import io.druid.segment.filter.FilterContext;
import org.joda.time.Interval;

/**
 */
public class FilterMetaQueryEngine
{
  public static Sequence<long[]> process(final FilterMetaQuery query, final Segment segment, final SessionCache cache)
  {
    final DimFilter filter = query.getFilter();
    final Interval interval = Iterables.getOnlyElement(query.getIntervals(), null);
    Preconditions.checkNotNull(interval, "Can only handle a single interval, got %s", query.getIntervals());

    final int totalRow = segment.getNumRows();
    if (filter == null && interval.contains(segment.getInterval())) {
      return Sequences.of(new long[]{totalRow, totalRow});
    }
    final StorageAdapter adapter = segment.asStorageAdapter(false);
    if (adapter == null) {
      throw new SegmentMissingException("Segment [%s] is unmapped.. retry", segment.getIdentifier());
    }
    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query,
        cache,
        cursor -> {
          final FilterContext context = cursor.getFilterContext();
          if (context != null && context.isFullScan()) {
            return new long[]{context.targetNumRows(), totalRow};
          }
          int row = 0;
          for (; !cursor.isDone(); cursor.advance(), row++) { }
          return new long[]{row, totalRow};
        }
    );
  }
}
