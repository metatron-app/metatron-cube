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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.cache.Cache;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.timeline.DataSegmentUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class SelectQueryEngine
{
  public Sequence<Result<SelectResultValue>> process(
      final SelectQuery baseQuery,
      final SelectQueryConfig config,
      final Segment segment
  )
  {
    return process(baseQuery, config, segment, null);
  }

  public Sequence<Result<SelectResultValue>> process(
      final SelectQuery query,
      final SelectQueryConfig config,
      final Segment segment,
      final Cache cache
  )
  {
    final RowResolver resolver = Segments.getResolver(segment, query);
    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    // should be rewritten with given interval
    final StorageAdapter adapter = segment.asStorageAdapter(true);
    final String segmentId = DataSegmentUtils.withInterval(dataSource, adapter.getSegmentIdentifier(), intervals.get(0));

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        resolver,
        query.getDimensionsFilter(),
        cache,
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<SelectResultValue>>()
        {
          @Override
          public Result<SelectResultValue> apply(Cursor cursor)
          {
            final SelectResultValueBuilder builder = new SelectResultValueBuilder(
                cursor.getTime(),
                query.getPagingSpec(),
                query.isDescending()
            );

            final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

            final Map<String, DimensionSelector> dimSelectors = Maps.newLinkedHashMap();
            for (DimensionSpec dim : query.getDimensions()) {
              final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
              dimSelectors.put(dim.getOutputName(), dimSelector);
            }

            final Map<String, ObjectColumnSelector> metSelectors = Maps.newLinkedHashMap();
            for (String metric : query.getMetrics()) {
              final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
              metSelectors.put(metric, metricSelector);
            }

            final PagingOffset offset = query.getPagingOffset(segmentId);

            cursor.advanceTo(offset.startDelta());

            int lastOffset = offset.startOffset();
            for (; !cursor.isDone() && offset.hasNext(); cursor.advance(), offset.next()) {
              final Map<String, Object> theEvent = Maps.newLinkedHashMap();
              long value = timestampColumnSelector.get();
              theEvent.put(EventHolder.timestampKey, config.isUseDateTime() ? new DateTime(value) : value);

              for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                final String dim = dimSelector.getKey();
                final DimensionSelector selector = dimSelector.getValue();

                if (selector == null) {
                  theEvent.put(dim, null);
                } else {
                  final IndexedInts vals = selector.getRow();

                  if (vals.size() == 1) {
                    final Comparable dimVal = selector.lookupName(vals.get(0));
                    theEvent.put(dim, dimVal);
                  } else {
                    List<Comparable> dimVals = Lists.newArrayList();
                    for (int i = 0; i < vals.size(); ++i) {
                      dimVals.add(selector.lookupName(vals.get(i)));
                    }
                    if (query.getConcatString() != null) {
                      theEvent.put(dim, StringUtils.join(dimVals, query.getConcatString()));
                    } else {
                      theEvent.put(dim, dimVals);
                    }
                  }
                }
              }

              for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                final String metric = metSelector.getKey();
                final ObjectColumnSelector selector = metSelector.getValue();

                if (selector == null) {
                  theEvent.put(metric, null);
                } else {
                  theEvent.put(metric, selector.get());
                }
              }

              builder.addEntry(
                  new EventHolder(
                      segmentId,
                      lastOffset = offset.current(),
                      theEvent
                  )
              );
            }

            builder.finished(segmentId, lastOffset);

            return builder.build();
          }
        }
    );
  }
}
