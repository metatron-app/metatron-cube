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
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.Cache;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class StreamQueryEngine
{
  public Sequence<StreamQueryRow> process(final StreamQuery baseQuery, final Segment segment, final Cache cache)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final StreamQuery query = (StreamQuery) ViewSupportHelper.rewrite(baseQuery, adapter);
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    return Sequences.concat(
        QueryRunnerHelper.makeCursorBasedQuery(
            adapter,
            Iterables.getOnlyElement(intervals),
            VirtualColumns.valueOf(query.getVirtualColumns()),
            query.getDimensionsFilter(),
            cache,
            query.isDescending(),
            query.getGranularity(),
            new Function<Cursor, Sequence<StreamQueryRow>>()
            {
              @Override
              public Sequence<StreamQueryRow> apply(final Cursor cursor)
              {
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
                return Sequences.simple(
                    new Iterable<StreamQueryRow>()
                    {
                      @Override
                      public Iterator<StreamQueryRow> iterator()
                      {
                        return new Iterator<StreamQueryRow>()
                        {
                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone();
                          }

                          @Override
                          public StreamQueryRow next()
                          {
                            final StreamQueryRow theEvent = new StreamQueryRow();
                            theEvent.put(EventHolder.timestampKey, timestampColumnSelector.get());

                            for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                              final String dim = dimSelector.getKey();
                              final DimensionSelector selector = dimSelector.getValue();

                              if (selector == null) {
                                theEvent.put(dim, null);
                              } else {
                                final IndexedInts vals = selector.getRow();

                                if (vals.size() == 1) {
                                  final String dimVal = selector.lookupName(vals.get(0));
                                  theEvent.put(dim, dimVal);
                                } else {
                                  List<String> dimVals = Lists.newArrayList();
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
                            cursor.advance();
                            return theEvent;
                          }
                        };
                      }
                    }
                );
              }
            }
        )
    );
  }
}
