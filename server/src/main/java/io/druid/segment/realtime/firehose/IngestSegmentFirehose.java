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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.guava.YieldingAccumulator;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.Granularity;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IngestSegmentFirehose implements Firehose
{
  private volatile Yielder<InputRow> rowYielder;

  public IngestSegmentFirehose(
      final List<WindowedStorageAdapter> adapters,
      final List<String> dims,
      final List<String> metrics,
      final DimFilter dimFilter,
      final Granularity granularity
  )
  {
    Sequence<InputRow> rows = Sequences.explode(
        Sequences.simple(adapters), new Function<WindowedStorageAdapter, Sequence<InputRow>>()
        {
          @Override
          public Sequence<InputRow> apply(WindowedStorageAdapter adapter)
          {
            return Sequences.explode(
                adapter.getAdapter().makeCursors(
                    dimFilter,
                    adapter.getInterval(),
                    RowResolver.of(adapter.getAdapter(), ImmutableList.<VirtualColumn>of()),
                    granularity,
                    false,
                    null
                ), new Function<Cursor, Sequence<InputRow>>()
                {
                  @Nullable
                  @Override
                  public Sequence<InputRow> apply(final Cursor cursor)
                  {
                    final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

                    final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                    for (String dim : dims) {
                      final DimensionSelector dimSelector = cursor.makeDimensionSelector(
                          new DefaultDimensionSpec(dim, dim)
                      );
                      // dimSelector is null if the dimension is not present
                      if (dimSelector != null) {
                        dimSelectors.put(dim, dimSelector);
                      }
                    }

                    final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                    for (String metric : metrics) {
                      final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                      if (metricSelector != null) {
                        metSelectors.put(metric, metricSelector);
                      }
                    }

                    return Sequences.simple(
                        new Iterable<InputRow>()
                        {
                          @Override
                          public Iterator<InputRow> iterator()
                          {
                            return new Iterator<InputRow>()
                            {
                              @Override
                              public boolean hasNext()
                              {
                                return !cursor.isDone();
                              }

                              @Override
                              public InputRow next()
                              {
                                final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                                final long timestamp = timestampColumnSelector.get();
                                theEvent.put(EventHolder.timestampKey, new DateTime(timestamp));

                                for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                                  final String dim = dimSelector.getKey();
                                  final DimensionSelector selector = dimSelector.getValue();
                                  final IndexedInts vals = selector.getRow();

                                  if (vals.size() == 1) {
                                    final Object dimVal = selector.lookupName(vals.get(0));
                                    theEvent.put(dim, dimVal);
                                  } else {
                                    List<Object> dimVals = Lists.newArrayList();
                                    for (int i = 0; i < vals.size(); ++i) {
                                      dimVals.add(selector.lookupName(vals.get(i)));
                                    }
                                    theEvent.put(dim, dimVals);
                                  }
                                }

                                for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                                  final String metric = metSelector.getKey();
                                  final ObjectColumnSelector selector = metSelector.getValue();
                                  theEvent.put(metric, selector.get());
                                }
                                cursor.advance();
                                return new MapBasedInputRow(timestamp, dims, theEvent);
                              }

                              @Override
                              public void remove()
                              {
                                throw new UnsupportedOperationException("Remove Not Supported");
                              }
                            };
                          }
                        }
                    );
                  }
                }
            );
          }
        }
    );
    rowYielder = rows.toYielder(
        null,
        new YieldingAccumulator<InputRow, InputRow>()
        {
          @Override
          public InputRow accumulate(InputRow accumulated, InputRow in)
          {
            yield();
            return in;
          }
        }
    );
  }

  @Override
  public boolean hasMore()
  {
    return !rowYielder.isDone();
  }

  @Override
  public InputRow nextRow()
  {
    final InputRow inputRow = rowYielder.get();
    rowYielder = rowYielder.next(null);
    return inputRow;
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    rowYielder.close();
  }

}
