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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.SteppingSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.query.ordering.Comparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class StreamRawQueryToolChest extends QueryToolChest<RawRows, StreamRawQuery>
{
  private static final TypeReference<RawRows> TYPE_REFERENCE =
      new TypeReference<RawRows>()
      {
      };

  @Override
  public QueryRunner<RawRows> mergeResults(final QueryRunner<RawRows> queryRunner)
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(
          Query<RawRows> query, Map<String, Object> responseContext
      )
      {
        final boolean finalWork = query.getContextBoolean(QueryContextKeys.FINAL_WORK, true);
        final StreamRawQuery stream = (StreamRawQuery) query.withOverriddenContext(QueryContextKeys.FINAL_WORK, false);

        final int limit = stream.getLimit();
        final List<String> sortOn = stream.getSortOn();
        if (GuavaUtils.isNullOrEmpty(sortOn)) {
          Sequence<RawRows> sequence = queryRunner.run(stream, responseContext);
          if (limit > 0 && finalWork) {
            return new SteppingSequence.Limit<RawRows>(sequence, limit)
            {
              @Override
              protected void accumulating(RawRows in)
              {
                count += in.getRows().size();
              }
            };
          }
          return sequence;
        }

        // remove limit.. sort whole
        final Sequence<RawRows> sequence = queryRunner.run(stream.withLimit(-1), responseContext);

        final AtomicReference<DateTime> dateTime = new AtomicReference<>();
        final AtomicReference<Schema> schema = new AtomicReference<>();
        List<Object[]> rows = sequence.accumulate(
            Lists.<Object[]>newArrayList(), new Accumulator<List<Object[]>, RawRows>()
            {
              @Override
              public List<Object[]> accumulate(List<Object[]> accumulated, RawRows in)
              {
                if (dateTime.get() == null || dateTime.get().compareTo(in.getTimestamp()) > 0) {
                  dateTime.set(in.getTimestamp());
                }
                schema.compareAndSet(null, in.getSchema());   // should be the same (same, afaik)
                accumulated.addAll(in.getRows());
                return accumulated;
              }
            }
        );
        if (rows.isEmpty()) {
          return Sequences.empty();
        }
        List<Integer> sortIndices = Lists.newArrayList();
        List<String> columnNames = schema.get().getColumnNames();
        for (String sortColumn : sortOn) {
          int index = columnNames.indexOf(sortColumn);
          if (index >= 0) {
            sortIndices.add(index);
          }
        }
        if (!sortIndices.isEmpty()) {
          long start = System.currentTimeMillis();
          Object[][] array = rows.toArray(new Object[rows.size()][]);
          Arrays.parallelSort(array, Comparators.toArrayComparator(Ints.toArray(sortIndices)));
          LOG.info("Sorted %,d rows in %,d msec", array.length, (System.currentTimeMillis() - start));
          rows = Arrays.asList(array);
        }
        if (limit > 0 && rows.size() > limit) {
          rows = rows.subList(0, limit);
        }
        return Sequences.simple(Arrays.asList(new RawRows(dateTime.get(), schema.get(), rows)));
      }
    };
  }

  @Override
  public TypeReference<RawRows> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public TabularFormat toTabularFormat(
      final StreamRawQuery query,
      final Sequence<RawRows> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.concat(
            Sequences.map(
                sequence, new Function<RawRows, Sequence<Map<String, Object>>>()
                {
                  @Override
                  public Sequence<Map<String, Object>> apply(RawRows input)
                  {
                    final String[] columnNames = query.getColumns().toArray(new String[0]);
                    final List<Object[]> rows = input.getRows();
                    return Sequences.simple(
                        new Iterable<Map<String, Object>>()
                        {
                          @Override
                          public Iterator<Map<String, Object>> iterator()
                          {
                            return new Iterator<Map<String, Object>>()
                            {
                              private final Iterator<Object[]> rowStream = rows.iterator();

                              @Override
                              public boolean hasNext()
                              {
                                return rowStream.hasNext();
                              }

                              @Override
                              public Map<String, Object> next()
                              {
                                final Object[] row = rowStream.next();
                                final Map<String, Object> converted = Maps.newLinkedHashMap();
                                for (int i = 0; i < columnNames.length; i++) {
                                  converted.put(columnNames[i], row[i]);
                                }
                                return converted;
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

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<RawRows> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<RawRows>> function(
          final Query<RawRows> query, Map<String, Object> context,
          final Segment segment
      )
      {
        final StreamQueryEngine engine = new StreamQueryEngine();
        final StreamRawQuery outerQuery = (StreamRawQuery) query;
        return new Function<Interval, Sequence<RawRows>>()
        {
          @Override
          public Sequence<RawRows> apply(Interval interval)
          {
            return engine.process(
                outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                segment,
                null,
                null
            );
          }
        };
      }
    };
  }
}
