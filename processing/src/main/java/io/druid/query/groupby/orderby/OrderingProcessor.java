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

package io.druid.query.groupby.orderby;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.query.ordering.Direction;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class OrderingProcessor
{
  public static OrderingProcessor from(Query.AggregationsSupport<?> query)
  {
    List<String> columns = Lists.newArrayList();
    columns.add(Row.TIME_COLUMN_NAME);
    columns.addAll(DimensionSpecs.toOutputNames(query.getDimensions()));
    columns.addAll(AggregatorFactory.toNames(query.getAggregatorSpecs()));
    columns.addAll(PostAggregators.toNames(query.getPostAggregatorSpecs()));

    boolean finalized = BaseQuery.isFinalize(query, true);
    Map<String, Comparator> comparators = Maps.newHashMap();
    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
      comparators.put(agg.getName(), finalized ? agg.getFinalizedComparator() : agg.getComparator());
    }
    for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
      comparators.put(postAgg.getName(), postAgg.getComparator());
    }
    return new OrderingProcessor(columns, comparators);
  }

  private final List<String> columns;
  private final Map<String, Comparator> comparatorMap;

  public OrderingProcessor(List<String> columns, Map<String, Comparator> comparatorMap)
  {
    this.columns = columns;
    this.comparatorMap = comparatorMap != null ? comparatorMap : ImmutableMap.<String, Comparator>of();
  }

  public Function<Sequence<Row>, Sequence<Row>> toRowLimitFn(
      final List<OrderByColumnSpec> orderingSpecs,
      final boolean sortOnTimeForLimit,
      final int limit
  )
  {
    final String[] columnNames = columns.toArray(new String[0]);
    final Ordering<Object[]> ordering = toArrayOrdering(orderingSpecs, sortOnTimeForLimit);
    return GuavaUtils.sequence(
        Sequences.mapper(GroupByQueryEngine.rowToArray(columnNames)),
        new LimitSpec.SortingArrayFn(ordering, limit),
        Sequences.mapper(GroupByQueryEngine.arrayToRow(columnNames))
    );
  }

  public Ordering<Object[]> toArrayOrdering(List<OrderByColumnSpec> orderingSpecs, boolean prependTimeOrdering)
  {
    List<Accessor<Object[]>> accessors = Lists.newArrayList();
    if (prependTimeOrdering) {
      accessors.add(arrayAccessor(columns.indexOf(Row.TIME_COLUMN_NAME)));
    }
    for (OrderByColumnSpec ordering : orderingSpecs) {
      accessors.add(arrayAccessor(columns.indexOf(ordering.getDimension())));
    }
    return makeComparator(orderingSpecs, accessors, prependTimeOrdering);
  }

  public static Accessor<Object[]> arrayAccessor(final int index)
  {
    return new Accessor<Object[]>()
    {
      @Override
      public Object get(Object[] source)
      {
        return index < 0 ? null : source[index];
      }
    };
  }

  public Ordering<Row> toRowOrdering(List<OrderByColumnSpec> orderingSpecs, boolean prependTimeOrdering)
  {
    List<Accessor<Row>> accessors = Lists.newArrayList();
    if (prependTimeOrdering) {
      accessors.add(rowAccessor(Row.TIME_COLUMN_NAME));
    }
    for (OrderByColumnSpec ordering : orderingSpecs) {
      accessors.add(rowAccessor(ordering.getDimension()));
    }
    return makeComparator(orderingSpecs, accessors, prependTimeOrdering);
  }

  private static Accessor<Row> rowAccessor(final String column)
  {
    if (Row.TIME_COLUMN_NAME.equals(column)) {
      return new Accessor<Row>()
      {
        @Override
        public Object get(Row source)
        {
          return source.getTimestampFromEpoch();
        }
      };
    }
    return new Accessor<Row>()
    {
      @Override
      public Object get(Row source)
      {
        return source.getRaw(column);
      }
    };
  }

  private <T> Ordering<T> makeComparator(
      List<OrderByColumnSpec> orderingSpecs,
      List<Accessor<T>> accessors,
      boolean prependTimeOrdering
  )
  {
    int index = 0;
    List<Comparator<T>> comparators = Lists.newArrayList();
    if (prependTimeOrdering) {
      comparators.add(new Accessor.TimeComparator<T>(accessors.get(index++)));
    }
    for (OrderByColumnSpec columnSpec : orderingSpecs) {
      Comparator comparator = comparatorMap.get(columnSpec.getDimension());
      Accessor<T> accessor = accessors.get(index++);
      Comparator<T> nextOrdering;
      if (comparator == null) {
        nextOrdering = new Accessor.ComparatorOn<>(columnSpec.getComparator(), accessor);    // as-is
      } else {
        nextOrdering = new Accessor.ComparatorOn<>(comparator, accessor);
        if (columnSpec.getDirection() == Direction.DESCENDING) {
          nextOrdering = Ordering.from(nextOrdering).reverse();
        }
      }
      comparators.add(nextOrdering);
    }

    return Comparators.compound(comparators);
  }
}

