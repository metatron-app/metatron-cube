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

package io.druid.query.frequency;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.TopNSorter;
import io.druid.query.ordering.Accessor;
import io.druid.query.ordering.Comparators;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 */
public class FrequencyQueryToolChest extends QueryToolChest<Object[], FrequencyQuery>
{
  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public FrequencyQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  private static final Comparator<Object[]> COUNT_DESC_ORDERING = (o1, o2) -> {
    return -Ints.compare(((Number) o1[0]).intValue(), ((Number) o2[0]).intValue());
  };

  private static final Comparator<Object[]> DEFAULT_ORDERING = (o1, o2) -> {
    int compare = COUNT_DESC_ORDERING.compare(o1, o2);
    for (int i = 1; compare == 0 && i < o1.length; i++) {
      compare = Comparators.compareNF((Comparable) o1[i], (Comparable) o2[i]);
    }
    return compare;
  };

  @Override
  public QueryRunner<Object[]> mergeResults(final QueryRunner<Object[]> runner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final FrequencyQuery frequency = (FrequencyQuery) query;
        final int columns = frequency.getDimensions().size();
        final BiFunction<ObjectArray, MutableInt, MutableInt> accumulator =
            new BiFunction<ObjectArray, MutableInt, MutableInt>()
            {
              @Override
              public MutableInt apply(ObjectArray key, MutableInt value)
              {
                final int count = ((Number) key.get(0)).intValue();
                if (value == null) {
                  value = new MutableInt(count);
                } else {
                  value.add(count);
                }
                return value;
              }
            };
        final Map<ObjectArray, MutableInt> merged = new TreeMap<>();
        runner.run(query, responseContext).accumulate(
            null, new Accumulator<Object, Object[]>()
            {
              @Override
              public Object accumulate(Object accumulated, Object[] in)
              {
                merged.compute(new ObjectArray.From<>(in, 1), accumulator);
                return null;
              }
            }
        );
        return Sequences.simple(Iterables.transform(
            merged.entrySet(),
            new Function<Map.Entry<ObjectArray, MutableInt>, Object[]>()
            {
              @Override
              public Object[] apply(Map.Entry<ObjectArray, MutableInt> entry)
              {
                final Object[] array = entry.getKey().array();
                array[0] = entry.getValue().intValue();
                return array;
              }
            }
        ));
      }
    };
  }

  @Override
  public QueryMetrics<? super FrequencyQuery> makeMetrics(FrequencyQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public QueryRunner<Object[]> finalQueryDecoration(final QueryRunner<Object[]> runner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final FrequencyQuery frequency = (FrequencyQuery) query;
        final LimitSpec spec = frequency.getLimitSpec();
        final List<String> columns = frequency.estimatedOutputColumns();
        final Sequence<Object[]> sequence = runner.run(query, responseContext);
        return Sequences.once(
            TopNSorter.topN(toComparator(spec, columns), sequence, frequency.limitForCandidate())
        );
      }
    };
  }

  private static Comparator<Object[]> toComparator(LimitSpec spec, List<String> columns)
  {
    if (!GuavaUtils.isNullOrEmpty(spec.getColumns())) {
      final List<Comparator<Object[]>> comparators = Lists.newArrayList();
      for (OrderByColumnSpec ordering : spec.getColumns()) {
        final int index = columns.indexOf(ordering.getDimension());
        if (index < 0) {
          continue;
        }
        comparators.add(new Accessor.ComparatorOn<>(ordering.getComparator(), row -> { return row[index]; }));
      }
      return Comparators.compound(GuavaUtils.concat(COUNT_DESC_ORDERING, comparators));
    }
    return DEFAULT_ORDERING;
  }

  @Override
  public Function<Sequence<Object[]>, Sequence<Map<String, Object>>> asMap(
      final FrequencyQuery query,
      final String timestampColumn
  )
  {
    return new Function<Sequence<Object[]>, Sequence<Map<String, Object>>>()
    {
      private final List<String> columnNames = query.estimatedOutputColumns();

      @Override
      public Sequence<Map<String, Object>> apply(Sequence<Object[]> sequence)
      {
        return Sequences.map(sequence, new Function<Object[], Map<String, Object>>()
        {
          @Override
          public Map<String, Object> apply(Object[] input)
          {
            final Map<String, Object> converted = Maps.newLinkedHashMap();
            for (int i = 0; i < columnNames.size(); i++) {
              converted.put(columnNames.get(i), input[i]);
            }
            return converted;
          }
        });
      }
    };
  }

  @Override
  public TypeReference<Object[]> getResultTypeReference(FrequencyQuery query)
  {
    return ARRAY_TYPE_REFERENCE;
  }
}
