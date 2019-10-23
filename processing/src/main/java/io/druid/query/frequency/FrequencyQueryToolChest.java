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
import com.google.common.collect.Maps;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.orderby.TopNSorter;
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
  private static final TypeReference<Object[]> TYPE_REFERENCE =
      new TypeReference<Object[]>()
      {
      };

  private static final Comparator<Object[]> COUNT_DESCENDING_NF = new Comparator<Object[]>()
  {
    @Override
    @SuppressWarnings("unchecked")
    public int compare(final Object[] o1, final Object[] o2)
    {
      int compare = -Comparators.compareNF((Comparable) o1[0], (Comparable) o2[0]);
      for (int i = 1; i < o1.length && compare == 0; i++) {
        compare = Comparators.compareNF((Comparable) o1[i], (Comparable) o2[i]);
      }
      return compare;
    }
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
        runner.run(query.toLocalQuery(), responseContext).accumulate(
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
  public QueryRunner<Object[]> finalQueryDecoration(final QueryRunner<Object[]> runner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final Sequence<Object[]> sequence = runner.run(query, responseContext);
        return Sequences.once(
            TopNSorter.topN(COUNT_DESCENDING_NF, sequence, ((FrequencyQuery) query).getCandidateLimit())
        );
      }
    };
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
  public TypeReference<Object[]> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }
}
