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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.Sequences;
import io.druid.data.UTF8Bytes;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.RowAggregator;
import io.druid.query.aggregation.RowCollector;
import io.druid.query.aggregation.countmin.CountMinSketch;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectArray;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

/**
 */
public class FrequencyQueryRunnerFactory extends QueryRunnerFactory.Abstract<Object[]>
{
  @Inject
  public FrequencyQueryRunnerFactory(FrequencyQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public Supplier<Object> preFactoring(
      Query<Object[]> query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Suppliers.ofInstance(CountMinSketch.fromCompressedBytes(((FrequencyQuery) query).getSketch()));
  }

  @Override
  public QueryRunner<Object[]> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final CountMinSketch sketch = (CountMinSketch) optimizer.get();
        return QueryRunnerHelper.makeCursorBasedQueryConcat(
            segment,
            query,
            cache,
            processor((FrequencyQuery) query, sketch)
        );
      }
    };
  }

  private static Function<Cursor, Sequence<Object[]>> processor(final FrequencyQuery query, final CountMinSketch sketch)
  {
    return new Function<Cursor, Sequence<Object[]>>()
    {
      private final int limit = query.limitForCandidate();

      private final MutableInt size = new MutableInt();
      private final java.util.function.Function<Integer, Map<ObjectArray, MutableInt>> map = x -> Maps.<ObjectArray, MutableInt>newHashMap();
      private final java.util.function.Function<ObjectArray, MutableInt> counter = a -> {
        size.increment();
        return new MutableInt();
      };

      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Object[]> apply(final Cursor cursor)
      {
        final TreeMap<Integer, Map<ObjectArray, MutableInt>> sortedMap = new TreeMap<>();
        final RowCollector collector = new RowCollector()
        {
          @Override
          public void collect(BytesRef[] values, BytesRef bytes)
          {
            final long hashCode = Murmur3.hash64(bytes);
            final int cardinality = sketch.getEstimatedCount(hashCode);
            if (size.intValue() > limit && cardinality < sortedMap.firstKey()) {
              return;
            }
            final Object[] array = new Object[values.length + 1];
            for (int i = 0; i < values.length; i++) {
              array[i + 1] = UTF8Bytes.of(values[i].toBytes());   // explicit copy
            }
            final ObjectArray<Object> key = ObjectArray.withHash(array, (int) hashCode);
            sortedMap.computeIfAbsent(cardinality, map).computeIfAbsent(key, counter).increment();
            if (size.intValue() > limit && cardinality > sortedMap.firstKey()) {
              size.subtract(sortedMap.pollFirstEntry().getValue().size());
            }
          }
        };
        final List<DimensionSelector> selectors = Lists.newArrayList();
        for (DimensionSpec dimension : query.getDimensions()) {
          selectors.add(cursor.makeDimensionSelector(dimension));
        }
        final int[][] groupings = query.getGroupings();
        final RowAggregator aggregator = new RowAggregator(selectors, groupings)
        {
          @Override
          protected Class collectorClass() {return collector.getClass();}
        };
        for (;!cursor.isDone(); cursor.advance()) {
          aggregator.aggregate(collector);
        }
        final List<Object[]> result = Lists.newArrayList();
        for (Map<ObjectArray, MutableInt> map : sortedMap.values()) {
          for (Map.Entry<ObjectArray, MutableInt> entry : map.entrySet()) {
            final Object[] array = entry.getKey().array();
            array[0] = entry.getValue().intValue();
            result.add(array);
          }
        }
        return Sequences.simple(result);
      }
    };
  }
}
