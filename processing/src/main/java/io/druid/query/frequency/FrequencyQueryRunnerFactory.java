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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.aggregation.Murmur3;
import io.druid.query.aggregation.countmin.CountMinSketch;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.UTF8Bytes;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectArray;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class FrequencyQueryRunnerFactory extends QueryRunnerFactory.Abstract<Object[], FrequencyQuery>
{
  private static final int MIN_CANDIDATES = 16;

  @Inject
  public FrequencyQueryRunnerFactory(FrequencyQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public Future<Object> preFactoring(
      FrequencyQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Futures.<Object>immediateFuture(CountMinSketch.fromCompressedBytes(query.getSketch()));
  }

  @Override
  public QueryRunner<Object[]> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final CountMinSketch sketch = (CountMinSketch) Futures.getUnchecked(optimizer);
        return Sequences.concat(QueryRunnerHelper.makeCursorBasedQuery(
            segment.asStorageAdapter(true),
            query,
            cache,
            processor((FrequencyQuery) query, sketch)
        ));
      }
    };
  }

  @Override
  public QueryRunner<Object[]> mergeRunners(
      final ExecutorService executor,
      final Iterable<QueryRunner<Object[]>> runners,
      final Future<Object> optimizer
  )
  {
    return QueryRunners.executeParallel(executor, Lists.newArrayList(runners), null);
  }

  private static Function<Cursor, Sequence<Object[]>> processor(final FrequencyQuery query, final CountMinSketch sketch)
  {
    return new Function<Cursor, Sequence<Object[]>>()
    {
      private final int limit = Math.max(MIN_CANDIDATES, query.getLimit() << 2);  // candidates x4
      private final String[] columns = query.getColumns().toArray(new String[0]);

      private final MutableInt size = new MutableInt();
      private final java.util.function.Function<Integer, Map<ObjectArray, MutableInt>> map =
          new java.util.function.Function<Integer, Map<ObjectArray, MutableInt>>()
          {
            @Override
            public Map<ObjectArray, MutableInt> apply(Integer integer)
            {
              return Maps.<ObjectArray, MutableInt>newHashMap();
            }
          };
      private final java.util.function.Function<ObjectArray, MutableInt> counter =
          new java.util.function.Function<ObjectArray, MutableInt>()
          {
            @Override
            public MutableInt apply(ObjectArray objectArray)
            {
              size.increment();
              return new MutableInt();
            }
          };

      @Override
      public Sequence<Object[]> apply(final Cursor cursor)
      {
        int index = 0;
        final List<DimensionSelector> selectors = Lists.newArrayList();
        for (String column : columns) {
          selectors.add(cursor.makeDimensionSelector(DefaultDimensionSpec.of(column)));
        }
        final TreeMap<Integer, Map<ObjectArray, MutableInt>> sortedMap = new TreeMap<>();
        final HashCollector collector = new HashCollector()
        {
          @Override
          public void collect(Object[] values, byte[] bytes)
          {
            final long hashCode = Murmur3.hash64(bytes);
            final int cardinality = sketch.getEstimatedCount(hashCode);
            if (size.intValue() > limit && cardinality < sortedMap.firstKey()) {
              return;
            }
            final Object[] array = new Object[values.length + 1];
            for (int i = 0; i < values.length; i++) {
              array[i + 1] = UTF8Bytes.of((byte[]) values[i]);
            }
            final ObjectArray<Object> key = new ObjectArray.WithHash<>(array, (int) hashCode);
            sortedMap.computeIfAbsent(cardinality, map).computeIfAbsent(key, counter).increment();
            if (size.intValue() > limit && cardinality > sortedMap.firstKey()) {
              size.subtract(sortedMap.pollFirstEntry().getValue().size());
            }
          }
        };
        final HashAggregator<HashCollector> aggregator = new HashAggregator<HashCollector>(selectors, true)
        {
          @Override
          protected final HashCollector newCollector()
          {
            throw new ISE("?");
          }
        };
        while (!cursor.isDone()) {
          aggregator.aggregate(collector);
          cursor.advance();
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
