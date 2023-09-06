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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.Scannable;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.IOUtils;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DictionaryID;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector.WithDictionary;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Dictionary;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

/**
 *
 */
public class CardinalityMetaQueryRunnerFactory extends QueryRunnerFactory.Abstract<CardinalityMeta>
{
  private static final Logger LOG = new Logger(CardinalityMetaQueryRunnerFactory.class);

  @Inject
  public CardinalityMetaQueryRunnerFactory(CardinalityMetaQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<CardinalityMeta> mergeRunners(
      final Query<CardinalityMeta> base,
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<CardinalityMeta>> runners,
      final Supplier<Object> optimizer
  )
  {
    QueryRunner<CardinalityMeta> parallel = super.mergeRunners(
        base,
        queryExecutor,
        Iterables.limit(runners, 2),
        optimizer
    );
    return (query, responseContext) -> Sequences.of(CardinalityMeta.merge(parallel.run(query, responseContext)));
  }

  @Override
  public QueryRunner<CardinalityMeta> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<CardinalityMeta>()
    {
      private final Map<String, HyperLogLogCollector> cardinalities = Maps.newTreeMap();

      @Override
      public Sequence<CardinalityMeta> run(Query<CardinalityMeta> query, Map<String, Object> responseContext)
      {
        CardinalityMetaQuery meta = (CardinalityMetaQuery) query;
        HyperLogLogCollector[] collectors;
        if (meta.getColumns().size() > 1 && meta.isComplex()) {
          collectors = complex(meta, segment, cache);
        } else {
          collectors = dimensions(meta, segment, cache);
        }
        return Sequences.of(CardinalityMeta.of(collectors, segment.getNumRows()));
      }
    };
  }

  private HyperLogLogCollector[] complex(CardinalityMetaQuery query, Segment segment, SessionCache cache)
  {
    List<String> columns = query.getColumns();
    StorageAdapter adapter = segment.asStorageAdapter(false);
    Set<String> dimensions = Sets.newHashSet(adapter.getAvailableDimensions());
    Sequence<Cursor> sequence = adapter.makeCursors(query.withQuerySegmentSpec(segment.asSpec()), cache);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    if (segment.isIndexed() && dimensions.containsAll(columns)) {
      DictionaryID.collect(sequence, DefaultDimensionSpec.toSpec(columns))
                  .forEach(collector::add);
    } else {
      sequence.accumulate(null, (x, c) -> {
        ObjectColumnSelector[] selectors = columns.stream().map(c::makeObjectColumnSelector)
                                                  .toArray(n -> new ObjectColumnSelector[n]);
        LongSupplier supplier = DictionaryID.hasher(selectors);
        for (; !c.isDone(); c.advance()) {
          collector.add(supplier.getAsLong());
        }
        return null;
      });
    }
    return new HyperLogLogCollector[]{collector};
  }

  @SuppressWarnings("unchecked")
  private HyperLogLogCollector[] dimensions(CardinalityMetaQuery query, Segment segment, SessionCache cache)
  {
    List<String> columns = query.getColumns();
    HyperLogLogCollector[] collectors = new HyperLogLogCollector[columns.size()];
    if (segment.isIndexed()) {
      QueryableIndex index = segment.asQueryableIndex(false);
      Set<String> dimensions = Sets.newHashSet(index.getAvailableDimensions());
      for (int i = 0; i < collectors.length; i++) {
        HyperLogLogCollector collector = collectors[i] = HyperLogLogCollector.makeLatestCollector();
        String columnName = columns.get(i);
        if (dimensions.contains(columnName)) {
          Dictionary<String> dictionary = index.getColumn(columnName).getDictionary();
          try {
            dictionary.scan(null, (x, b, o, l) -> collector.add(Murmur3.hash64(b, o, l)));
          }
          finally {
            IOUtils.closeQuietly(dictionary);
          }
        } else {
          Column column = index.getColumn(columnName);
          if (column == null || !column.getType().isString()) {
            continue;
          }
          GenericColumn generic = column.getGenericColumn();
          try {
            if (generic instanceof Scannable.BufferBacked) {
              Scannable.BufferBacked scannable = (Scannable.BufferBacked) generic;
              scannable.scan(null, (x, b, o, l) -> collector.add(Murmur3.hash64(b, o, l)));
            } else if (generic instanceof Scannable) {
              Scannable<String> scannable = (Scannable) generic;
              scannable.scan(null, (x, v) -> collector.add(v));
            } else {
              for (int x = 0; x < column.getNumRows(); x++) {
                collector.add(generic.getString(x));
              }
            }
          }
          finally {
            IOUtils.closeQuietly(generic);
          }
        }
      }
    } else {
      StorageAdapter adapter = segment.asStorageAdapter(false);
      Sequence<Cursor> sequence = adapter.makeCursors(query.withQuerySegmentSpec(segment.asSpec()), cache);
      Set<String> dimensions = Sets.newHashSet(adapter.getAvailableDimensions());
      List<String> metrics = Lists.newArrayList();
      for (int i = 0; i < collectors.length; i++) {
        String columnName = columns.get(i);
        if (dimensions.contains(columnName)) {
          HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
          Sequences.explode(sequence, c -> {
            WithDictionary selector = (WithDictionary) c.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
            selector.values().forEach(collector::add);
            return null;
          });
          collectors[i] = collector;
        } else {
          metrics.add(columnName);
        }
      }
      if (!metrics.isEmpty()) {
        HyperLogLogCollector[] mcollectors = new HyperLogLogCollector[metrics.size()];
        for (int i = 0; i < metrics.size(); i++) {
          mcollectors[i] = HyperLogLogCollector.makeLatestCollector();
        }
        Sequences.explode(sequence, c -> {
          ObjectColumnSelector[] selectors = metrics.stream().map(c::makeObjectColumnSelector)
                                                    .toArray(x -> new ObjectColumnSelector[x]);
          for (; !c.isDone(); c.advance()) {
            for (int i = 0; i < selectors.length; i++) {
              collectors[i].add((String) selectors[i].get());
            }
          }
          return null;
        });
        for (int i = 0; i < metrics.size(); i++) {
          collectors[columns.indexOf(metrics.get(i))] = mcollectors[i];
        }
      }
    }
    return collectors;
  }
}
