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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.AggregationQueryBinaryFn;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 */
public abstract class BaseAggregationQueryToolChest<T extends BaseAggregationQuery>
    extends QueryToolChest.CacheSupport<Row, Object[], T>
{
  private static final TypeReference<Object[]> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object[]>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public BaseAggregationQueryToolChest(IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        T aggregation = (T) query;
        if (aggregation.getContextBoolean(QueryContextKeys.FINAL_MERGE, true)) {
          Sequence<Row> sequence = runner.run(aggregation.removePostActions(), responseContext);
          if (BaseQuery.getContextBySegment(aggregation)) {
            return Sequences.map((Sequence) sequence, BySegmentResultValueClass.applyAll(
                Functions.compose(toPostAggregator(aggregation), toMapBasedRow(aggregation)))
            );
          }
          sequence = CombiningSequence.create(sequence, getMergeOrdering(aggregation), getMergeFn(aggregation));
          sequence = Sequences.map(
              sequence, Functions.compose(toPostAggregator(aggregation), toMapBasedRow(aggregation))
          );
          return sequence;
        }
        return runner.run(aggregation, responseContext);
      }
    };
  }

  protected abstract Ordering<Row> getMergeOrdering(final T aggregation);

  protected BinaryFn getMergeFn(T aggregation)
  {
    return new AggregationQueryBinaryFn(aggregation);
  }

  protected Function<Row, Row> toMapBasedRow(final T query)
  {
    return new Function<Row, Row>()
    {
      private final Granularity granularity = query.getGranularity();
      private final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
      private final List<String> metrics = AggregatorFactory.toNames(query.getAggregatorSpecs());

      @Override
      public Row apply(Row input)
      {
        final Object[] values = ((CompactRow) input).getValues();
        final Map<String, Object> event = Maps.newLinkedHashMap();
        int x = 1;
        for (String dimension : dimensions) {
          event.put(dimension, values[x++]);
        }
        for (String metric : metrics) {
          event.put(metric, values[x++]);
        }
        return new MapBasedRow(granularity.toDateTime(input.getTimestampFromEpoch()), event);
      }
    };
  }

  protected Function<Row, Row> toPostAggregator(final T query)
  {
    final Granularity granularity = query.getGranularity();
    final List<PostAggregator> postAggregators = PostAggregators.decorate(
        query.getPostAggregatorSpecs(),
        query.getAggregatorSpecs()
    );
    if (postAggregators.isEmpty() && granularity.isUTC()) {
      return Functions.identity();
    }
    return new Function<Row, Row>()
    {
      @Override
      public Row apply(final Row row)
      {
        final Map<String, Object> newMap = Maps.newLinkedHashMap(((MapBasedRow) row).getEvent());

        for (PostAggregator postAggregator : postAggregators) {
          newMap.put(postAggregator.getName(), postAggregator.compute(row.getTimestamp(), newMap));
        }
        return new MapBasedRow(granularity.toDateTime(row.getTimestampFromEpoch()), newMap);
      }
    };
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(final T query, final MetricManipulationFn fn)
  {
    if (fn == MetricManipulatorFns.identity()) {
      return Functions.identity();
    }
    return new Function<Row, Row>()
    {
      private final int start = query.getDimensions().size() + 1;
      private final List<AggregatorFactory> metrics = query.getAggregatorSpecs();

      @Override
      public Row apply(Row input)
      {
        final Object[] values = ((CompactRow) input).getValues();
        int x = start;
        for (AggregatorFactory metric : metrics) {
          values[x] = fn.manipulate(metric, values[x++]);
        }
        return input;
      }
    };
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(final T query, final MetricManipulationFn fn)
  {
    if (fn == MetricManipulatorFns.identity()) {
      return Functions.identity();
    }
    return new Function<Row, Row>()
    {
      private final int start = query.getDimensions().size() + 1;
      private final List<AggregatorFactory> metrics = query.getAggregatorSpecs();

      @Override
      public Row apply(Row input)
      {
        final Row.Updatable updatable = Rows.toUpdatable(input);
        for (AggregatorFactory agg : metrics) {
          final String name = agg.getName();
          updatable.set(name, fn.manipulate(agg, input.getRaw(name)));
        }
        return updatable;
      }
    };
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CacheStrategy<Row, Object[], T> getCacheStrategy(final T query)
  {
    return new CacheStrategy<Row, Object[], T>()
    {
      @Override
      public byte[] computeCacheKey(T query)
      {
        final byte[] granularityBytes = QueryCacheHelper.computeCacheBytes(query.getGranularity());
        final byte[] filterBytes = QueryCacheHelper.computeCacheBytes(query.getDimFilter());
        final byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        final byte[] dimensionsBytes = QueryCacheHelper.computeCacheKey(query.getDimensions());
        final byte[] aggregatorBytes = QueryCacheHelper.computeCacheKeys(query.getAggregatorSpecs());

        return ByteBuffer
            .allocate(
                2
                + granularityBytes.length
                + filterBytes.length
                + vcBytes.length
                + dimensionsBytes.length
                + aggregatorBytes.length
            )
            .put(queryCode())
            .put(granularityBytes)
            .put(filterBytes)
            .put(vcBytes)
            .put(dimensionsBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<Object[]> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object[]> prepareForCache()
      {
        return new Function<Row, Object[]>()
        {
          @Override
          public Object[] apply(Row input)
          {
            return ((CompactRow) input).getValues();
          }
        };
      }

      @Override
      public Function<Object[], Row> pullFromCache()
      {
        return new Function<Object[], Row>()
        {
          private final int start = 1 + query.getDimensions().size();
          private final List<AggregatorFactory> metrics = query.getAggregatorSpecs();

          @Override
          public Row apply(final Object[] input)
          {
            int x = start;
            for (AggregatorFactory metric : metrics) {
              input[x] = metric.deserialize(input[x++]);
            }
            return new CompactRow(input);
          }
        };
      }
    };
  }

  protected abstract byte queryCode();

  @Override
  public TabularFormat toTabularFormat(
      final T query,
      final Sequence<Row> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(
            sequence, new Function<Row, Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(Row input)
              {
                Map<String, Object> event = ((MapBasedRow) input).getEvent();
                if (timestampColumn != null) {
                  if (!MapBasedRow.supportInplaceUpdate(event)) {
                    event = Maps.newLinkedHashMap(event);
                  }
                  event.put(timestampColumn, input.getTimestamp());
                }
                return event;
              }
            }
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
  public QueryRunner<Row> finalQueryDecoration(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        return finalDecoration(query, runner.run(query, responseContext));
      }
    };
  }

  @SuppressWarnings("unchecked")
  private Sequence<Row> finalDecoration(Query<Row> query, Sequence<Row> sequence)
  {
    T aggregation = (T) query;
    sequence = aggregation.applyLimit(sequence, aggregation.isSortOnTimeForLimit(isSortOnTime()));

    final List<String> outputColumns = aggregation.getOutputColumns();
    final LateralViewSpec lateralViewSpec = aggregation.getLateralView();
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      sequence = Sequences.map(
          sequence, new Function<Row, Row>()
          {
            @Override
            public Row apply(Row input)
            {
              DateTime timestamp = input.getTimestamp();
              Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
              for (String retain : outputColumns) {
                retained.put(retain, input.getRaw(retain));
              }
              return new MapBasedRow(timestamp, retained);
            }
          }
      );
    }
    return lateralViewSpec != null ? toLateralView(sequence, lateralViewSpec) : sequence;
  }

  private Sequence<Row> toLateralView(final Sequence<Row> result, final LateralViewSpec lateralViewSpec)
  {
    return Sequences.concat(
        Sequences.map(
            result, new Function<Row, Sequence<Row>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Sequence<Row> apply(Row input)
              {
                final DateTime timestamp = input.getTimestamp();
                final Map<String, Object> event = ((MapBasedRow) input).getEvent();
                return Sequences.simple(
                    Iterables.transform(
                        lateralViewSpec.apply(event),
                        new Function<Map<String, Object>, Row>()
                        {
                          @Override
                          public Row apply(Map<String, Object> input)
                          {
                            return new MapBasedRow(timestamp, input);
                          }
                        }
                    )
                );
              }
            }
        )
    );
  }

  protected boolean isSortOnTime()
  {
    return false;
  }
}
