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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.CombineFn;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;
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
import io.druid.query.groupby.AggregationCombineFn;
import io.druid.query.groupby.having.HavingSpec.PostMergeSupport;
import io.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.ToIntFunction;

/**
 */
public abstract class BaseAggregationQueryToolChest<T extends BaseAggregationQuery>
    extends QueryToolChest.CacheSupport<Row, Object[], T>
{
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
        if (BaseQuery.isBrokerSide(aggregation)) {
          Sequence<Row> sequence = runner.run(aggregation, responseContext);
          if (BaseQuery.isBySegment(aggregation)) {
            Function function = BySegmentResultValue.applyAll(
                Functions.compose(toPostAggregator(aggregation, false), aggregation.compactToMap(sequence.columns())));
            return Sequences.map(sequence, function);
          }
          boolean finalize = BaseQuery.isFinalize(query);
          sequence = CombiningSequence.create(sequence, getMergeOrdering(aggregation), getMergeFn(aggregation, finalize));
          if (aggregation.getHavingSpec() instanceof PostMergeSupport) {
            RowSignature signature = Queries.postMergeSignature(aggregation, finalize);
            Predicate<Row> predicate = ((PostMergeSupport) aggregation.getHavingSpec()).toCompactEvaluator(signature);
            if (predicate != null) {
              responseContext.put(Query.RESPONSE_HAVING_EVALUATED, true);
              sequence = Sequences.filter(sequence, predicate);
            }
          }
          sequence = postAggregation(aggregation, Sequences.map(sequence, aggregation.compactToMap(sequence.columns())));
          return sequence;
        }
        Sequence<Row> sequence = runner.run(aggregation, responseContext);
        if (aggregation instanceof TimeseriesQuery && !BaseQuery.isBySegment(aggregation)) {
          sequence = CombiningSequence.create(sequence, getMergeOrdering(aggregation), getMergeFn(aggregation, false));
        }
        return sequence;
      }
    };
  }

  protected abstract Comparator<Row> getMergeOrdering(final T aggregation);

  protected CombineFn.Identical<Row> getMergeFn(T aggregation, boolean finalize)
  {
    return AggregationCombineFn.of(aggregation, finalize);
  }

  protected Sequence<Row> postAggregation(final T query, final Sequence<Row> sequence)
  {
    final List<String> columns = GuavaUtils.dedupConcat(
        sequence.columns(), PostAggregators.toNames(query.getPostAggregatorSpecs())
    );
    return Sequences.map(columns, sequence, toPostAggregator(query, BaseQuery.isFinalize(query)));
  }

  private Function<Row, Row> toPostAggregator(final T query, final boolean finalize)
  {
    final RowSignature signature = Queries.postAggregatorSignature(query, finalize);
    final List<PostAggregator.Processor> postAggregators = PostAggregators.toProcessors(
        PostAggregators.decorate(query.getPostAggregatorSpecs(), query.getAggregatorSpecs()), signature
    );
    final Granularity granularity = query.getGranularity();
    if (!postAggregators.isEmpty()) {
      return row -> {
        final Map<String, Object> event = ((MapBasedRow) row).getEvent();
        final Map<String, Object> updatable = MapBasedRow.toUpdatable(event);

        for (PostAggregator.Processor postAggregator : postAggregators) {
          updatable.put(postAggregator.getName(), postAggregator.compute(row.getTimestamp(), updatable));
        }
        final DateTime current = row.getTimestamp();
        if (current == null || granularity.isUTC()) {
          return event == updatable ? row : new MapBasedRow(current, updatable);
        }
        return new MapBasedRow(granularity.toDateTime(current.getMillis()), updatable);
      };
    }
    if (granularity.isUTC()) {
      return GuavaUtils.identity("postAggr");
    }
    return row -> {
      final DateTime current = row.getTimestamp();
      return current == null ? row : ((MapBasedRow) row).withDateTime(granularity.toDateTime(current.getMillis()));
    };
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(final Query<Row> query, final MetricManipulationFn fn)
  {
    if (fn == MetricManipulatorFns.identity()) {
      return super.makePreComputeManipulatorFn(query, fn);
    }
    final List<String> columns = query.estimatedOutputColumns();
    return manipulateMetricOnCompactRow(BaseQuery.getAggregators(query), columns, fn);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<Row> deserializeSequence(Query<Row> query, Sequence sequence, ExecutorService executor)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      Sequence<Object[]> decomposed = Sequences.explode((Sequence<BulkRow>) sequence, bulk -> bulk.decompose(executor));
      Long timestamp = BaseQuery.getUniversalTimestamp(query, null);
      if (timestamp != null) {
        sequence = Sequences.map(decomposed, v -> CompactRow.timestamp(timestamp, v));
      } else {
        sequence = Sequences.map(decomposed, CompactRow::new);
      }
    }
    return super.deserializeSequence(query, sequence, executor);
  }

  @Override
  public ToIntFunction numRows(Query<Row> query)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return v -> ((BulkRow) v).count();
    }
    return super.numRows(query);
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(final Query<Row> query, final MetricManipulationFn fn)
  {
    if (fn == MetricManipulatorFns.identity()) {
      return super.makePostComputeManipulatorFn(query, fn);
    }
    final List<AggregatorFactory> metrics = BaseQuery.getAggregators(query);
    if (!BaseQuery.isBrokerSide(query)) {
      return manipulateMetricOnCompactRow(metrics, query.estimatedOutputColumns(), fn);
    }
    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        final Row.Updatable updatable = Rows.toUpdatable(input);
        for (AggregatorFactory agg : metrics) {
          final String name = agg.getName();
          final Object value = input.getRaw(name);
          final Object manipulated = fn.manipulate(agg, value);
          if (value != manipulated) {
            updatable.set(name, manipulated);
          }
        }
        return updatable;
      }
    };
  }

  private static Function<Row, Row> manipulateMetricOnCompactRow(
      List<AggregatorFactory> factories,
      List<String> columns,
      MetricManipulationFn fn
  )
  {
    final int[] indices = GuavaUtils.indexOf(columns, AggregatorFactory.toNames(factories));
    final AggregatorFactory[] metrics = factories.toArray(new AggregatorFactory[0]);

    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        final Object[] values = ((CompactRow) input).getValues();
        for (int i = 0; i < metrics.length; i++) {
          if (indices[i] >= 0) {
            values[indices[i]] = fn.manipulate(metrics[i], values[indices[i]]);
          }
        }
        return input;
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeReference getResultTypeReference(Query<Row> query)
  {
    if (query != null && query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return BulkRow.TYPE_REFERENCE;
    } else {
      return ROW_TYPE_REFERENCE;
    }
  }

  @Override
  public CacheStrategy<Row, Object[], T> getCacheStrategy(final T base)
  {
    return new CacheStrategy<Row, Object[], T>()
    {
      @Override
      public byte[] computeCacheKey(T query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(queryCode())
                         .append(query.getGranularity())
                         .append(query.getFilter())
                         .append(query.getVirtualColumns())
                         .append(query.getDimensions())
                         .append(query.getAggregatorSpecs())
                         .build();
      }

      @Override
      public TypeReference<Object[]> getCacheObjectClazz()
      {
        return ARRAY_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object[]> prepareForCache(T query)
      {
        return CompactRow.UNWRAP;
      }

      @Override
      public Function<Object[], Row> pullFromCache(T query)
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
  public Function<Sequence<Row>, Sequence<Map<String, Object>>> asMap(final Query<Row> query, final String timestampColumn)
  {
    return new Function<Sequence<Row>, Sequence<Map<String, Object>>>()
    {
      @Override
      public Sequence<Map<String, Object>> apply(Sequence<Row> input)
      {
        return Sequences.map(input, new Function<Row, Map<String, Object>>()
        {
          @Override
          public Map<String, Object> apply(Row input)
          {
            Map<String, Object> event = ((MapBasedRow) input).getEvent();
            if (timestampColumn != null) {
              event = MapBasedRow.toUpdatable(event);
              event.put(timestampColumn, input.getTimestamp());
            }
            return event;
          }
        });
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
        Sequence<Row> sequence = runner.run(query, responseContext);
        if (BaseQuery.isBrokerSide(query)) {
          sequence = finalDecoration(query, sequence, responseContext);
        }
        return sequence;
      }
    };
  }

  @SuppressWarnings("unchecked")
  private Sequence<Row> finalDecoration(Query<Row> query, Sequence<Row> sequence, Map<String, Object> response)
  {
    T aggregation = (T) query;
    if (response != null && response.get(Query.RESPONSE_HAVING_EVALUATED) != null) {
      aggregation = (T) aggregation.withHavingSpec(null);
    }
    sequence = aggregation.applyLimit(sequence, aggregation.isSortOnTimeForLimit(isSortOnTime()));

    final List<String> outputColumns = aggregation.getOutputColumns();
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      if (Rows.isEquivalent(sequence.columns(), outputColumns)) {
        sequence = Sequences.map(outputColumns, sequence, Functions.identity());  // just change column-names
      } else {
        sequence = Sequences.map(
            outputColumns, sequence, input -> {
              DateTime timestamp = input.getTimestamp();
              Map<String, Object> retained = Maps.newHashMapWithExpectedSize(outputColumns.size());
              for (String retain : outputColumns) {
                retained.put(retain, input.getRaw(retain));
              }
              return new MapBasedRow(timestamp, retained);
            }
        );
      }
    }
    final LateralViewSpec lateralViewSpec = aggregation.getLateralView();
    return lateralViewSpec != null ? toLateralView(sequence, lateralViewSpec) : sequence;
  }

  private Sequence<Row> toLateralView(final Sequence<Row> sequence, final LateralViewSpec lateralViewSpec)
  {
    List<String> columns = sequence.columns();
    if (columns != null && lateralViewSpec instanceof RowSignature.Evolving) {
      columns = ((RowSignature.Evolving) lateralViewSpec).evolve(columns);
    }
    final Function<Map<String, Object>, Iterable<Map<String, Object>>> function = lateralViewSpec.prepare();
    return Sequences.explode(
        columns, sequence, new Function<Row, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Row input)
          {
            final DateTime timestamp = input.getTimestamp();
            final Map<String, Object> event = ((MapBasedRow) input).getEvent();
            return Sequences.simple(
                Iterables.transform(
                    function.apply(event),
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
    );
  }

  protected boolean isSortOnTime()
  {
    return false;
  }
}
