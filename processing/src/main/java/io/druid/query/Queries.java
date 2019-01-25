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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByMetaQuery;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.select.EventHolder;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRow;
import io.druid.query.sketch.GenericSketchAggregatorFactory;
import io.druid.query.sketch.QuantileOperation;
import io.druid.query.sketch.SketchOp;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.DimensionSpecVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class Queries
{
  private static final Logger LOG = new Logger(Queries.class);

  public static void verifyAggregations(
      List<String> columns,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    Preconditions.checkArgument(!columns.contains(Column.TIME_COLUMN_NAME), "__time cannot be used as output name");
    Preconditions.checkNotNull(aggFactories, "aggregations cannot be null");

    final Set<String> aggNames = Sets.newHashSet();
    for (AggregatorFactory aggFactory : aggFactories) {
      Preconditions.checkArgument(aggNames.add(aggFactory.getName()), "[%s] already defined", aggFactory.getName());
    }

    if (postAggs != null && !postAggs.isEmpty()) {
      final Set<String> combinedAggNames = Sets.newHashSet(aggNames);
      combinedAggNames.addAll(columns);

      for (PostAggregator postAgg : postAggs) {
        final Set<String> dependencies = postAgg.getDependentFields();
        final Set<String> missing = Sets.difference(dependencies, combinedAggNames);

        Preconditions.checkArgument(
            missing.isEmpty() || missing.size() == 1 && missing.contains(Column.TIME_COLUMN_NAME),
            "Missing fields %s for postAggregator [%s]", missing, postAgg.getName()
        );
        Preconditions.checkArgument(combinedAggNames.add(postAgg.getName()), "[%s] already defined", postAgg.getName());
      }
    }
  }

  public static List<AggregatorFactory> getAggregators(Query query)
  {
    if (query instanceof Query.AggregationsSupport) {
      return ((Query.AggregationsSupport<?>) query).getAggregatorSpecs();
    }
    return ImmutableList.of();
  }

  public static List<PostAggregator> getPostAggregators(Query query)
  {
    if (query instanceof Query.AggregationsSupport) {
      return ((Query.AggregationsSupport<?>) query).getPostAggregatorSpecs();
    }
    return ImmutableList.of();
  }

  public static <T> T convert(Object object, ObjectMapper jsonMapper, Class<T> expected)
  {
    try {
      return jsonMapper.convertValue(object, expected);
    }
    catch (Exception ex) {
      LOG.warn(ex, "Failed to convert to " + expected.getClass().getSimpleName());
    }
    return null;
  }

  public static Query toQuery(Map<String, Object> object, ObjectMapper jsonMapper)
  {
    return convert(object, jsonMapper, Query.class);
  }

  public static IncrementalIndexSchema relaySchema(Query subQuery, QuerySegmentWalker segmentWalker)
  {
    ObjectMapper mapper = segmentWalker.getObjectMapper();
    IncrementalIndexSchema schema = _relaySchema(subQuery, segmentWalker);
    PostProcessingOperator postProcessor = PostProcessingOperators.load(subQuery, mapper);
    if (postProcessor instanceof PostProcessingOperator.SchemaResolving) {
      schema = ((PostProcessingOperator.SchemaResolving) postProcessor).resolve(subQuery, schema, mapper);
    }
    return schema;
  }

  private static IncrementalIndexSchema _relaySchema(Query subQuery, QuerySegmentWalker segmentWalker)
  {
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema.Builder builder = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(Granularities.ALL)
        .withFixedSchema(true);

    if (subQuery instanceof Query.ColumnsSupport) {
      // no type information in query
      Query.ColumnsSupport<?> columnsSupport = (Query.ColumnsSupport) subQuery;
      Schema schema = QueryUtils.retrieveSchema(subQuery, segmentWalker);
      Schema resolved = schema.resolve(subQuery, false);
      if (!GuavaUtils.isNullOrEmpty(resolved.getDimensionNames())) {
        builder.withDimensions(resolved.getDimensionNames(), resolved.getDimensionTypes());
      }
      if (!GuavaUtils.isNullOrEmpty(resolved.getMetricNames())) {
        builder.withMetrics(AggregatorFactory.toRelay(resolved.getMetricNames(), resolved.getMetricTypes()));
      }
      return builder.withRollup(false).build();
    }
    Schema instance = Schema.EMPTY.resolve(subQuery, false);
    if (subQuery instanceof Query.DimensionSupport) {
      List<String> dimensions = DimensionSpecs.toOutputNames(((Query.DimensionSupport<?>) subQuery).getDimensions());
      List<ValueDesc> types = instance.tryColumnTypes(dimensions);
      if (types == null) {
        instance = QueryUtils.retrieveSchema(subQuery, segmentWalker).resolve(subQuery, false);
        types = instance.tryColumnTypes(dimensions);
      }
      builder.withDimensions(dimensions, Preconditions.checkNotNull(types));
    }
    if (subQuery instanceof Query.MetricSupport) {
      List<String> metrics = ((Query.MetricSupport<?>) subQuery).getMetrics();
      List<ValueDesc> types = instance.tryColumnTypes(metrics);
      if (types == null) {
        instance = QueryUtils.retrieveSchema(subQuery, segmentWalker).resolve(subQuery, false);
        types = instance.tryColumnTypes(metrics);
      }
      return builder.withMetrics(metrics, Preconditions.checkNotNull(types))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof Query.AggregationsSupport) {
      // todo: cannot handle lateral view, windowing, post-processing, etc. should throw exception ?
      Query.AggregationsSupport<?> aggrSupport = (Query.AggregationsSupport) subQuery;
      List<AggregatorFactory> aggregators = aggrSupport.getAggregatorSpecs();
      List<PostAggregator> postAggregators = aggrSupport.getPostAggregatorSpecs();
      List<String> metrics = AggregatorFactory.toNames(aggregators, postAggregators);
      List<ValueDesc> types = instance.tryColumnTypes(metrics);
      if (types == null) {
        instance = QueryUtils.retrieveSchema(subQuery, segmentWalker).resolve(subQuery, false);
        types = instance.tryColumnTypes(metrics);
      }
      return builder.withMetrics(metrics, Preconditions.checkNotNull(types))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate joinQuery = (JoinQuery.JoinDelegate) subQuery;
      List<String> dimensions = Lists.newArrayList();
      List<AggregatorFactory> metrics = Lists.newArrayList();
      List queries = joinQuery.getQueries();
      List<String> aliases = joinQuery.getPrefixAliases();
      for (int i = 0; i < queries.size(); i++) {
        final String prefix = aliases == null ? "" : aliases.get(i) + ".";
        IncrementalIndexSchema schema = relaySchema((Query) queries.get(i), segmentWalker);
        for (String dimension : schema.getDimensionsSpec().getDimensionNames()) {
          String prefixed = prefix + dimension;
          if (!dimensions.contains(prefixed)) {
            dimensions.add(prefixed);
          }
        }
        AggregatorFactory[] aggregators = schema.getMetrics();
        for (AggregatorFactory aggregator : aggregators) {
          Preconditions.checkArgument(aggregator instanceof RelayAggregatorFactory);
          metrics.add(new RelayAggregatorFactory(prefix + aggregator.getName(), aggregator.getOutputType()));
        }
      }
      return builder.withDimensions(dimensions)
                    .withMetrics(AggregatorFactory.toRelay(metrics))
                    .withRollup(false)
                    .build();
    } else {
      // todo union-all (partitioned-join, etc.)
      throw new UnsupportedOperationException("Cannot extract metric from query " + subQuery);
    }
  }

  @SuppressWarnings("unchecked")
  public static <I> Sequence<Row> convertToRow(Query<I> subQuery, Sequence<I> sequence)
  {
    if (subQuery instanceof JoinQuery.JoinDelegate) {
      final String timeColumn = ((JoinQuery.JoinDelegate) subQuery).getTimeColumnName();
      return Sequences.map((Sequence<Map<String, Object>>) sequence, Rows.mapToRow(timeColumn));
    } else if (subQuery instanceof SelectQuery) {
      return Sequences.explode((Sequence<Result<SelectResultValue>>) sequence, SELECT_TO_ROWS);
    } else if (subQuery instanceof StreamQuery) {
      return Sequences.map((Sequence<StreamQueryRow>) sequence, STREAM_TO_ROW);
    } else if (subQuery instanceof TopNQuery) {
      return Sequences.explode((Sequence<Result<TopNResultValue>>) sequence, TOP_N_TO_ROWS);
    } else if (subQuery instanceof BaseAggregationQuery) {
      return (Sequence<Row>) sequence;
    }
    return Sequences.map(sequence, GuavaUtils.<I, Row>caster());
  }

  public static Function<StreamQueryRow, Row> STREAM_TO_ROW =
      new Function<StreamQueryRow, Row>()
      {
        @Override
        public Row apply(StreamQueryRow input)
        {
          return new MapBasedRow(input.getTimestamp(), input);
        }
      };

  public static Function<Result<TopNResultValue>, Sequence<Row>> TOP_N_TO_ROWS =
      new Function<Result<TopNResultValue>, Sequence<Row>>()
      {
        @Override
        public Sequence<Row> apply(Result<TopNResultValue> input)
        {
          final DateTime dateTime = input.getTimestamp();
          return Sequences.simple(
              Iterables.transform(
                  input.getValue(), new Function<Map<String, Object>, Row>()
                  {
                    @Override
                    public Row apply(Map<String, Object> input)
                    {
                      return new MapBasedRow(dateTime, input);
                    }
                  }
              )
          );
        }
      };

  public static Function<Result<SelectResultValue>, Sequence<Row>> SELECT_TO_ROWS =
      new Function<Result<SelectResultValue>, Sequence<Row>>()
      {
        @Override
        public Sequence<Row> apply(Result<SelectResultValue> input)
        {
          final DateTime dateTime = input.getTimestamp();
          return Sequences.simple(
              Iterables.transform(
                  input.getValue(), new Function<EventHolder, Row>()
                  {
                    @Override
                    public Row apply(EventHolder input)
                    {
                      return new MapBasedRow(dateTime, input.getEvent());
                    }
                  }
              )
          );
        }
      };

  @SuppressWarnings("unchecked")
  public static <I> Sequence<I> convertBack(Query<I> subQuery, Sequence<Row> sequence)
  {
    if (subQuery instanceof BaseAggregationQuery) {
      return Sequences.map(sequence, GuavaUtils.<Row, I>caster());
    }
    throw new UnsupportedOperationException("cannot convert to " + subQuery.getType() + " result");
  }

  public static Map<String, Object> extractContext(Query<?> query, String... keys)
  {
    Map<String, Object> context = query.getContext();
    if (context == null) {
      context = Maps.newHashMap();
    }
    Map<String, Object> extracted = Maps.newHashMap();
    for (String key : keys) {
      if (context.containsKey(key)) {
        extracted.put(key, context.get(key));
      }
    }
    return extracted;
  }

  public static Query iterate(Query query, Function<Query, Query> function)
  {
    if (query.getDataSource() instanceof QueryDataSource) {
      Query source = ((QueryDataSource) query.getDataSource()).getQuery();
      Query converted = iterate(source, function);
      if (source != converted) {
        query = query.withDataSource(new QueryDataSource(converted));
      }
    } else if (query instanceof JoinQuery) {
      JoinQuery joinQuery = (JoinQuery) query;
      for (Map.Entry<String, DataSource> entry : joinQuery.getDataSources().entrySet()) {
        if (entry.getValue() instanceof QueryDataSource) {
          Query source = ((QueryDataSource) entry.getValue()).getQuery();
          Query converted = iterate(source, function);
          if (source != converted) {
            entry.setValue(new QueryDataSource(converted));
          }
        }
      }
    } else if (query instanceof UnionAllQuery) {
      UnionAllQuery<?> union = (UnionAllQuery) query;
      if (union.getQuery() != null) {
        Query source = union.getQuery();
        Query converted = iterate(source, function);
        if (source != converted) {
          query = union.withQuery(converted);
        }
      } else {
        boolean changed = false;
        List<Query> queries = Lists.newArrayList();
        for (Query source : union.getQueries()) {
          Query converted = iterate(source, function);
          changed |= source != converted;
          queries.add(converted);
        }
        if (changed) {
          query = union.withQueries(queries);
        }
      }
    } else if (query instanceof Query.WrappingQuery) {
      Query.WrappingQuery wrapping = (Query.WrappingQuery) query;
      Query source = wrapping.query();
      Query converted = iterate(source, function);
      if (source != converted) {
        return wrapping.withQuery(converted);
      }
    }
    return function.apply(query);
  }

  public static long estimateCardinality(
      GroupByQuery query,
      QuerySegmentWalker segmentWalker,
      QueryConfig config
  )
  {
    ObjectMapper objectMapper = segmentWalker.getObjectMapper();
    query = query.withOverriddenContext(BaseQuery.copyContextForMeta(query));
    Query<Row> counter = new GroupByMetaQuery(query).rewriteQuery(segmentWalker, config);
    Row row = Sequences.only(counter.run(segmentWalker, Maps.<String, Object>newHashMap()));
    return row.getLongMetric("cardinality");
  }

  public static int getNumSplits(List<DictionaryEncodedColumn> dictionaries, int numSplit)
  {
    Union union = (Union) SetOperation.builder().setNominalEntries(64).build(Family.UNION);
    for (DictionaryEncodedColumn dictionary : dictionaries) {
      union.update(dictionary.getTheta());
    }
    int cardinality = (int) union.getResult().getEstimate();
    if (cardinality > 0) {
      return Math.max(numSplit, 1 + (cardinality >> 18));
    }
    return numSplit;
  }

  public static Object[] getThresholds(List<DictionaryEncodedColumn> dictionaries, int numSplit, String strategy)
  {
    ItemsUnion<String> itemsUnion = ItemsUnion.getInstance(32, Ordering.natural().nullsFirst());
    for (DictionaryEncodedColumn dictionary : dictionaries) {
      itemsUnion.update(dictionary.getQuantile());
    }
    if (!itemsUnion.isEmpty()) {
      return (Object[]) QuantileOperation.QUANTILES.calculate(
          itemsUnion.getResult(), QuantileOperation.valueOf(strategy, numSplit + 1, true)
      );
    }
    return null;
  }

  private static final String DUMMY_VC = "$VC";

  public static Object[] makeColumnHistogramOn(
      Supplier<RowResolver> supplier,
      QuerySegmentWalker segmentWalker,
      TimeseriesQuery metaQuery,
      DimensionSpec dimensionSpec,
      int numSplits,
      String splitType
  )
  {
    if (!Granularities.ALL.equals(metaQuery.getGranularity())) {
      return null;
    }
    ValueDesc type = dimensionSpec.resolve(supplier.get());
    if (type.isDimension()) {
      type = ValueDesc.STRING;
    }
    if (!type.isPrimitive()) {
      return null;  // todo
    }
    List<OrderingSpec> orderingSpecs = Lists.newArrayList();
    List<VirtualColumn> virtualColumns = Lists.newArrayList(metaQuery.getVirtualColumns());
    String fieldName = dimensionSpec.getDimension();
    if (dimensionSpec instanceof DimensionSpecWithOrdering) {
      DimensionSpecWithOrdering explicit = (DimensionSpecWithOrdering) dimensionSpec;
      orderingSpecs.add(explicit.asOrderingSpec());
      dimensionSpec = explicit.getDelegate();
    }
    if (!(dimensionSpec instanceof DefaultDimensionSpec)) {
      virtualColumns.add(DimensionSpecVirtualColumn.wrap(dimensionSpec, DUMMY_VC));
      fieldName = DUMMY_VC;
    }

    AggregatorFactory aggregator = new GenericSketchAggregatorFactory(
        "SKETCH", fieldName, type, SketchOp.QUANTILE, 128, orderingSpecs, false
    );

    Map<String, Object> pg = ImmutableMap.<String, Object>builder()
                                         .put("type", "sketch.quantiles")
                                         .put("name", "SPLIT")
                                         .put("fieldName", "SKETCH")
                                         .put("op", "QUANTILES")
                                         .put("dedup", true)
                                         .put(splitType, numSplits + 1)
                                         .build();

    PostAggregator postAggregator = Queries.convert(pg, segmentWalker.getObjectMapper(), PostAggregator.class);
    if (postAggregator == null) {
      LOG.info("Failed to convert map to 'sketch.quantiles' operator.. fix this");
      return null;
    }

    metaQuery = metaQuery.withGranularity(Granularities.ALL)
                         .withVirtualColumns(virtualColumns)
                         .withAggregatorSpecs(Arrays.asList(aggregator))
                         .withPostAggregatorSpecs(Arrays.asList(postAggregator))
                         .withOutputColumns(Arrays.asList("SPLIT"))
                         .withOverriddenContext(Query.LOCAL_POST_PROCESSING, true);

    Row result = Sequences.only(
        metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()), null
    );

    return result == null ? null : (Object[]) result.getRaw("SPLIT");
  }


  public static <I, T> QueryRunner<T> makeIteratingQueryRunner(
      final Query.IteratingQuery<I, T> iterating,
      final QuerySegmentWalker walker
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            new Iterable<Sequence<T>>()
            {
              @Override
              public Iterator<Sequence<T>> iterator()
              {
                return new Iterator<Sequence<T>>()
                {
                  private Query<I> query = iterating.next(null, null).rhs;

                  @Override
                  public boolean hasNext()
                  {
                    return query != null;
                  }

                  @Override
                  public Sequence<T> next()
                  {
                    Sequence<I> sequence = query.run(walker, responseContext);
                    Pair<Sequence<T>, Query<I>> next = iterating.next(sequence, query);
                    query = next.rhs;
                    return next.lhs;
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException("remove");
                  }
                };
              }
            }
        );
      }
    };
  }
}
