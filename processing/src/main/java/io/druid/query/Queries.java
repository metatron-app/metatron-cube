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
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularities;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.PartitionedGroupByQuery;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.select.EventHolder;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRow;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.DimensionSpecVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;

import java.util.Arrays;
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
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema.Builder builder = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(QueryGranularities.ALL)
        .withFixedSchema(true);

    // cannot handle lateral view, windowing, post-processing, etc.
    // throw exception ?
    if (subQuery instanceof Query.MetricSupport) {
      Query.MetricSupport<?> metricSupport = (Query.MetricSupport) subQuery;
      Schema schema = QueryUtils.resolveSchema(subQuery, segmentWalker);
      List<String> dimensionNames = DimensionSpecs.toOutputNames(metricSupport.getDimensions());
      return builder.withDimensions(dimensionNames, schema.resolveDimensionTypes(metricSupport))
                    .withMetrics(AggregatorFactory.toRelay(schema.resolveMetricTypes(metricSupport)))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<?> aggrSupport = (Query.AggregationsSupport) subQuery;
      List<String> dimensionNames = DimensionSpecs.toOutputNames(aggrSupport.getDimensions());
      List<PostAggregator> postAggregators = aggrSupport.getPostAggregatorSpecs();
      if (aggrSupport.needsSchemaResolution()) {
        Schema schema = QueryUtils.resolveSchema(subQuery, segmentWalker);
        return builder.withDimensions(dimensionNames, schema.resolveDimensionTypes(aggrSupport))
                      .withMetrics(AggregatorFactory.toRelay(aggrSupport.getAggregatorSpecs(), postAggregators))
                      .withRollup(false)
                      .build();
      } else {
        return builder.withDimensions(dimensionNames, DimensionSpecs.toOutputTypes(aggrSupport))
                      .withMetrics(AggregatorFactory.toRelay(aggrSupport.getAggregatorSpecs(), postAggregators))
                      .withRollup(false)
                      .build();
      }
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate<?> joinQuery = (JoinQuery.JoinDelegate) subQuery;
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
          metrics.add(new RelayAggregatorFactory(prefix + aggregator.getName(), aggregator.getTypeName()));
        }
      }
      // multiple timestamps.. we use timestamp from the first alias as indexing timestamp and add others into metric
      if (aliases != null) {
        for (String alias : aliases) {
          metrics.add(new RelayAggregatorFactory(alias + "." + EventHolder.timestampKey, ValueDesc.LONG_TYPE));
        }
      }
      return builder.withDimensions(dimensions)
                    .withMetrics(AggregatorFactory.toRelay(metrics))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof PartitionedGroupByQuery.GroupByDelegate) {
      GroupByQuery query = (GroupByQuery) ((UnionAllQuery) subQuery).getQueries().get(0);
      return relaySchema(query, segmentWalker);
    } else {
      // todo union-all (partitioned-join, etc.)
      // todo timeseries topN query
      throw new UnsupportedOperationException("Cannot extract metric from query " + subQuery);
    }
  }

  @SuppressWarnings("unchecked")
  public static <I> Sequence<Row> convertToRow(Query<I> subQuery, Sequence<I> sequence)
  {
    if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate delegate = (JoinQuery.JoinDelegate) subQuery;
      final String timeColumn = delegate.getPrefixAliases() == null
                                ? EventHolder.timestampKey
                                : delegate.getPrefixAliases().get(0) + "." + EventHolder.timestampKey;
      return Sequences.map(
          sequence, new Function<I, Row>()
          {
            @Override
            @SuppressWarnings("unchecked")
            public Row apply(I input)
            {
              Map<String, Object> event = (Map<String, Object>) input;
              return new MapBasedRow((Long) event.get(timeColumn), event);
            }
          }
      );
    } else if (subQuery instanceof SelectQuery) {
      Sequence<Result<SelectResultValue>> select = (Sequence<Result<SelectResultValue>>) sequence;
      return Sequences.concat(
          Sequences.map(
              select, new Function<Result<SelectResultValue>, Sequence<Row>>()
              {
                @Override
                public Sequence<Row> apply(Result<SelectResultValue> input)
                {
                  final DateTime timestamp = input.getTimestamp();
                  return Sequences.simple(
                      Lists.transform(
                          input.getValue().getEvents(),
                          new Function<EventHolder, Row>()
                          {
                            @Override
                            public Row apply(EventHolder input)
                            {
                              return new MapBasedRow(timestamp, input.getEvent());
                            }
                          }
                      )
                  );
                }
              }
          )
      );
    } else if (subQuery instanceof StreamQuery) {
      Sequence<StreamQueryRow> stream = (Sequence<StreamQueryRow>) sequence;
      return Sequences.map(
          stream, new Function<StreamQueryRow, Row>()
          {
            @Override
            public Row apply(StreamQueryRow input)
            {
              return new MapBasedRow(input.getTimestamp(), input);
            }
          }
      );
    } else if (subQuery instanceof TopNQuery) {
      Sequence<Result<TopNResultValue>> topN = (Sequence<Result<TopNResultValue>>) sequence;
      return Sequences.concat(
          Sequences.map(
              topN, new Function<Result<TopNResultValue>, Sequence<Row>>()
              {
                @Override
                public Sequence<Row> apply(Result<TopNResultValue> input)
                {
                  final DateTime timestamp = input.getTimestamp();
                  return Sequences.simple(
                      Lists.transform(
                          input.getValue().getValue(),
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
    } else if (subQuery instanceof TimeseriesQuery) {
      return Sequences.map((Sequence<Result<TimeseriesResultValue>>) sequence, TIMESERIES_TO_ROW);
    } else if (subQuery instanceof GroupByQuery) {
      return (Sequence<Row>) sequence;
    }
    return Sequences.map(sequence, GuavaUtils.<I, Row>caster());
  }

  public static Function<Result<TimeseriesResultValue>, Row> TIMESERIES_TO_ROW =
      new Function<Result<TimeseriesResultValue>, Row>()
      {
        @Override
        public Row apply(Result<TimeseriesResultValue> input)
        {
          return new MapBasedRow(input.getTimestamp(), input.getValue().getBaseObject());
        }
      };

  public static Function<Row, Result<TimeseriesResultValue>> ROW_TO_TIMESERIES =
      new Function<Row, Result<TimeseriesResultValue>>()
      {
        @Override
        public Result<TimeseriesResultValue> apply(Row input)
        {
          return new Result<TimeseriesResultValue>(input.getTimestamp(), new TimeseriesResultValue(Rows.asMap(input)));
        }
      };

  @SuppressWarnings("unchecked")
  public static <I> Sequence<I> convertBack(Query<I> subQuery, Sequence<Row> sequence)
  {
    if (subQuery instanceof TimeseriesQuery) {
      return (Sequence<I>) Sequences.map(sequence, ROW_TO_TIMESERIES);
    }
    if (subQuery instanceof GroupByQuery) {
      return Sequences.map(sequence, GuavaUtils.<Row, I>caster());
    }
    throw new UnsupportedOperationException("cannot convert to " + subQuery.getType() + " result");
  }

  public static Map<String, Object> extractContext(Query<?> query, String... keys)
  {
    Map<String, Object> context = query.getContext();
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
      if (!source.equals(converted)) {
        query = query.withDataSource(new QueryDataSource(converted));
      }
    } else if (query instanceof JoinQuery) {
      JoinQuery<?> joinQuery = (JoinQuery) query;
      for (Map.Entry<String, DataSource> entry : joinQuery.getDataSources().entrySet()) {
        if (entry.getValue() instanceof QueryDataSource) {
          Query source = ((QueryDataSource) entry.getValue()).getQuery();
          Query converted = iterate(source, function);
          if (!source.equals(converted)) {
            entry.setValue(new QueryDataSource(converted));
          }
        }
      }
    } else if (query instanceof UnionAllQuery) {
      UnionAllQuery<?> union = (UnionAllQuery) query;
      if (union.getQuery() != null) {
        Query source = union.getQuery();
        Query converted = iterate(source, function);
        if (!source.equals(converted)) {
          query = converted;
        }
      } else {
        List<Query> queries = Lists.newArrayList();
        for (Query source : union.getQueries()) {
          queries.add(iterate(source, function));
        }
        query = union.withQueries(queries);
      }
    }
    return function.apply(query);
  }

  private static final String DUMMY_VC = "$VC";

  public static ColumnHistogram getColumnHistogramOfFirstDimension(
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker,
      ObjectMapper jsonMapper,
      GroupByQuery query,
      int numSplits
  )
  {
    if (query.getDimensions().isEmpty() || !Granularities.ALL.equals(query.getGranularity())) {
      return null;
    }
    List<VirtualColumn> virtualColumns = Lists.newArrayList(query.getVirtualColumns());
    List<OrderingSpec> orderingSpecs = Lists.newArrayList();

    DimensionSpec dimensionSpec = query.getDimensions().get(0);
    ValueDesc type = dimensionSpec.resolveType(resolver.get());
    if (type.isDimension()) {
      type = ValueDesc.STRING;
    }
    if (!type.isPrimitive()) {
      return null;  // todo
    }
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

    Map<String, Object> ag = ImmutableMap.<String, Object>builder()
                                         .put("type", "sketch")
                                         .put("name", "SKETCH")
                                         .put("fieldName", fieldName)
                                         .put("sourceType", type)
                                         .put("orderingSpecs", orderingSpecs)
                                         .put("sketchOp", "QUANTILE")
                                         .put("sketchParam", 128)
                                         .build();

    Map<String, Object> pg = ImmutableMap.<String, Object>builder()
                                         .put("type", "sketch.quantiles")
                                         .put("name", "SPLIT")
                                         .put("fieldName", "SKETCH")
                                         .put("op", "QUANTILES_CDF")
                                         .put("slopedSpaced", numSplits + 1)
                                         .put("ratioAsCount", true)
                                         .build();

    Map<String, Object> context = Maps.<String, Object>newHashMap(query.getContext());
    context.put(Query.LOCAL_POST_PROCESSING, true);

    AggregatorFactory aggregator = Queries.convert(ag, jsonMapper, AggregatorFactory.class);
    PostAggregator postAggregator = Queries.convert(pg, jsonMapper, PostAggregator.class);

    TimeseriesQuery metaQuery = new TimeseriesQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.isDescending(),
        query.getDimFilter(),
        query.getGranularity(),
        virtualColumns,
        Arrays.asList(aggregator),
        Arrays.asList(postAggregator),
        null,
        null,
        Arrays.asList("SPLIT"),
        null,
        context
    );
    Result<TimeseriesResultValue> result = Iterables.getOnlyElement(
        Sequences.toList(
            metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
            Lists.<Result<TimeseriesResultValue>>newArrayList()
        ), null
    );
    if (result == null) {
      return null;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> thresholds = (Map<String, Object>) result.getValue().getMetric("SPLIT");
    Object[] values = (Object[]) thresholds.get("splits");
    long[] counts = (long[]) thresholds.get("cdf");
    if (values == null || counts == null) {
      return null;
    }
    return new ColumnHistogram(values, counts);
  }
}
