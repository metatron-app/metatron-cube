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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.JoinQuery.JoinDelegate;
import io.druid.query.Query.ArrayOutputSupport;
import io.druid.query.Query.ColumnsSupport;
import io.druid.query.Query.DimensionSupport;
import io.druid.query.Query.SchemaProvider;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByMetaQuery;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.sketch.GenericSketchAggregatorFactory;
import io.druid.query.sketch.QuantileOperation;
import io.druid.query.sketch.SketchOp;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.DimensionSpecVirtualColumn;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;
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

  // best effort.. implement SchemaProvider if not enough
  public static RowSignature relaySchema(Query query, QuerySegmentWalker segmentWalker)
  {
    RowSignature schema = null;
    if (query.getDataSource() instanceof QueryDataSource) {
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
      schema = relaySchema(dataSource.getQuery(), segmentWalker).relay(query, false);
    }
    if (schema == null && query instanceof SchemaProvider) {
      schema = ((SchemaProvider) query).schema(segmentWalker);
    }
    if (schema == null && query instanceof UnionAllQuery) {
      schema = ((UnionAllQuery) query).getSchema();
    }
    if (schema == null && query instanceof JoinDelegate) {
      JoinDelegate joinQuery = (JoinDelegate) query;
      schema = joinQuery.getSchema();
      if (schema == null) {
        List<String> columnNames = Lists.newArrayList();
        List<ValueDesc> columnTypes = Lists.newArrayList();

        List queries = joinQuery.getQueries();
        List<String> aliases = joinQuery.getPrefixAliases();
        Set<String> uniqueNames = Sets.newHashSet();
        for (int i = 0; i < queries.size(); i++) {
          final RowSignature element = relaySchema((Query) queries.get(i), segmentWalker);
          final String prefix = aliases == null ? "" : aliases.get(i) + ".";
          for (Pair<String, ValueDesc> pair : element.columnAndTypes()) {
            columnNames.add(uniqueName(prefix + pair.lhs, uniqueNames));
            columnTypes.add(pair.rhs);
          }
        }
        schema = new RowSignature.Simple(columnNames, columnTypes);
      }
    }
    if (schema == null) {
      schema = QueryUtils.retrieveSchema(query, segmentWalker).relay(query, false);
    }
    LOG.debug(
        "%s resolved schema : %s%s",
        query.getDataSource().getNames(), schema.getColumnNames(), schema.getColumnTypes()
    );
    return schema;
  }

  public static List<String> relayColumns(ArrayOutputSupport<?> query, ObjectMapper mapper)
  {
    List<String> columns = query.estimatedOutputColumns();
    if (columns == null) {
      return columns;
    }
    if (query instanceof Query.LateralViewSupport) {
      LateralViewSpec lateralView = ((Query.LateralViewSupport) query).getLateralView();
      if (lateralView != null) {
        columns = lateralView.resolve(columns);
      }
    }
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query, mapper);
    if (postProcessor instanceof Schema.SchemaResolving) {
      columns = ((Schema.SchemaResolving) postProcessor).resolve(columns);
    }
    return columns;
  }

  public static RowSignature bestEffortOf(Query<?> query, boolean finalzed)
  {
    return bestEffortOf(null, query, finalzed);
  }

  // best effort without segment walker, upto before final decoration (output-columns) and post processing
  public static RowSignature bestEffortOf(RowSignature source, Query<?> query, boolean finalzed)
  {
    List<String> newColumnNames = Lists.newArrayList();
    List<ValueDesc> newColumnTypes = Lists.newArrayList();

    RowSignature.Simple resolving = new RowSignature.Simple(newColumnNames, newColumnTypes);
    TypeResolver resolver;
    if (source != null) {
      resolver = TypeResolver.override(RowResolver.of(source, BaseQuery.getVirtualColumns(query)), resolving);
    } else {
      resolver = RowResolver.of(resolving, BaseQuery.getVirtualColumns(query));
    }

    if (query instanceof Query.ColumnsSupport) {
      final List<String> columns = ((Query.ColumnsSupport<?>) query).getColumns();
      for (String column : columns) {
        newColumnTypes.add(resolver.resolve(column));
        newColumnNames.add(column);
      }
      return resolving;
    }
    for (DimensionSpec dimensionSpec : BaseQuery.getDimensions(query)) {
      newColumnTypes.add(dimensionSpec.resolve(resolver));
      newColumnNames.add(dimensionSpec.getOutputName());
    }
    for (String metric : BaseQuery.getMetrics(query)) {
      newColumnTypes.add(resolver.resolve(metric));
      newColumnNames.add(metric);
    }
    List<AggregatorFactory> aggregators = Lists.newArrayList(BaseQuery.getAggregators(query));
    List<PostAggregator> postAggregators = BaseQuery.getPostAggregators(query);
    for (int i = 0; i < aggregators.size(); i++) {
      AggregatorFactory metric = aggregators.get(i);
      AggregatorFactory resolved = metric.resolveIfNeeded(Suppliers.ofInstance(resolver));
      newColumnTypes.add(finalzed ? resolved.finalizedType() : resolved.getOutputType());
      newColumnNames.add(resolved.getName());
      if (resolved != metric) {
        aggregators.set(i, resolved);
      }
    }
    for (PostAggregator postAggregator : PostAggregators.decorate(postAggregators, aggregators)) {
      newColumnTypes.add(postAggregator.resolve(resolver));
      newColumnNames.add(postAggregator.getName());
    }
    return resolving;
  }

  public static RowSignature finalize(RowSignature source, Query query, ObjectMapper mapper)
  {
    if (query instanceof Query.LateralViewSupport) {
      LateralViewSpec lateralView = ((Query.LateralViewSupport) query).getLateralView();
      if (lateralView != null) {
        source = lateralView.resolve(query, source, mapper);
      }
    }
    if (query instanceof Query.ArrayOutputSupport) {
      List<String> outputColumns = ((ArrayOutputSupport<?>) query).estimatedOutputColumns();
      if (outputColumns != null) {
        source = source.retain(outputColumns);
      }
    }
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query, mapper);
    if (postProcessor instanceof Schema.SchemaResolving) {
      source = ((Schema.SchemaResolving) postProcessor).resolve(query, source, mapper);
    }
    return source;
  }

  public static List<String> uniqueNames(List<String> names, Set<String> uniqueNames, List<String> appendTo)
  {
    for (String name : names) {
      appendTo.add(uniqueName(name, uniqueNames));
    }
    return appendTo;
  }

  public static String uniqueName(String name, Set<String> uniqueNames)
  {
    // Ensure that name is unique from all previous field names
    String nameBase = name;
    for (int i = 0; !uniqueNames.add(name); i++) {
      name = nameBase + i;
    }
    return name;
  }

  @SuppressWarnings("unchecked")
  public static <I> Sequence<Row> convertToRow(Query<I> subQuery, Sequence<I> sequence)
  {
    if (subQuery instanceof SelectQuery) {
      return Sequences.explode((Sequence<Result<SelectResultValue>>) sequence, SELECT_TO_ROWS);
    } else if (subQuery instanceof TopNQuery) {
      return Sequences.explode((Sequence<Result<TopNResultValue>>) sequence, TOP_N_TO_ROWS);
    } else if (subQuery instanceof Query.RowOutputSupport) {
      return ((Query.RowOutputSupport) subQuery).asRow(sequence);
    } else if (subQuery instanceof UnionAllQuery) {
      return ((UnionAllQuery) subQuery).asRow(sequence);
    }
    return Sequences.map(sequence, GuavaUtils.<I, Row>caster());
  }

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

  public static Query iterate(Query query, Function<Query, Query> function)
  {
    if (query.getDataSource() instanceof QueryDataSource) {
      Query source = ((QueryDataSource) query.getDataSource()).getQuery();
      Query converted = iterate(source, function);
      if (source != converted) {
        query = query.withDataSource(QueryDataSource.of(converted));
      }
    } else if (query instanceof JoinQuery) {
      JoinQuery joinQuery = (JoinQuery) query;
      Map<String, DataSource> rewritten = Maps.newHashMap();
      for (Map.Entry<String, DataSource> entry : joinQuery.getDataSources().entrySet()) {
        if (entry.getValue() instanceof QueryDataSource) {
          Query source = ((QueryDataSource) entry.getValue()).getQuery();
          Query converted = iterate(source, function);
          if (source != converted) {
            rewritten.put(entry.getKey(), QueryDataSource.of(converted));
          }
        }
      }
      if (!rewritten.isEmpty()) {
        Map<String, DataSource> copy = Maps.newHashMap(joinQuery.getDataSources());
        copy.putAll(rewritten);
        query = joinQuery.withDataSources(copy);
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
      boolean changed = false;
      List<Query> queries = Lists.newArrayList();
      Query.WrappingQuery<?> wrapping = (Query.WrappingQuery) query;
      for (Query source : wrapping.getQueries()) {
        Query converted = iterate(source, function);
        changed |= source != converted;
        queries.add(converted);
      }
      if (changed) {
        query = wrapping.withQueries(queries);
      }
    }
    return function.apply(query);
  }

  public static List<DimensionSpec> extractInputFields(Query query, List<String> outputNames)
  {
    if (query instanceof ColumnsSupport) {
      ColumnsSupport<?> columnsSupport = (ColumnsSupport<?>) query;
      if (columnsSupport.getColumns().containsAll(outputNames)) {
        return DefaultDimensionSpec.toSpec(outputNames);
      }
    }
    if (query instanceof DimensionSupport) {
      DimensionSupport<?> dimensionSupport = (DimensionSupport) query;
      List<DimensionSpec> dimensionSpecs = dimensionSupport.getDimensions();
      int[] indices = GuavaUtils.indexOf(DimensionSpecs.toOutputNames(dimensionSpecs), outputNames, true);
      if (indices != null) {
        List<DimensionSpec> extracted = Lists.newArrayList();
        for (int index : indices) {
          extracted.add(dimensionSpecs.get(index));
        }
        return extracted;
      }
    }
    return null;
  }

  public static long estimateCardinality(
      BaseAggregationQuery query,
      QuerySegmentWalker segmentWalker,
      QueryConfig config
  )
  {
    if (query instanceof TimeseriesQuery) {
      return estimateCardinality((TimeseriesQuery) query, segmentWalker, config);
    } else if (query instanceof GroupByQuery) {
      return estimateCardinality((GroupByQuery) query, segmentWalker, config);
    } else {
      return -1;
    }
  }

  public static long estimateCardinality(
      TimeseriesQuery query,
      QuerySegmentWalker segmentWalker,
      QueryConfig config
  )
  {
    Granularity granularity = query.getGranularity();
    long estimated = 0;
    for (Interval interval : QueryUtils.analyzeInterval(segmentWalker, query)) {
      estimated += Iterables.size(granularity.getIterable(interval));
    }
    return estimated;
  }

  public static long estimateCardinality(
      GroupByQuery query,
      QuerySegmentWalker segmentWalker,
      QueryConfig config
  )
  {
    ObjectMapper objectMapper = segmentWalker.getObjectMapper();
    query = query.withOverriddenContext(BaseQuery.copyContextForMeta(query));
    Query<Row> sequence = new GroupByMetaQuery(query).rewriteQuery(segmentWalker, config);

    return QueryRunners.run(sequence, segmentWalker).accumulate(new MutableLong(), new Accumulator<MutableLong, Row>()
    {
      @Override
      public MutableLong accumulate(MutableLong accumulated, Row in)
      {
        accumulated.add(in.getLongMetric("cardinality"));
        return accumulated;
      }
    }).longValue();
  }

  // for cubing
  public static int estimateCardinality(GroupByQuery query, Segment segment)
  {
    final CardinalityAggregatorFactory cardinality = new CardinalityAggregatorFactory(
        "$cardinality", null, query.getDimensions(), query.getGroupingSets(), null, true, true
    );
    TimeseriesQuery timeseries = new TimeseriesQuery.Builder(query).setAggregatorSpecs(cardinality).build();
    TimeseriesQueryEngine engine = new TimeseriesQueryEngine();

    return engine.process(timeseries, segment, false).accumulate(new MutableInt(), new Accumulator<MutableInt, Row>()
    {
      @Override
      public MutableInt accumulate(MutableInt accumulated, Row in)
      {
        accumulated.add(in.getLongMetric("cardinality"));
        return accumulated;
      }
    }).intValue();
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

  public static Object[] getThresholds(
      List<DictionaryEncodedColumn> dictionaries,
      int numSplit,
      String strategy,
      int maxThreshold,
      Comparator comparator
  )
  {
    ItemsUnion itemsUnion = ItemsUnion.getInstance(32, comparator);
    for (DictionaryEncodedColumn dictionary : dictionaries) {
      itemsUnion.update(dictionary.getQuantile());
    }
    if (!itemsUnion.isEmpty()) {
      return (Object[]) QuantileOperation.QUANTILES.calculate(
          itemsUnion.getResult(), QuantileOperation.valueOf(strategy, numSplit + 1, maxThreshold, true)
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
      String splitType,
      int maxThreshold
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
                                         .put("maxThreshold", maxThreshold)
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

    Row result = Sequences.only(QueryRunners.run(metaQuery, segmentWalker), null);

    return result == null ? null : (Object[]) result.getRaw("SPLIT");
  }

  public static boolean isNestedQuery(Query<?> query)
  {
    return query instanceof UnionAllQuery ||
           query instanceof Query.IteratingQuery ||
           query.getDataSource() instanceof QueryDataSource;
  }
}
