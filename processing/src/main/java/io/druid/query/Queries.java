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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
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
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.groupby.GroupByMetaQuery;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.sketch.GenericSketchAggregatorFactory;
import io.druid.query.sketch.QuantileOperation;
import io.druid.query.sketch.SketchOp;
import io.druid.query.sketch.SketchQuantilesPostAggregator;
import io.druid.query.sketch.TypedSketch;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSpecVirtualColumn;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.Column;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.joda.time.Interval;

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
    if (schema == null) {
      Query disabled = query.withOverriddenContext(Query.DISABLE_LOG, true);
      schema = QueryUtils.retrieveSchema(disabled, segmentWalker).relay(query, false);
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
        columns = lateralView.evolve(columns);
      }
    }
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query, mapper);
    if (postProcessor instanceof RowSignature.Evolving) {
      columns = ((RowSignature.Evolving) postProcessor).evolve(columns);
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
    if (query instanceof BaseAggregationQuery) {
      newColumnTypes.add(ValueDesc.LONG);
      newColumnNames.add(Row.TIME_COLUMN_NAME);
    }
    for (DimensionSpec dimensionSpec : BaseQuery.getDimensions(query)) {
      newColumnTypes.add(dimensionSpec.resolve(Suppliers.ofInstance(resolver)));
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
        source = lateralView.evolve(query, source, mapper);
      }
    }
    if (query instanceof Query.ArrayOutputSupport) {
      List<String> outputColumns = ((ArrayOutputSupport<?>) query).estimatedOutputColumns();
      if (outputColumns != null) {
        source = source.retain(outputColumns);
      }
    }
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query, mapper);
    if (postProcessor instanceof RowSignature.Evolving) {
      source = ((RowSignature.Evolving) postProcessor).evolve(query, source, mapper);
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
    if (!(query.getDataSource() instanceof TableDataSource)) {
      DataSource converted = iterate(query.getDataSource(), function);
      if (query.getDataSource() != converted) {
        query = query.withDataSource(converted);
      }
    }
    if (query instanceof JoinQuery) {
      JoinQuery joinQuery = (JoinQuery) query;
      Map<String, DataSource> rewritten = Maps.newHashMap();
      for (Map.Entry<String, DataSource> entry : joinQuery.getDataSources().entrySet()) {
        DataSource converted = iterate(entry.getValue(), function);
        if (converted != entry.getValue()) {
          rewritten.put(entry.getKey(), converted);
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

  private static DataSource iterate(DataSource dataSource, Function<Query, Query> function)
  {
    if (dataSource instanceof QueryDataSource) {
      Query source = ((QueryDataSource) dataSource).getQuery();
      Query converted = iterate(source, function);
      return source == converted ? dataSource : QueryDataSource.of(converted);
    } else if (dataSource instanceof ViewDataSource) {
      // later..
//      StreamQuery source = ((ViewDataSource) dataSource).asStreamQuery(null);
//      StreamQuery converted = (StreamQuery) function.apply(source);
//      return source == converted ? dataSource : ViewDataSource.from(converted);
    }
    return dataSource;
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

  private static long tryEstimateOnDictionary(GroupByQuery query, List<Segment> segments, long minCardinality)
  {
    long multiply = 1;
    for (DimensionSpec dimension : query.getDimensions()) {
      int cardinality = 0;
      for (Segment segment : segments) {
        QueryableIndex index = segment.asQueryableIndex(false);
        if (index == null) {
          return -1;
        }
        BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(index, TypeResolver.UNKNOWN);
        Column column = selector.getColumn(dimension.getDimension());
        if (column == null) {
          continue;
        }
        if (column.getCapabilities().isDictionaryEncoded()) {
          try (Dictionary<String> dictionary = column.getDictionary()) {
            cardinality += dictionary.size();
          }
        } else {
          cardinality += column.getNumRows();
        }
      }
      multiply *= Math.max(1, cardinality);
      if (multiply >= minCardinality) {
        return -1;
      }
    }
    return minCardinality;
  }

  public static long estimateCardinality(
      GroupByQuery query,
      List<Segment> segments,
      QuerySegmentWalker segmentWalker,
      QueryConfig config,
      long minCardinality
  )
  {
    if (query.getFilter() == null && segments.size() < QueryRunners.MAX_QUERY_PARALLELISM << 1) {
      final long estimate = tryEstimateOnDictionary(query, segments, minCardinality);
      if (estimate >= 0 && estimate < minCardinality) {
        return estimate;
      }
    }
    return estimateCardinality(query, segmentWalker, config);
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

  public static Object[] makeColumnHistogramOn(
      Supplier<RowResolver> supplier,
      List<Segment> segments,
      QuerySegmentWalker segmentWalker,
      BaseAggregationQuery metaQuery,
      DimensionSpec column,
      int numSplits,
      String splitType,
      int maxThreshold,
      Cache cache
  )
  {
    if (!Granularities.ALL.equals(metaQuery.getGranularity())) {
      return null;
    }
    OrderingSpec orderingSpec = null;
    if (column instanceof DimensionSpecWithOrdering) {
      DimensionSpecWithOrdering explicit = (DimensionSpecWithOrdering) column;
      orderingSpec = explicit.asOrderingSpec();
      column = explicit.getDelegate();
    }
    ValueDesc type = column.resolve(supplier);
    if (!type.isPrimitive() && !type.isDimension()) {
      return null;  // todo
    }

    TimeseriesQuery.Builder builder = new TimeseriesQuery.Builder(metaQuery);

    String fieldName = column.getDimension();
    if (!(column instanceof DefaultDimensionSpec)) {
      builder.append(DimensionSpecVirtualColumn.wrap(column, "$VC"));
      fieldName = "$VC";
    }
    AggregatorFactory aggregator = new GenericSketchAggregatorFactory(
        "$SKETCH", fieldName, type.isDimension() ? ValueDesc.STRING : type, SketchOp.QUANTILE, 4096, orderingSpec, false
    );
    PostAggregator postAggregator = SketchQuantilesPostAggregator.quantile(
        "$SPLIT", "$SKETCH", QuantileOperation.of(splitType, numSplits + 1, maxThreshold, true)
    );

    TimeseriesQuery query = builder.aggregators(aggregator)
                                   .postAggregators(postAggregator)
                                   .addContext(Query.FINAL_MERGE, true)
                                   .build();

    Object[] histogram = null;
    if (type.isDimension() && segments.size() < QueryRunners.MAX_QUERY_PARALLELISM && allQueryableIndex(segments)) {
      histogram = makeColumnHistogramOn(segments, column, orderingSpec, query.withQuerySegmentSpec(null), cache);
    }
    if (histogram == null && segments.size() == 1) {
      histogram = makeColumnHistogramOn(segments.get(0), query.withQuerySegmentSpec(null), cache);
    }
    if (histogram == null && segmentWalker != null) {
      Row result = Sequences.only(QueryRunners.run(query, segmentWalker), null);
      histogram = result == null ? null : (Object[]) result.getRaw("$SPLIT");
    }
    return histogram;
  }

  @SuppressWarnings("unchecked")
  private static Object[] makeColumnHistogramOn(
      List<Segment> segments,
      DimensionSpec dimensionSpec,
      OrderingSpec orderingSpec,
      TimeseriesQuery query,
      Cache cache
  )
  {
    final Comparator comparator = orderingSpec == null ? GuavaUtils.noNullableNatural() : orderingSpec.getComparator();
    ItemsUnion union = null;
    for (Segment segment : segments) {
      union = segment.asStorageAdapter(true).makeCursors(query, cache).accumulate(union, (current, cursor) -> {
        ItemsSketch<Integer> sketch = ItemsSketch.getInstance(4096, comparator);
        DimensionSelector selector = cursor.makeDimensionSelector(dimensionSpec);
        ItemsSketch.rand.setSeed(0);
        if (selector instanceof DimensionSelector.Scannable) {
          ((DimensionSelector.Scannable) selector).scan(
              IntIterators.wrap(cursor), (x, v) -> sketch.update(v.applyAsInt(x))
          );
        } else if (selector instanceof DimensionSelector.SingleValued) {
          for (; !cursor.isDone(); cursor.advance()) {
            sketch.update(selector.getRow().get(0));
          }
        } else {
          for (; !cursor.isDone(); cursor.advance()) {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            for (int i = 0; i < size; i++) {
              sketch.update(row.get(i));
            }
          }
        }
        ArrayOfItemsSerDe converter = new ArrayItemConverter(selector);
        Memory memory = Memory.wrap(sketch.toByteArray(converter));
        ItemsSketch instance = ItemsSketch.getInstance(memory, comparator, converter);
        if (current == null) {
          return ItemsUnion.getInstance(instance);
        }
        current.update(instance);
        return current;
      });
    }
    if (union != null) {
      PostAggregator postAggregator = query.getPostAggregatorSpecs().get(0);
      return (Object[]) postAggregator.processor().compute(
          DateTime.now(), GuavaUtils.mutableMap("SKETCH", TypedSketch.of(ValueDesc.STRING, union.getResult()))
      );
    }
    return null;
  }

  private static Object[] makeColumnHistogramOn(Segment segment, TimeseriesQuery query, Cache cache)
  {
    TimeseriesQueryEngine engine = new TimeseriesQueryEngine();
    Row row = Sequences.only(engine.process(query, segment, false, cache), null);
    if (row != null) {
      PostAggregator postAggregator = query.getPostAggregatorSpecs().get(0);
      return (Object[]) postAggregator.processor().compute(row.getTimestamp(), ((MapBasedRow) row).getEvent());
    }
    return null;
  }

  private static boolean allQueryableIndex(List<Segment> segments)
  {
    for (Segment segment : segments) {
      while (segment instanceof Segment.Delegated) {
        segment = ((Segment.Delegated) segment).getSegment();
      }
      if (!(segment instanceof QueryableIndexSegment)) {
        return false;
      }
    }
    return true;
  }

  private static class ArrayItemConverter extends ArrayOfItemsSerDe
  {
    private final DimensionSelector selector;

    private ArrayItemConverter(DimensionSelector selector) {this.selector = selector;}

    @Override
    public byte[] serializeToByteArray(Object[] items)
    {
      final BytesOutputStream output = new BytesOutputStream();
      output.writeInt(0);
      for (int i = 0; i < items.length; i++) {
        output.writeUnsignedVarInt((Integer) items[i]);
      }
      final byte[] bytes = output.toByteArray();
      WritableMemory.wrap(bytes).putInt(0, bytes.length - Integer.BYTES);
      return bytes;
    }

    @Override
    public Object[] deserializeFromMemory(Memory mem, int numItems)
    {
      UnsafeUtil.checkBounds(0, Integer.BYTES, mem.getCapacity());
      final byte[] bytes = new byte[mem.getInt(0)];
      UnsafeUtil.checkBounds(Integer.BYTES, bytes.length, mem.getCapacity());
      mem.getByteArray(Integer.BYTES, bytes, 0, bytes.length);

      final BytesInputStream input = new BytesInputStream(bytes);
      final Object[] dictionary = new String[numItems];
      for (int i = 0; i < dictionary.length; i++) {
        dictionary[i] = selector.lookupName(input.readUnsignedVarInt());
      }
      return dictionary;
    }
  }

  public static boolean isNestedQuery(Query<?> query)
  {
    return query instanceof UnionAllQuery ||
           query instanceof Query.IteratingQuery ||
           query.getDataSource() instanceof QueryDataSource;
  }
}
