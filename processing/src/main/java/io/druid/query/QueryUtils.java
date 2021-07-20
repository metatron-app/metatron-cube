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
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.Intervals;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.BaseFilteredDimensionSpec;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class QueryUtils
{
  private static final Logger log = new Logger(QueryUtils.class);

  private static <T> List<String> getColumns(List<Sequence<T>> sequences)
  {
    return Iterables.getFirst(
        Iterables.filter(Iterables.transform(sequences, Sequence::columns), Predicates.notNull()), null
    );
  }

  public static <T> Sequence<T> mergeSort(Query<T> query, List<Sequence<T>> sequences)
  {
    List<String> columns = getColumns(sequences);
    if (sequences.isEmpty()) {
      return Sequences.empty(columns);
    }
    if (sequences.size() == 1) {
      return sequences.get(0);
    }
    Comparator<T> ordering = query.getMergeOrdering(columns);
    if (ordering == null) {
      return Sequences.concat(columns, sequences);
    }
    return Sequences.mergeSort(columns, ordering, Sequences.simple(sequences));
  }

  public static <T> Sequence<T> mergeSort(List<String> columns, Comparator<T> ordering, Sequence<Sequence<T>> sequences)
  {
    if (ordering == null) {
      return Sequences.concat(columns, sequences);
    }
    return mergeSort(columns, ordering, Sequences.toList(sequences));
  }

  public static <T> Sequence<T> mergeSort(List<String> columns, Comparator<T> ordering, List<Sequence<T>> sequences)
  {
    if (sequences.isEmpty()) {
      return Sequences.empty(columns);
    }
    if (sequences.size() == 1) {
      return sequences.get(0);
    }
    if (ordering == null) {
      return Sequences.concat(columns, sequences);
    }
    return Sequences.mergeSort(columns, ordering, Sequences.simple(sequences));
  }

  public static <T> Sequence<T> mergeSort(Query<T> query, Sequence<Sequence<T>> sequences)
  {
    Comparator<T> ordering = query.getMergeOrdering(null);
    if (ordering == null) {
      return Sequences.concat(sequences);
    } else {
      return Sequences.mergeSort(null, ordering, sequences);
    }
  }

  public static List<Interval> analyzeInterval(QuerySegmentWalker segmentWalker, Query<?> query)
  {
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        null,
        ColumnIncluderator.NONE,
        null,
        false,
        EnumSet.of(AnalysisType.INTERVAL),
        false,
        false,
        BaseQuery.copyContextForMeta(query)
    );
    return Sequences.only(QueryRunners.run(metaQuery, segmentWalker)).getIntervals();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Map<ValueDesc, MutableInt>> analyzeTypes(QuerySegmentWalker segmentWalker, Query query)
  {
    // this is for queries no need of resolving something like summary or covariance query
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    SchemaQuery metaQuery = SchemaQuery.of(dataSource, query)
                                       .withOverriddenContext(ImmutableMap.<String, Object>of(Query.BY_SEGMENT, true));

    final Sequence sequence = QueryRunners.run(metaQuery, segmentWalker);
    final Map<String, Map<ValueDesc, MutableInt>> results = Maps.newHashMap();
    sequence.accumulate(
        null, new Accumulator<Object, Result<BySegmentResultValue<Schema>>>()
        {
          @Override
          public Object accumulate(Object accumulated, Result<BySegmentResultValue<Schema>> in)
          {
            BySegmentResultValue<Schema> bySegment = in.getValue();
            for (RowSignature schema : bySegment.getResults()) {
              for (Pair<String, ValueDesc> pair : schema.columnAndTypes()) {
                Map<ValueDesc, MutableInt> counters = results.get(pair.lhs);
                if (counters == null) {
                  results.put(pair.lhs, counters = Maps.newHashMap());
                }
                ValueDesc type = pair.rhs;
                MutableInt counter = counters.get(type);
                if (counter == null) {
                  counters.put(type, counter = new MutableInt());
                }
                counter.increment();
              }
            }
            return accumulated;
          }
        }
    );
    return results;
  }

  public static Map<String, ValueDesc> toMajorType(Map<String, Map<ValueDesc, MutableInt>> types)
  {
    Map<String, ValueDesc> majorTypes = Maps.newHashMap();
    for (Map.Entry<String, Map<ValueDesc, MutableInt>> entry : types.entrySet()) {
      String column = entry.getKey();
      Map<ValueDesc, MutableInt> value = entry.getValue();
      if (value.isEmpty()) {
        continue;
      }
      if (value.size() == 1) {
        majorTypes.put(column, Iterables.getOnlyElement(value.keySet()));
        continue;
      }
      int max = -1;
      ValueDesc major = null;
      for (Map.Entry<ValueDesc, MutableInt> x : value.entrySet()) {
        int count = x.getValue().intValue();
        if (max < 0 || max <= count) {
          major = x.getKey();
          max = count;
        }
      }
      majorTypes.put(column, major);
    }
    return majorTypes;
  }

  public static Iterable<DimFilter> toFilters(final String column, final List<String> partitions)
  {
    return new Iterable<DimFilter>()
    {
      @Override
      public Iterator<DimFilter> iterator()
      {
        return new Iterator<DimFilter>()
        {
          private int index = 1;

          @Override
          public boolean hasNext()
          {
            return index < partitions.size();
          }

          @Override
          public DimFilter next()
          {
            DimFilter filter;
            if (index == 1) {
              filter = BoundDimFilter.lt(column, partitions.get(index));
            } else if (index == partitions.size() - 1) {
              filter = BoundDimFilter.gte(column, partitions.get(index - 1));
            } else {
              filter = BoundDimFilter.between(column, partitions.get(index - 1), partitions.get(index));
            }
            index++;
            return filter;
          }
        };
      }
    };
  }

  public static List<String> getAllDataSources(final Query query)
  {
    final Set<String> dataSources = Sets.newHashSet();
    Queries.iterate(
        query, new QueryVisitor()
        {
          @Override
          public Query out(Query input)
          {
            if (input.getDataSource() instanceof TableDataSource) {
              dataSources.add(((TableDataSource) input.getDataSource()).getName());
            } else if (input.getDataSource() instanceof ViewDataSource) {
              dataSources.add(((ViewDataSource) input.getDataSource()).getName());
            }
            return input;
          }
        }
    );
    String[] array = dataSources.toArray(new String[0]);
    Arrays.sort(array);
    return Arrays.asList(array);
  }

  public static Query readPostProcessors(final Query query, final ObjectMapper mapper)
  {
    return Queries.iterate(
        query, new QueryVisitor()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Query out(Query input)
          {
            Map<String, Object> context = input.getContext();
            if (context == null) {
              return input;
            }
            Object postProcessing = context.get(Query.POST_PROCESSING);
            Object localProcessing = context.get(Query.LOCAL_POST_PROCESSING);
            if ((postProcessing == null || postProcessing instanceof PostProcessingOperator) &&
                (localProcessing == null || localProcessing instanceof PostProcessingOperator)) {
              return input;
            }
            Map<String, Object> override = Maps.newHashMap();
            if (postProcessing != null && !(postProcessing instanceof PostProcessingOperator)) {
              override.put(Query.POST_PROCESSING, PostProcessingOperators.convert(mapper, postProcessing));
            }
            if (localProcessing != null && !(localProcessing instanceof PostProcessingOperator)) {
              override.put(Query.LOCAL_POST_PROCESSING, PostProcessingOperators.convert(mapper, localProcessing));
            }
            return input.withOverriddenContext(override);
          }
        }
    );
  }

  public static Query setQueryId(final Query query, final String queryId)
  {
    final Map<String, Object> override = ImmutableMap.of(Query.QUERYID, queryId);
    return Queries.iterate(
        query, new QueryVisitor()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Query out(Query input)
          {
            Map<String, Object> context = input.getContext();
            if (context == null || context.get(Query.QUERYID) == null) {
              return input.withOverriddenContext(override);
            }
            return input;
          }
        }
    );
  }

  public static Query forLog(final Query query)
  {
    return Queries.iterate(
        query, new QueryVisitor()
        {
          @Override
          public Query out(Query input)
          {
            if (input instanceof Query.LogProvider) {
              input = ((Query.LogProvider) input).forLog();
            }
            if (input instanceof Query.FilterSupport) {
              input = DimFilters.rewrite(input, DimFilters.LOG_PROVIDER);
            }
            Object proc = input.getContextValue(Query.POST_PROCESSING, null);
            if (proc instanceof PostProcessingOperator.LogProvider) {
              PostProcessingOperator forLog = ((PostProcessingOperator.LogProvider) proc).forLog();
              if (proc != forLog) {
                input = input.withOverriddenContext(ImmutableMap.of(Query.POST_PROCESSING, forLog));
              }
            }
            Object local = input.getContextValue(Query.LOCAL_POST_PROCESSING, null);
            if (local instanceof PostProcessingOperator.LogProvider) {
              PostProcessingOperator forLog = ((PostProcessingOperator.LogProvider) local).forLog();
              if (local != forLog) {
                input = input.withOverriddenContext(ImmutableMap.of(Query.LOCAL_POST_PROCESSING, forLog));
              }
            }
            return input;
          }
        }
    );
  }

  public static Query rewriteRecursively(Query query, QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    // todo: there would be better way than this
    return Queries.iterate(query, new QueryVisitor()
    {
      private boolean outermost;

      @Override
      public Query in(Query input)
      {
        if (!outermost && input instanceof JoinQuery) {
          outermost = true;
          return input.withOverriddenContext(Query.OUTERMOST_JOIN, true);
        }
        return input;
      }

      @Override
      public Query out(Query input)
      {
        return rewrite(input, segmentWalker, queryConfig);
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> compress(Query query)
  {
    return Queries.iterate(query, input -> DimFilters.rewrite(input, DimFilters.compressor(input)));
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> decompress(Query query)
  {
    return Queries.iterate(query, input -> DimFilters.rewrite(input, DimFilters.decompressor(input)));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Query> T rewrite(
      final Query query,
      final QuerySegmentWalker segmentWalker,
      final QueryConfig queryConfig
  )
  {
    Query input = query;
    if (input instanceof Query.RewritingQuery) {
      input = ((Query.RewritingQuery) input).rewriteQuery(segmentWalker, queryConfig);
    }
    if (input instanceof Query.FilterSupport) {
      input = DimFilters.inflate((Query.FilterSupport) input);
      input = DimFilters.rewrite(input, DimFilters.rewriter(segmentWalker, input));
    }
    if (input.getDataSource() instanceof ViewDataSource) {
      ViewDataSource view = (ViewDataSource) input.getDataSource();
      ViewDataSource rewrite = DimFilters.inflate(view);
      if (rewrite != input.getDataSource()) {
        input = input.withDataSource(rewrite);
      }
      rewrite = DimFilters.rewrite(view, DimFilters.rewriter(segmentWalker, input));
      if (rewrite != input.getDataSource()) {
        input = input.withDataSource(rewrite);
      }
    }
    return (T) input;
  }

  // called by Broker or QueryMaker (resolves leaf queries)
  @SuppressWarnings("unchecked")
  public static Query resolveRecursively(Query query, QuerySegmentWalker walker)
  {
    return Queries.iterate(query, input -> resolve(input, walker));
  }

  // called by QueryRunners.getSubQueryResolver
  public static <T> Query<T> resolve(Query<T> query, QuerySegmentWalker walker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getSchema() == null) {
      // cannot be resolved before sub-query is resolved
      return query;
    }
    if (dataSource instanceof UnionDataSource) {
      return query; // todo
    }

    ViewDataSource view = ViewDataSource.DUMMY;
    if (dataSource instanceof ViewDataSource) {
      view = (ViewDataSource) dataSource;
      query = query.withDataSource(TableDataSource.of(view.getName()));
    }
    if (query instanceof Query.VCSupport && !GuavaUtils.isNullOrEmpty(view.getVirtualColumns())) {
      Query.VCSupport<T> vcSupport = (Query.VCSupport<T>) query;
      query = vcSupport.withVirtualColumns(
          VirtualColumns.override(view.getVirtualColumns(), vcSupport.getVirtualColumns())
      );
    }
    if (query instanceof Query.FilterSupport && view.getFilter() != null) {
      Query.FilterSupport<T> filterSupport = (Query.FilterSupport<T>) query;
      query = filterSupport.withFilter(DimFilters.and(view.getFilter(), filterSupport.getFilter()));
    }

    Supplier<RowResolver> resolver = QueryUtils.asResolverSupplier(query, walker);
    if (dataSource instanceof QueryDataSource) {
      resolver = Suppliers.ofInstance(
          RowResolver.of(((QueryDataSource) dataSource).getSchema(), BaseQuery.getVirtualColumns(query))
      );
    }
    query = ColumnExpander.expand(query, resolver);

    List<String> viewColumns = view.getColumns();
    if (!viewColumns.isEmpty()) {
      query = retainViewColumns(query, viewColumns);
    }
    return query.resolveQuery(resolver, false);   // already expanded
  }

  private static <T> Query<T> retainViewColumns(Query<T> query, List<String> view)
  {
    if (query instanceof Query.ColumnsSupport) {
      Query.ColumnsSupport<T> columnsSupport = (Query.ColumnsSupport<T>) query;
      List<String> columns = columnsSupport.getColumns();
      if (columns.isEmpty()) {
        query = columnsSupport.withColumns(view);
      } else {
        List<String> retained = GuavaUtils.retain(columns, view);
        if (retained.size() != columns.size()) {
          query = columnsSupport.withColumns(retained);
        }
      }
    }
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimSupport = (Query.DimensionSupport<T>) query;
      List<DimensionSpec> dimensions = dimSupport.getDimensions();
      if (dimensions.isEmpty()) {
        query = dimSupport.withDimensionSpecs(DefaultDimensionSpec.toSpec(view));
      } else {
        List<DimensionSpec> retained = DimensionSpecs.retain(dimensions, view);
        if (dimensions.size() != retained.size()) {
          query = dimSupport.withDimensionSpecs(retained);
        }
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport<T>) query;
      List<String> metrics = metricSupport.getMetrics();
      if (metrics.isEmpty()) {
        query = metricSupport.withMetrics(view);
      } else {
        List<String> retained = GuavaUtils.retain(metrics, view);
        if (metrics.size() != retained.size()) {
          query = metricSupport.withMetrics(retained);
        }
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport<T>) query;
      List<AggregatorFactory> aggregators = aggrSupport.getAggregatorSpecs();
      List<AggregatorFactory> retained = AggregatorFactory.retain(aggregators, view);
      if (aggregators.size() != retained.size()) {
        query = aggrSupport.withAggregatorSpecs(retained);
      }
    }
    return query;
  }

  // some queries uses expression vc as alias.. which disables effective filtering
  // also default dimension spec
  public static Map<String, String> aliasMapping(Query<?> query)
  {
    Map<String, String> mapping = Maps.newHashMap();
    for (VirtualColumn vc : BaseQuery.getVirtualColumns(query)) {
      if (vc instanceof ExprVirtualColumn) {
        Expr expr = Parser.parse(((ExprVirtualColumn) vc).getExpression());
        if (Evals.isIdentifier(expr) && !Evals.getIdentifier(expr).equals(vc.getOutputName())) {
          mapping.put(vc.getOutputName(), Evals.getIdentifier(expr));
        }
      }
    }
    for (DimensionSpec dimension : BaseQuery.getDimensions(query)) {
      if (dimension instanceof BaseFilteredDimensionSpec) {
        dimension = ((BaseFilteredDimensionSpec) dimension).getDelegate();
      }
      if (dimension instanceof DefaultDimensionSpec) {
        if (!dimension.getOutputName().equals(dimension.getDimension())) {
          mapping.put(dimension.getOutputName(), dimension.getDimension());
        }
      }
    }
    return mapping;
  }

  private static Supplier<RowResolver> asResolverSupplier(final Query query, final QuerySegmentWalker segmentWalker)
  {
    return Suppliers.memoize(() -> {
      return RowResolver.of(retrieveSchema(query, segmentWalker), BaseQuery.getVirtualColumns(query));
    });
  }

  public static RowSignature retrieveSchema(Query<?> query, QuerySegmentWalker segmentWalker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      return Preconditions.checkNotNull(
          schema, "schema of subquery %s is null", ((QueryDataSource) dataSource).getQuery()
      );
    }
    if (dataSource instanceof ViewDataSource) {
      dataSource = TableDataSource.of(((ViewDataSource) dataSource).getName());
    }
    SchemaQuery schemaQuery = SchemaQuery.of(Iterables.getOnlyElement(dataSource.getNames()), query)
                                         .withOverriddenContext(BaseQuery.copyContextForMeta(query));

    return Sequences.only(QueryRunners.run(schemaQuery, segmentWalker), Schema.EMPTY);
  }

  // nasty..
  public static boolean coveredBy(Query<?> query1, Query<?> query2)
  {
    final Granularity granularity = query1.getGranularity();

    List<Interval> intervals1 = query1.getIntervals();
    if (granularity != null && granularity != Granularities.ALL) {
      intervals1 = Lists.newArrayList(
          Iterables.transform(
              intervals1, new Function<Interval, Interval>()
              {
                @Override
                public Interval apply(Interval input)
                {
                  return Intervals.of(
                      granularity.bucketStart(input.getStart()),
                      granularity.bucketStart(input.getEnd())
                  );
                }
              }
          )
      );
    }
    List<Interval> intervals2 = query2.getIntervals();
    if (intervals1.size() == 1 && intervals2.size() == 1) {
      return intervals2.get(0).contains(intervals1.get(0));
    }
    // todo
    return intervals2.equals(intervals1);
  }
}
