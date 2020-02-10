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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.BaseFilteredDimensionSpec;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.query.select.Schema;
import io.druid.query.select.SchemaQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryUtils
{
  private static final Logger log = new Logger(QueryUtils.class);

  public static <T> Sequence<T> mergeSort(Query<T> query, List<Sequence<T>> sequences)
  {
    if (sequences.isEmpty()) {
      return Sequences.empty();
    }
    if (sequences.size() == 1) {
      return sequences.get(0);
    }
    return mergeSort(query, Sequences.simple(sequences));
  }

  public static <T> Sequence<T> mergeSort(Query<T> query, Sequence<Sequence<T>> sequences)
  {
    Ordering<T> ordering = query.getMergeOrdering();
    return ordering == null ? Sequences.concat(sequences) : Sequences.mergeSort(ordering, sequences);
  }

  public static List<Interval> analyzeInterval(QuerySegmentWalker segmentWalker, Query<?> query)
  {
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        null,
        new NoneColumnIncluderator(),
        null,
        false,
        EnumSet.of(AnalysisType.INTERVAL),
        false,
        false,
        Queries.extractContext(query, BaseQuery.QUERYID)
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
            for (Schema schema : bySegment.getResults()) {
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

  public static Query setQueryId(final Query query, final String queryId)
  {
    return Queries.iterate(
        query, new IdentityFunction<Query>()
        {
          @Override
          public Query apply(Query input)
          {
            if (input.getId() == null) {
              input = input.withId(queryId);
            }
            return input;
          }
        }
    );
  }

  public static Query forLog(final Query query)
  {
    return Queries.iterate(
        query, new IdentityFunction<Query>()
        {
          @Override
          public Query apply(Query input)
          {
            if (input instanceof Query.LogProvider) {
              input = ((Query.LogProvider) input).forLog();
            }
            if (input instanceof Query.FilterSupport) {
              input = DimFilters.rewrite(input, DimFilters.LOG_PROVIDER);
            }
            return input;
          }
        }
    );
  }

  public static Query rewriteRecursively(
      final Query query,
      final QuerySegmentWalker segmentWalker,
      final QueryConfig queryConfig
  )
  {
    return Queries.iterate(
        query, new IdentityFunction<Query>()
        {
          @Override
          public Query apply(Query input)
          {
            return rewrite(input, segmentWalker, queryConfig);
          }
        }
    );
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
      input = DimFilters.rewrite(input, DimFilters.rewriter(segmentWalker, input));
    }
    return (T) input;
  }

  public static Query resolveRecursively(final Query query, final QuerySegmentWalker segmentWalker)
  {
    return Queries.iterate(
        query, new IdentityFunction<Query>()
        {
          @Override
          public Query apply(Query input)
          {
            return resolve(input, segmentWalker);
          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> resolve(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getSchema() == null) {
      // cannot be resolved before sub-query is resolved (see FluentQueryRunnerBuilder.applySubQueryResolver)
      return query;
    }
    if (dataSource instanceof UnionDataSource) {
      return query; // cannot
    }
    ViewDataSource view = ViewDataSource.of("dummy");
    if (dataSource instanceof ViewDataSource) {
      view = (ViewDataSource) dataSource;
      query = query.withDataSource(TableDataSource.of(view.getName()));
    }
    if (query instanceof Query.VCSupport && !view.getVirtualColumns().isEmpty()) {
      Query.VCSupport<T> vcSupport = (Query.VCSupport) query;
      query = vcSupport.withVirtualColumns(
          VirtualColumns.override(view.getVirtualColumns(), vcSupport.getVirtualColumns())
      );
    }
    if (query instanceof Query.FilterSupport && view.getFilter() != null) {
      Query.FilterSupport<T> filterSupport = (Query.FilterSupport) query;
      query = filterSupport.withFilter(DimFilters.and(view.getFilter(), filterSupport.getFilter()));
    }

    Supplier<RowResolver> schema = QueryUtils.resolverSupplier(query, segmentWalker);
    query = query.resolveQuery(schema, segmentWalker.getObjectMapper());

    if (query instanceof Query.ColumnsSupport) {
      Query.ColumnsSupport<T> columnsSupport = (Query.ColumnsSupport) query;
      if (GuavaUtils.isNullOrEmpty(columnsSupport.getColumns())) {
        query = view.getColumns().isEmpty() ?
                columnsSupport.withColumns(schema.get().getColumnNames()) :
                columnsSupport.withColumns(view.getColumns());
      }
    }
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimSupport = (Query.DimensionSupport) query;
      if (dimSupport.getDimensions().isEmpty() && dimSupport.allDimensionsForEmpty()) {
        List<String> dimensions = GuavaUtils.retain(schema.get().getDimensionNamesExceptTime(), view.getColumns());
        query = dimSupport.withDimensionSpecs(DefaultDimensionSpec.toSpec(dimensions));
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport<T> metricSupport = (Query.MetricSupport) query;
      if (metricSupport.getMetrics().isEmpty() && metricSupport.allMetricsForEmpty()) {
        List<String> metrics = GuavaUtils.retain(schema.get().getMetricNames(), view.getColumns());
        query = metricSupport.withMetrics(metrics);
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<T> aggrSupport = (Query.AggregationsSupport) query;
      if (aggrSupport.getAggregatorSpecs().isEmpty() && aggrSupport.allMetricsForEmpty()) {
        Map<String, AggregatorFactory> factories = GuavaUtils.retain(schema.get().getAggregators(), view.getColumns());
        query = aggrSupport.withAggregatorSpecs(Lists.newArrayList(factories.values()));
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
        dimension = ((BaseFilteredDimensionSpec)dimension).getDelegate();
      }
      if (dimension instanceof DefaultDimensionSpec) {
        if (!dimension.getOutputName().equals(dimension.getDimension())) {
          mapping.put(dimension.getOutputName(), dimension.getDimension());
        }
      }
    }
    return mapping;
  }

  public static Supplier<RowResolver> resolverSupplier(final Query query, final QuerySegmentWalker segmentWalker)
  {
    return Suppliers.memoize(
        new Supplier<RowResolver>()
        {
          @Override
          public RowResolver get()
          {
            return toResolver(query, segmentWalker);
          }
        }
    );
  }

  public static RowResolver toResolver(Query query, QuerySegmentWalker segmentWalker)
  {
    final Schema schema = retrieveSchema(query, segmentWalker);
    return RowResolver.of(schema, BaseQuery.getVirtualColumns(query));
  }

  public static Schema retrieveSchema(Query<?> query, QuerySegmentWalker segmentWalker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      Schema schema = ((QueryDataSource) dataSource).getSchema();
      return Preconditions.checkNotNull(
          schema, "schema of subquery %s is null", ((QueryDataSource) dataSource).getQuery()
      );
    }
    if (dataSource instanceof ViewDataSource) {
      dataSource = TableDataSource.of(((ViewDataSource) dataSource).getName());
    }
    SchemaQuery schemaQuery = SchemaQuery.of(
        Iterables.getOnlyElement(dataSource.getNames()), query
    );
    return Sequences.only(
        QueryRunners.run(schemaQuery.withOverriddenContext(BaseQuery.copyContextForMeta(query)), segmentWalker),
        Schema.EMPTY
    );
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
