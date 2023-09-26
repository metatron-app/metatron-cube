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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.ISE;
import io.druid.query.JoinQuery.JoinHolder;
import io.druid.query.Query.FilterSupport;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.StreamQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.filter.Filters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
public class DataSources
{
  // best effort based..
  public static boolean hasFilter(DataSource dataSource)
  {
    if (dataSource instanceof ViewDataSource && ((ViewDataSource) dataSource).getFilter() != null) {
      return true;
    }
    if (dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getQuery().hasFilters()) {
      return true;
    }
    return false;
  }

  public static DataSource from(List<String> names)
  {
    return names.size() == 1 ? TableDataSource.of(names.get(0)) : UnionDataSource.of(names);
  }

  public static String getName(Query query)
  {
    return getName(query.getDataSource());
  }

  public static String getName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  public static Query nestedQuery(DataSource dataSource)
  {
    return dataSource instanceof QueryDataSource ? ((QueryDataSource) dataSource).getQuery() : null;
  }

  public static RowSignature schema(DataSource dataSource, Query query, QuerySegmentWalker segmentWalker)
  {
    RowSignature schema = dataSource instanceof QueryDataSource ? ((QueryDataSource) dataSource).getSchema() : null;
    return schema != null ? schema : Queries.relaySchema(query, segmentWalker);
  }

  public static boolean isFilterSupport(DataSource dataSource)
  {
    return dataSource instanceof ViewDataSource || isFromQuery(dataSource, q -> q instanceof FilterSupport);
  }

  public static boolean isFromQuery(DataSource dataSource, Predicate<Query> predicate)
  {
    return dataSource instanceof QueryDataSource && predicate.apply(((QueryDataSource) dataSource).getQuery());
  }

  public static DataSource applyFilter(DataSource dataSource, DimFilter filter, double selectivity, QuerySegmentWalker segmentWalker)
  {
    if (dataSource instanceof ViewDataSource) {
      final ViewDataSource view = (ViewDataSource) dataSource;
      return view.withFilter(DimFilters.and(view.getFilter(), filter));
    }
    if (dataSource instanceof QueryDataSource) {
      final Query query = ((QueryDataSource) dataSource).getQuery();
      final RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      final Query applied = applyFilter(query, filter, selectivity, segmentWalker);
      if (applied != null) {
        return QueryDataSource.of(applied, schema);
      }
      return QueryDataSource.of(
          Druids.newSelectQueryBuilder()
                .dataSource(dataSource)
                .filters(filter)
                .streaming(),
          schema
      );
    }
    throw new ISE("Not filter support %s", dataSource);
  }

  public static DataSource applyProjection(DataSource dataSource, List<String> projection)
  {
    final List<String> sourceColumns = Preconditions.checkNotNull(DataSources.getOutputColumns(dataSource));
    if (sourceColumns.equals(projection)) {
      return dataSource;
    }
    if (dataSource instanceof QueryDataSource) {
      final Query query = ((QueryDataSource) dataSource).getQuery();
      final RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      if (query instanceof StreamQuery && ((StreamQuery) query).viewLike()) {
        // special handling
        final PostProcessingOperator processor = (PostProcessingOperator) query.getContextValue(Query.LOCAL_POST_PROCESSING);
        if (processor == null) {
          final StreamQuery stream = ((StreamQuery) query);
          if (stream.getDataSource() instanceof TableDataSource && schema == null) {
            return ViewDataSource.of(getName(query), stream.getVirtualColumns(), stream.getFilter(), projection);
          }
          return QueryDataSource.of(stream.withColumns(projection), schema == null ? null : schema.retain(projection));
        }
        PostProcessingOperator rewritten = PostProcessingOperators.rewriteLast(
            processor,
            p -> p instanceof BroadcastJoinProcessor ? ((BroadcastJoinProcessor) p).withOutputColumns(projection) : p
        );
        if (rewritten != processor) {
          return QueryDataSource.of(
              query.withOverriddenContext(Query.LOCAL_POST_PROCESSING, rewritten),
              schema == null ? null : schema.retain(projection)
          );
        }
      }
      if (query instanceof Query.LastProjectionSupport) {
        return QueryDataSource.of(
            ((Query.LastProjectionSupport<?>) query).withOutputColumns(projection),
            schema == null ? null : schema.retain(projection)
        );
      }
      // todo: implement Query.LastProjectionSupport for JoinHolders
    }
    // wrap
    return QueryDataSource.of(Druids.newSelectQueryBuilder()
                                    .dataSource(dataSource)
                                    .outputColumns(projection)
                                    .streaming()
    );
  }

  public static boolean isBroadcasting(DataSource ds)
  {
    return isBroadcasting(nestedQuery(ds));
  }

  public static boolean isBroadcasting(Query<?> query)
  {
    return query != null && query.getContextValue(Query.LOCAL_POST_PROCESSING) != null;
  }

  public static boolean isDataLocalFilterable(Query<?> query, List<String> joinColumns)
  {
    return DataSources.isDataNodeSourced(query) && DataSources.isFilterableOn(query, joinColumns);
  }

  public static boolean isDataNodeSourced(DataSource source)
  {
    if (source instanceof QueryDataSource) {
      return isDataNodeSourced(((QueryDataSource) source).getQuery());
    }
    return source instanceof TableDataSource || source instanceof ViewDataSource;
  }

  public static boolean isDataNodeSourced(Query<?> query)
  {
    if (query instanceof StreamQuery && query.getDataSource() instanceof TableDataSource) {
      StreamQuery stream = (StreamQuery) query;
      return stream.getLimitSpec().isNoop() && stream.getContextValue(Query.POST_PROCESSING) == null;
    }
    return false;
  }

  // just for ordering
  public static double roughCost(DataSource source)
  {
    if (source instanceof QueryDataSource) {
      return roughCost(((QueryDataSource) source).getQuery());
    }
    if (source instanceof ViewDataSource || source instanceof TableDataSource) {
      return 1;
    }
    if (source instanceof UnionDataSource) {
      return 0;   // just marker
    }
    return 100; // ??
  }

  private static double roughCost(Query<?> query)
  {
    if (query instanceof MaterializedQuery) {
      return 0;
    }
    double base = roughCost(query.getDataSource()) + PostProcessingOperators.roughCost(query);
    if (query instanceof TimeseriesQuery) {
      return base / 4f;
    } else if (query instanceof BaseAggregationQuery) {
      return base + 0.3 + (Math.pow(1.2, BaseQuery.getDimensions(query).size()) - 1);
    } else if (query instanceof StreamQuery) {
      return base + (((StreamQuery) query).getOrderingSpecs().isEmpty() ? 0 : 1);
    } else if (query instanceof UnionAllQuery) {
      return base + ((UnionAllQuery<?>) query).getQueries().stream().mapToDouble(q -> roughCost(q)).sum();
    }
    return 100;
  }

  public static boolean isFilterableOn(DataSource dataSource, List<String> columns)
  {
    if (dataSource instanceof QueryDataSource) {
      return isFilterableOn(((QueryDataSource) dataSource).getQuery(), columns);
    } else if (dataSource instanceof ViewDataSource) {
      List<String> invariant = ((ViewDataSource) dataSource).getColumns();
      return invariant != null && invariant.containsAll(columns);
    }
    return false;
  }

  public static boolean isFilterableOn(Query<?> query, List<String> columns)
  {
    return isFilterableOn(query, columns, Predicates.alwaysTrue());
  }

  public static boolean isFilterableOn(Query<?> query, List<String> columns, Predicate<Query> predicate)
  {
    return findFilterableOn(query, columns, predicate) != null;
  }

  public static List<DimensionSpec> findFilterableOn(Query<?> query, List<String> columns, Predicate<Query> predicate)
  {
    if (query instanceof Query.AggregationsSupport) {
      List<DimensionSpec> dimensions = BaseQuery.getDimensions(query);
      if (dimensions.isEmpty() || !DimensionSpecs.isAllDefault(dimensions)) {
        return null;
      }
      int[] indices = GuavaUtils.indexOf(DimensionSpecs.toOutputNames(dimensions), columns, true);
      if (indices != null && predicate.apply(query)) {
        if (Arrays.equals(indices, GuavaUtils.intsTo(columns.size()))) {
          return dimensions;
        }
        List<DimensionSpec> extracted = Lists.newArrayList();
        for (int index : indices) {
          extracted.add(dimensions.get(index));
        }
        return extracted;
      }
    } else if (query instanceof Query.ColumnsSupport) {
      List<String> invariant = ((Query.ColumnsSupport<?>) query).getColumns();
      if (invariant != null && invariant.containsAll(columns) && predicate.apply(query)) {
        return DefaultDimensionSpec.toSpec(columns);
      }
    } else if (query instanceof JoinHolder) {
      JoinHolder holder = (JoinHolder) query;
      List<Query<Object[]>> queries = holder.getQueries();
      for (int i = 0; i < queries.size(); i++) {
        JoinType type = holder.elementFor(i).getJoinType();
        if (i == 0 && !type.isLeftDrivable() || i > 0 && !type.isRightDrivable()) {
          continue;
        }
        List<DimensionSpec> filterable = findFilterableOn(queries.get(i), columns, predicate);
        if (filterable != null) {
          return filterable;
        }
      }
    }
    return null;
  }

  public static Query applyFilter(Query<?> query, DimFilter filter, double selectivity, QuerySegmentWalker segmentWalker)
  {
    return applyFilter(query, filter, selectivity, Filters.getDependents(filter), segmentWalker);
  }

  public static Query applyFilter(
      Query<?> query,
      DimFilter filter,
      double selectivity,
      Collection<String> dependents,
      QuerySegmentWalker segmentWalker
  )
  {
    if (query.getDataSource() instanceof QueryDataSource) {
      Query<?> nested = nestedQuery(query.getDataSource());
      Query applied = applyFilter(nested, filter, selectivity, dependents, segmentWalker);
      if (applied != null) {
        return query.withDataSource(QueryDataSource.of(applied));
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport<?> aggregations = (Query.AggregationsSupport) query;
      List<DimensionSpec> dimensions = aggregations.getDimensions();
      if (DimensionSpecs.isAllDefault(dimensions) && DimensionSpecs.toOutputNames(dimensions).containsAll(dependents)) {
        return Estimations.mergeSelectivity(DimFilters.and(aggregations, filter), selectivity);
      }
    } else if (query instanceof Query.ColumnsSupport) {
      Query.ColumnsSupport<?> columns = (Query.ColumnsSupport<?>) query;
      List<String> invariant = columns.getColumns();
      if (invariant != null && invariant.containsAll(dependents)) {
        return Estimations.mergeSelectivity(DimFilters.and(columns, filter), selectivity);
      }
    } else if (query instanceof JoinHolder) {
      JoinHolder holder = (JoinHolder) query;
      List<Query<Object[]>> queries = holder.getQueries();
      for (int i = 0; i < queries.size(); i++) {
        JoinElement element = holder.elementFor(i);
        JoinType type = element.getJoinType();
        if (i == 0 && !type.isLeftDrivable() || i > 0 && !type.isRightDrivable()) {
          continue;
        }
        Query applied = applyFilter(queries.get(i), filter, selectivity, dependents, segmentWalker);
        if (applied != null) {
          List<Query> rewritten = Lists.newArrayList(holder.getQueries());
          rewritten.set(i, applied);
          JoinQuery.filterMerged(element, rewritten, i, i == 0 ? 1 : i - 1, segmentWalker);
          return rewritten.size() == 1 ? rewritten.get(0) : holder.withQueries(rewritten);
        }
      }
    }
    return null;
  }

  public static List<String> getOutputColumns(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      if (schema != null) {
        return schema.getColumnNames();
      }
      Query<?> query = ((QueryDataSource) dataSource).getQuery();
      return query.estimatedOutputColumns();
    } else if (dataSource instanceof ViewDataSource) {
      return ((ViewDataSource) dataSource).getColumns();
    }
    return null;
  }

  public static String getMetricName(DataSource dataSource)
  {
    final List<String> names = dataSource.getNames();
    return names.size() == 1 ? names.get(0) : names.toString();
  }

  public static DataSource unwrapView(DataSource dataSource)
  {
    return dataSource instanceof ViewDataSource
           ? TableDataSource.of(((ViewDataSource) dataSource).getName()) : dataSource;
  }

  public static byte[] toCacheKey(DataSource dataSource)
  {
    if (dataSource instanceof Cacheable) {
      return ((Cacheable) dataSource).getCacheKey();
    }
    if (dataSource instanceof QueryDataSource) {
      Query<Object> query = ((QueryDataSource) dataSource).getQuery();
      if (query instanceof Cacheable) {
        return ((Cacheable) query).getCacheKey();
      }
    }
    return null;
  }
}
