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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.common.Cacheable;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.JoinQuery.JoinHolder;
import io.druid.query.Query.FilterSupport;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.SemiJoinFactory;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.filter.Filters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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

  public static DataSource applyFilter(DataSource dataSource, DimFilter filter, Estimation estimation, QuerySegmentWalker segmentWalker)
  {
    if (dataSource instanceof ViewDataSource) {
      final ViewDataSource view = (ViewDataSource) dataSource;
      return view.withFilter(DimFilters.and(view.getFilter(), filter));
    }
    if (dataSource instanceof QueryDataSource) {
      final Query query = ((QueryDataSource) dataSource).getQuery();
      final RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      final Query applied = applyFilter(query, filter, estimation.selectivity, segmentWalker);
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
      JoinElement element = holder.getElement();
      List<Query<Object[]>> queries = holder.getQueries();
      for (int i = 0; i < queries.size(); i++) {
        JoinType type = element.getJoinType();
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
      JoinElement element = holder.getElement();
      List<Query<Object[]>> queries = holder.getQueries();
      for (int i = 0; i < queries.size(); i++) {
        JoinType type = element.getJoinType();
        if (i == 0 && !type.isLeftDrivable() || i > 0 && !type.isRightDrivable()) {
          continue;
        }
        Query applied = applyFilter(queries.get(i), filter, selectivity, dependents, segmentWalker);
        if (applied != null) {
          List<Query> rewritten = Lists.newArrayList(holder.getQueries());
          rewritten.set(i, applied);
          DataSources.filterMerged(element, rewritten, i, i == 0 ? 1 : i - 1, segmentWalker);
          return rewritten.size() == 1 ? rewritten.get(0) : holder.withQueries(rewritten);
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public static Query<Object[]> disableSort(Query<Object[]> query, List<String> joinKey)
  {
    if (query instanceof StreamQuery) {
      StreamQuery stream = (StreamQuery) query;
      if (OrderByColumnSpec.ascending(joinKey).equals(stream.getOrderingSpecs())) {
        query = stream.withOrderingSpec(null);
      }
    } else if (query instanceof Query.OrderingSupport) {
      Query.OrderingSupport ordering = (Query.OrderingSupport) query;
      if (OrderByColumnSpec.ascending(joinKey).equals(ordering.getResultOrdering())) {
        query = ordering.withResultOrdering(null);    // todo: seemed not working
      }
    }
    return query.withOverriddenContext(JoinQuery.SORTING, null);
  }

  public static final Supplier<List<List<OrderByColumnSpec>>> NO_COLLATION = Suppliers.ofInstance(ImmutableList.of());

  public static Supplier<List<List<OrderByColumnSpec>>> getCollations(Query<?> query)
  {
    if (query instanceof JoinQuery.JoinHolder) {
      return () -> ((JoinQuery.JoinHolder) query).getCollations();
    }
    List<OrderByColumnSpec> ordering;
    if (query instanceof StreamQuery) {
      ordering = ((StreamQuery) query).getOrderingSpecs();
    } else if (query instanceof Query.OrderingSupport) {
      ordering = ((Query.OrderingSupport<?>) query).getResultOrdering();
    } else {
      ordering = null;
    }
    return ordering == null ? NO_COLLATION : () -> Arrays.asList(ordering);
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

  private static final Logger LOG = new Logger(JoinQuery.class);

  @SuppressWarnings("unchecked")
  static List<Query> filterMerged(JoinElement element, List<Query> queries, int ix, int iy, QuerySegmentWalker walker)
  {
    // todo convert back to JoinQuery and rewrite
    if (!element.isInnerJoin()) {
      return queries;   // todo
    }
    Query<Object[]> query0 = queries.get(ix);
    Query<Object[]> query1 = queries.get(iy);
    if (JoinQuery.isHashing(query0)) {
      return queries;
    }

    int rc0 = Estimation.getRowCount(query0);
    int rc1 = Estimation.getRowCount(query1);
    if (rc0 < 0 || rc1 < 0 || rc0 >= rc1) {
      return queries;
    }

    QueryConfig config = walker.getConfig();
    if (rc0 > config.getHashJoinThreshold(query0)) {
      return queries;
    }

    if (JoinQuery.isHashing(query1)) {
      LOG.debug("--- reassigning 'hashing' to %s:%d (was %s:%d)", query0.alias(), rc0, query1.alias(), rc1);
    } else {
      LOG.debug("--- assigning 'hashing' to %s:%d.. overriding sort-merge join", query0.alias(), rc0);
    }
    List<String> joinColumn0 = ix < iy ? element.getLeftJoinColumns() : element.getRightJoinColumns();
    List<String> joinColumn1 = ix < iy ? element.getRightJoinColumns() : element.getLeftJoinColumns();

    query0 = DataSources.disableSort(query0, joinColumn0);
    query1 = DataSources.disableSort(query1, joinColumn1);

    int semiJoinThrehold = config.getSemiJoinThreshold(query0);
    if (semiJoinThrehold > 0 && rc0 <= semiJoinThrehold &&
        DataSources.isDataNodeSourced(query0) && DataSources.isFilterableOn(query1, joinColumn1)) {
      DimFilter filter = removeTrivials(BaseQuery.getDimFilter(query0), joinColumn0);
      List<String> outputColumns = query0.estimatedOutputColumns();
      if (filter != null && outputColumns != null) {
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, walker);
        List<Object[]> values = Sequences.toList(sequence);
        query0 = MaterializedQuery.of(query0, sequence.columns(), values);
        Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, joinColumn0));
        query1 = DataSources.applyFilter(query1, SemiJoinFactory.from(joinColumn1, keys.iterator()), -1, walker);
        LOG.debug("--- %s:%d (hash) is applied as filter to %s", query0.alias(), values.size(), query1.alias());
      }
    }
    queries.set(ix, query0.withOverriddenContext(JoinQuery.HASHING, true));
    queries.set(iy, query1.withOverriddenContext(JoinQuery.HASHING, null));
    return queries;
  }

  static BloomDimFilter.Factory bloom(
      Query source, List<String> sourceJoinOn, Estimation sourceEstimation,
      Query target, List<String> targetJoinOn
  )
  {
    DimFilter filter = removeTrivials(removeFactory(BaseQuery.getDimFilter(source)), sourceJoinOn);
    if (filter == null || !DataSources.isDataNodeSourced(source)) {
      return null;
    }
    if (source.getContextValue(Query.LOCAL_POST_PROCESSING) != null) {
      Query<?> view = source.withOverriddenContext(Query.LOCAL_POST_PROCESSING, null);
      List<String> outputColumns = view.estimatedOutputColumns();
      if (outputColumns == null || !outputColumns.containsAll(sourceJoinOn)) {
        return null;
      }
    }
    List<DimensionSpec> extracted = DataSources.findFilterableOn(target, targetJoinOn, q -> !Queries.isNestedQuery(q));
    if (extracted != null) {
      // bloom factory will be evaluated in UnionQueryRunner
      ViewDataSource view = BaseQuery.asView(source, filter, sourceJoinOn);
      return BloomDimFilter.Factory.fields(extracted, view, Ints.checkedCast(sourceEstimation.estimated));
    }
    return null;
  }

  private static DimFilter removeFactory(DimFilter filter)
  {
    return filter == null ? null : DimFilters.rewrite(filter, f -> f instanceof DimFilter.FilterFactory ? null : f);
  }

  private static DimFilter removeTrivials(DimFilter filter, List<String> joinColumns)
  {
    if (filter == null) {
      return null;
    }
    if (joinColumns.size() == 1 && DimFilters.not(SelectorDimFilter.of(joinColumns.get(0), null)).equals(filter)) {
      return null;
    }
    if (filter instanceof AndDimFilter) {
      boolean changed = false;
      Set<DimFilter> filters = Sets.newLinkedHashSet(((AndDimFilter) filter).getFields());
      for (String joinColumn : joinColumns) {
        changed |= filters.remove(DimFilters.not(SelectorDimFilter.of(joinColumn, null)));
      }
      return changed ? DimFilters.and(Lists.newArrayList(filters)) : filter;
    }
    return filter;
  }
}
