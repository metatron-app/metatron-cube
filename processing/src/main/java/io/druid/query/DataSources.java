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
import com.google.common.collect.Iterables;
import io.druid.java.util.common.ISE;
import io.druid.query.Query.FilterSupport;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.StreamQuery;

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

  public static boolean isFilterSupport(DataSource dataSource)
  {
    return dataSource instanceof ViewDataSource ||
           dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getQuery() instanceof FilterSupport;
  }

  public static DataSource applyFilterAndProjection(DataSource dataSource, DimFilter filter, List<String> projection)
  {
    return applyProjection(andFilter(dataSource, filter), projection);
  }

  public static DataSource andFilter(DataSource dataSource, DimFilter filter)
  {
    if (dataSource instanceof ViewDataSource) {
      final ViewDataSource view = (ViewDataSource) dataSource;
      return view.withFilter(DimFilters.and(view.getFilter(), filter));
    }
    if (dataSource instanceof QueryDataSource) {
      final Query query = ((QueryDataSource) dataSource).getQuery();
      final RowSignature schema = ((QueryDataSource) dataSource).getSchema();
      if (query instanceof FilterSupport) {
        return QueryDataSource.of(DimFilters.and((FilterSupport<?>) query, filter), schema);
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
      if (query instanceof StreamQuery && ((StreamQuery) query).isView()) {
        // special handling
        final StreamQuery stream = ((StreamQuery) query);
        if (stream.getDataSource() instanceof TableDataSource && schema == null) {
          return ViewDataSource.of(getName(query), stream.getVirtualColumns(), stream.getFilter(), projection);
        }
        return QueryDataSource.of(stream.withColumns(projection), schema == null ? null : schema.retain(projection));
      }
      if (query instanceof Query.LastProjectionSupport) {
        return QueryDataSource.of(
            ((Query.LastProjectionSupport) query).withOutputColumns(projection),
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

  public static boolean isDataNodeSourced(DataSource source)
  {
    if (source instanceof QueryDataSource) {
      Query inner = ((QueryDataSource) source).getQuery();
      if (inner.getContextValue(Query.POST_PROCESSING) != null) {
        return false;
      }
      if (!(inner.getDataSource() instanceof TableDataSource)) {
        return false;
      }
      if (inner instanceof StreamQuery) {
        StreamQuery stream = (StreamQuery) inner;
        if (stream.getLimitSpec().isNoop() || stream.getContextValue(Query.LOCAL_POST_PROCESSING) == null) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  public static List<String> getInvariantColumns(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      return getInvariantColumns(((QueryDataSource) dataSource).getQuery());
    } else if (dataSource instanceof ViewDataSource) {
      return ((ViewDataSource) dataSource).getColumns();
    }
    return null;
  }

  public static List<String> getInvariantColumns(Query<?> query)
  {
    if (query instanceof BaseAggregationQuery) {
      return DimensionSpecs.toOutputNames(((BaseAggregationQuery) query).getDimensions());
    } else if (query instanceof Query.ColumnsSupport) {
      return ((Query.ColumnsSupport<?>) query).getColumns();
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
}
