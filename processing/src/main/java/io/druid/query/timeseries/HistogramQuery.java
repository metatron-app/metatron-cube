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

package io.druid.query.timeseries;

import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class HistogramQuery extends BaseQuery<Row> implements Query.FilterSupport<Row>
{
  private final DimensionSpec dimensionSpec;
  private final Comparator comparator;
  private final DimFilter filter;
  private final List<VirtualColumn> virtualColumns;
  private final PostAggregator postAggregator;

  public HistogramQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      DimFilter filter,
      List<VirtualColumn> virtualColumns,
      DimensionSpec dimensionSpec,
      Comparator comparator,
      PostAggregator postAggregator,
      Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        descending,
        context
    );
    this.filter = filter;
    this.virtualColumns = virtualColumns;
    this.dimensionSpec = dimensionSpec;
    this.comparator = comparator;
    this.postAggregator = postAggregator;
  }

  @Override
  public String getType()
  {
    return "histogram";
  }

  @Override
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public DimFilter getFilter()
  {
    return filter;
  }

  public DimensionSpec getDimensionSpec()
  {
    return dimensionSpec;
  }

  public Comparator getComparator()
  {
    return comparator;
  }

  public PostAggregator getPostAggregator()
  {
    return postAggregator;
  }

  @Override
  public HistogramQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new HistogramQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        virtualColumns,
        dimensionSpec,
        comparator,
        postAggregator,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public HistogramQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new HistogramQuery(
        getDataSource(),
        spec,
        isDescending(),
        filter,
        virtualColumns,
        dimensionSpec,
        comparator,
        postAggregator,
        getContext()
    );
  }

  @Override
  public HistogramQuery withDataSource(DataSource dataSource)
  {
    return new HistogramQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        virtualColumns,
        dimensionSpec,
        comparator,
        postAggregator,
        getContext()
    );
  }

  @Override
  public FilterSupport<Row> withFilter(DimFilter filter)
  {
    return new HistogramQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        virtualColumns,
        dimensionSpec,
        comparator,
        postAggregator,
        getContext()
    );
  }

  @Override
  public FilterSupport<Row> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new HistogramQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        filter,
        virtualColumns,
        dimensionSpec,
        comparator,
        postAggregator,
        getContext()
    );
  }
}
