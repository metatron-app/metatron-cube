/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.rel;

import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class Grouping
{
  private final Granularity granularity;
  private final List<DimensionExpression> dimensions;
  private final List<DimensionSpec> dimensionSpecs;
  private final GroupingSetSpec groupingSet;
  private final List<AggregatorFactory> aggregations;
  private final List<VirtualColumn> virtualColumns;
  private final List<PostAggregator> postAggregators;
  private final HavingSpec havingFilter;
  private final RowSignature outputRowSignature;

  public Grouping(
      final Granularity granularity,
      final List<DimensionExpression> dimensions,
      final List<DimensionSpec> dimensionSpecs,
      final GroupingSetSpec groupingSet,
      final List<AggregatorFactory> aggregations,
      final List<VirtualColumn> virtualColumns,
      final List<PostAggregator> postAggregators,
      final HavingSpec havingFilter,
      final RowSignature outputRowSignature
  )
  {
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.dimensionSpecs = dimensionSpecs;
    this.groupingSet = groupingSet;
    this.aggregations = aggregations;
    this.havingFilter = havingFilter;
    this.virtualColumns = virtualColumns;
    this.postAggregators = postAggregators;
    this.outputRowSignature = outputRowSignature;
  }

  public Granularity getGranularity()
  {
    return granularity;
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  public List<DimensionExpression> getDimensions()
  {
    return dimensions;
  }

  public List<DimensionSpec> getDimensionSpecs()
  {
    return dimensionSpecs;
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregations;
  }

  public List<PostAggregator> getPostAggregators()
  {
    return postAggregators;
  }

  public GroupingSetSpec getGroupingSets()
  {
    return groupingSet;
  }

  @Nullable
  public HavingSpec getHavingFilter()
  {
    return havingFilter;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public String toString()
  {
    return "Grouping{" +
           "dimensions=" + dimensionSpecs +
           ", virtualColumns=" + virtualColumns +
           ", groupingSet=" + groupingSet +
           ", aggregations=" + aggregations +
           ", postAggregators=" + postAggregators +
           ", havingFilter=" + havingFilter +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
