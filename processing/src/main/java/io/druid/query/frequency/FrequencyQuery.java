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

package io.druid.query.frequency;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.countmin.CountMinAggregatorFactory;
import io.druid.query.aggregation.countmin.CountMinSketch;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("frequency")
public class FrequencyQuery extends BaseQuery<Object[]>
    implements Query.ColumnsSupport<Object[]>, Query.ArrayOutputSupport<Object[]>, Query.RewritingQuery<Object[]>, Query.LogProvider<Object[]>
{
  private static final int DEFAULT_DEPTH = 4;
  private static final int MAX_LIMIT = 16384;

  private final List<String> columns;
  private final List<VirtualColumn> virtualColumns;
  private final DimFilter filter;
  private final byte[] sketch;
  private final int width;
  private final int depth;
  private final int limit;

  @JsonCreator
  public FrequencyQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("width") int width,
      @JsonProperty("depth") int depth,
      @JsonProperty("limit") int limit,
      @JsonProperty("sketch") byte[] sketch,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.columns = columns == null ? ImmutableList.<String>of() : columns;
    this.filter = filter;
    this.width = width;
    this.depth = depth <= 0 ? DEFAULT_DEPTH : depth;
    this.limit = limit;
    this.sketch = sketch;
    Preconditions.checkArgument(limit > 0 && limit < MAX_LIMIT, "invalid limit %d", limit);
  }

  @Override
  public String getType()
  {
    return "frequency";
  }

  @Override
  public Granularity getGranularity()
  {
    return Granularities.ALL;
  }

  @Override
  public Ordering<Object[]> getMergeOrdering()
  {
    return null;
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    if (width > 0 && sketch != null) {
      return this;
    }
    TimeseriesQuery meta = new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        false,
        getFilter(),
        Granularities.ALL,
        virtualColumns,
        null,
        null,
        null,
        null,
        Arrays.asList("$v"),
        null,
        BaseQuery.copyContextForMeta(this)
    );
    int newWidth = width;
    byte[] newSketch = sketch;
    if (newWidth <= 0) {
      meta = meta.withAggregatorSpecs(Arrays.<AggregatorFactory>asList(new CardinalityAggregatorFactory(
          "$v", null, DefaultDimensionSpec.toSpec(columns), GroupingSetSpec.EMPTY, null, true, true
      )));
      Row result = Sequences.only(meta.run(segmentWalker, Maps.<String, Object>newHashMap()));
      newWidth = Preconditions.checkNotNull(((Number) result.getRaw("$v")), "cardinality?").intValue();
    }
    if (newSketch == null) {
      meta = meta.withAggregatorSpecs(Arrays.<AggregatorFactory>asList(
          new CountMinAggregatorFactory("$v", columns, null, null, null, true, newWidth, depth, false)
      ));
      Row result = Sequences.only(meta.run(segmentWalker, Maps.<String, Object>newHashMap()));
      newSketch = Preconditions.checkNotNull((CountMinSketch) result.getRaw("$v"), "sketch?")
                               .toCompressedBytes();
    }
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        newWidth,
        depth,
        limit,
        newSketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new FrequencyQuery(
        getDataSource(),
        spec,
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withDataSource(DataSource dataSource)
  {
    return new FrequencyQuery(
        dataSource,
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public FrequencyQuery withFilter(DimFilter filter)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withColumns(List<String> columns)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        sketch,
        getContext()
    );
  }


  @Override
  public Query<Object[]> forLog()
  {
    if (sketch == null) {
      return this;
    }
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        columns,
        width,
        depth,
        limit,
        null,
        getContext()
    );
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public int getWidth()
  {
    return width;
  }

  @JsonProperty
  public int getDepth()
  {
    return depth;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  public byte[] getSketch()
  {
    return sketch;
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return GuavaUtils.concat("count", columns);
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    FrequencyQuery that = (FrequencyQuery) o;

    if (!Objects.equals(columns, that.columns)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(filter, that.filter)) {
      return false;
    }
    return width == that.width && depth == that.depth;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(columns);
    result = 31 * result + Objects.hashCode(virtualColumns);
    result = 31 * result + Objects.hashCode(filter);
    result = 31 * result + width;
    result = 31 * result + depth;
    return result;
  }

  @Override
  public String toString()
  {
    return "FrequencyQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", virtualColumns=" + virtualColumns +
           ", columns=" + columns +
           ", filter=" + filter +
           ", width=" + width +
           ", depth=" + depth +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT) +
           '}';
  }
}
