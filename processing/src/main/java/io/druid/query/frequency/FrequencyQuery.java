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
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.countmin.CountMinAggregatorFactory;
import io.druid.query.aggregation.countmin.CountMinSketch;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("frequency")
public class FrequencyQuery extends BaseQuery<Object[]>
    implements Query.DimensionSupport<Object[]>,
    Query.ArrayOutput,
    Query.RewritingQuery<Object[]>,
    Query.LogProvider<Object[]>
{
  public static final int DEFAULT_DEPTH = 4;  // 4 for 90%, 7 for 99%, 10 for 99.9%
  public static final int MAX_LIMIT = 16384;

  private final DimFilter filter;
  private final List<VirtualColumn> virtualColumns;
  private final GroupingSetSpec groupingSets;
  private final List<DimensionSpec> dimensions;

  private final int width;
  private final int depth;
  private final LimitSpec limitSpec;
  private final byte[] sketch;

  @JsonCreator
  public FrequencyQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("groupingSets") GroupingSetSpec groupingSets,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("width") int width,
      @JsonProperty("depth") int depth,
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JsonProperty("sketch") byte[] sketch,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.groupingSets = groupingSets == null || groupingSets.isEmpty() ? null : groupingSets;
    this.dimensions = dimensions;
    this.filter = filter;
    this.width = width;
    this.depth = depth;
    this.limitSpec = limitSpec;
    this.sketch = sketch;
    int limit = limitSpec.getLimit();
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(dimensions), "'dimensions' cannot be null or empty");
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
  public Comparator<Object[]> getMergeOrdering()
  {
    return null;
  }

  @Override
  public FrequencyQuery rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    int newDepth = depth > 0 ? depth :
                   getContextInt(FREQUENCY_SKETCH_DEPTH, queryConfig.getFrequency().getSketchDepth());
    if (width > 0 && sketch != null) {
      if (depth > 0) {
        return this;
      }
      return new FrequencyQuery(
          getDataSource(),
          getQuerySegmentSpec(),
          filter,
          virtualColumns,
          groupingSets,
          dimensions,
          width,
          newDepth,
          limitSpec,
          sketch,
          getContext()
      );
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
      AggregatorFactory factory = CardinalityAggregatorFactory.dimensions("$v", dimensions, GroupingSetSpec.EMPTY);
      Row result = QueryRunners.only(meta.withAggregatorSpecs(Arrays.asList(factory)), segmentWalker);
      int cardinality = Preconditions.checkNotNull(((Number) result.getRaw("$v")), "cardinality?").intValue();
      BigInteger prime = BigInteger.valueOf(cardinality).nextProbablePrime();
      newWidth = Math.min(prime.intValue(), (int)(cardinality * 1.11));
    }
    if (newSketch == null) {
      AggregatorFactory factory = new CountMinAggregatorFactory(
          "$v", null, dimensions, null, null, true, newWidth, newDepth, false
      );
      // disable cache
      meta = meta.withOverriddenContext(ImmutableMap.<String, Object>of(USE_CACHE, false, POPULATE_CACHE, false));
      Row result = QueryRunners.only(meta.withAggregatorSpecs(Arrays.asList(factory)), segmentWalker);
      newSketch = Preconditions.checkNotNull((CountMinSketch) result.getRaw("$v"), "sketch?")
                               .toCompressedBytes();
    }
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        groupingSets,
        dimensions,
        newWidth,
        newDepth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
        sketch,
        getContext()
    );
  }

  @Override
  public FrequencyQuery withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
        sketch,
        getContext()
    );
  }

  public FrequencyQuery withLimitSpec(LimitSpec limitSpec)
  {
    return new FrequencyQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        virtualColumns,
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
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
        groupingSets,
        dimensions,
        width,
        depth,
        limitSpec,
        null,
        getContext()
    );
  }

  public int[][] getGroupings()
  {
    return groupingSets == null ? new int[][]{} : groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensions));
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
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
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
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  @JsonProperty
  public byte[] getSketch()
  {
    return sketch;
  }

  private static final int MIN_CANDIDATES = 16;

  public int limitForCandidate()
  {
    final int limit = limitSpec.getLimit();
    return getContextBoolean(FINAL_MERGE, true) ? limit : Math.max(MIN_CANDIDATES, limit << 1);
  }

  @Override
  public List<String> estimatedInitialColumns()
  {
    return GuavaUtils.concat("count", DimensionSpecs.toOutputNames(dimensions));
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return GuavaUtils.concat("count", DimensionSpecs.toOutputNames(dimensions));
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

    if (!Objects.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (!Objects.equals(virtualColumns, that.virtualColumns)) {
      return false;
    }
    if (!Objects.equals(groupingSets, that.groupingSets)) {
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
    result = 31 * result + Objects.hashCode(dimensions);
    result = 31 * result + Objects.hashCode(virtualColumns);
    result = 31 * result + Objects.hashCode(groupingSets);
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
           ", groupingSets=" + groupingSets +
           ", dimensions=" + dimensions +
           ", filter=" + filter +
           ", width=" + width +
           ", depth=" + depth +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT) +
           '}';
  }
}
