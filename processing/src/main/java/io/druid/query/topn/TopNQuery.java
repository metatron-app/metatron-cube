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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class TopNQuery extends BaseQuery<Result<TopNResultValue>>
    implements Query.AggregationsSupport<Result<TopNResultValue>>
{
  private final DimensionSpec dimensionSpec;
  private final List<VirtualColumn> virtualColumns;
  private final TopNMetricSpec topNMetricSpec;
  private final int threshold;
  private final DimFilter dimFilter;
  private final Granularity granularity;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;
  private final List<String> outputColumns;

  @JsonCreator
  public TopNQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("dimension") DimensionSpec dimensionSpec,
      @JsonProperty("metric") TopNMetricSpec topNMetricSpec,
      @JsonProperty("threshold") int threshold,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("outputColumns") List<String> outputColumns,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.dimensionSpec = dimensionSpec;
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;

    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
    this.outputColumns = outputColumns;

    Preconditions.checkNotNull(dimensionSpec, "dimensionSpec can't be null");
    Preconditions.checkNotNull(topNMetricSpec, "must specify a metric");

    Preconditions.checkArgument(threshold != 0, "Threshold cannot be equal to 0.");
    topNMetricSpec.verifyPreconditions(this.aggregatorSpecs, this.postAggregatorSpecs);

    Queries.verifyAggregations(
        Arrays.asList(dimensionSpec.getOutputName()), this.aggregatorSpecs, this.postAggregatorSpecs
    );
  }

  @Override
  public String getType()
  {
    return TOPN;
  }

  @Override
  public List<DimensionSpec> getDimensions()
  {
    return Arrays.asList(dimensionSpec);
  }

  @Override
  @JsonProperty("virtualColumns")
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty("dimension")
  public DimensionSpec getDimensionSpec()
  {
    return dimensionSpec;
  }

  @JsonProperty("metric")
  public TopNMetricSpec getTopNMetricSpec()
  {
    return topNMetricSpec;
  }

  @JsonProperty("threshold")
  public int getThreshold()
  {
    return threshold;
  }

  @JsonProperty("filter")
  @JsonInclude(Include.NON_NULL)
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @Override
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Override
  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  @JsonProperty("aggregations")
  @JsonInclude(Include.NON_EMPTY)
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @Override
  @JsonProperty("postAggregations")
  @JsonInclude(Include.NON_EMPTY)
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @JsonProperty("outputColumns")
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      return outputColumns;
    }
    Set<String> outputNames = Sets.newLinkedHashSet();
    outputNames.addAll(AggregatorFactory.toNames(aggregatorSpecs));
    outputNames.addAll(PostAggregators.toNames(postAggregatorSpecs));
    return GuavaUtils.concat(dimensionSpec.getOutputName(), outputNames);
  }

  @Override
  public Sequence<Object[]> array(Sequence<Result<TopNResultValue>> sequence)
  {
    final List<String> outputNames = estimatedOutputColumns();
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(outputNames));
    return Sequences.explode(
        sequence,
        new Function<Result<TopNResultValue>, Sequence<Object[]>>()
        {
          private final String[] columns = outputNames.toArray(new String[0]);

          @Override
          public Sequence<Object[]> apply(Result<TopNResultValue> input)
          {
            final List<Object[]> list = Lists.newArrayList();
            for (Map<String, Object> event : input.getValue()) {
              final Object[] array = new Object[columns.length];
              for (int i = 0; i < columns.length; i++) {
                array[i] = event.get(columns[i]);
              }
              list.add(array);
            }
            return Sequences.simple(list);
          }
        }
    );
  }

  public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector)
  {
    if (dimensionSpec.getExtractionFn() != null) {
      selector.setHasExtractionFn(true);
    }
    topNMetricSpec.initTopNAlgorithmSelector(selector);
  }

  @Override
  public TopNQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        querySegmentSpec,
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public AggregationsSupport<Result<TopNResultValue>> withDimensionSpecs(List<DimensionSpec> dimensions)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        Iterables.getOnlyElement(dimensions),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public AggregationsSupport<Result<TopNResultValue>> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  public TopNQuery withDimensionSpec(DimensionSpec spec)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        spec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public TopNQuery withAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public TopNQuery withPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public Query<Result<TopNResultValue>> withDataSource(DataSource dataSource)
  {
    return new TopNQuery(
        dataSource,
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  public TopNQuery withThreshold(int threshold)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  @Override
  public TopNQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        dimensionSpec,
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public TopNQuery withDimFilter(DimFilter dimFilter)
  {
    return new TopNQuery(
        getDataSource(),
        virtualColumns,
        getDimensionSpec(),
        topNMetricSpec,
        threshold,
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        aggregatorSpecs,
        postAggregatorSpecs,
        outputColumns,
        getContext()
    );
  }

  public TopNQuery withOutputColumns(List<String> outputColumns)
  {
    return new TopNQuery(
        getDataSource(),
        getVirtualColumns(),
        getDimensionSpec(),
        getTopNMetricSpec(),
        getThreshold(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        outputColumns,
        getContext()
    );
  }

  @Override
  public TopNQuery removePostActions()
  {
    return new TopNQuery(
        getDataSource(),
        getVirtualColumns(),
        getDimensionSpec(),
        getTopNMetricSpec(),
        getThreshold(),
        getQuerySegmentSpec(),
        getDimensionsFilter(),
        getGranularity(),
        getAggregatorSpecs(),
        getPostAggregatorSpecs(),
        null,
        computeOverriddenContext(defaultPostActionContext())
    );
  }

  @Override
  public String toString()
  {
    return "TopNQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimensionSpec=" + dimensionSpec +
           ", virtualColumns=" + virtualColumns +
           ", topNMetricSpec=" + topNMetricSpec +
           ", threshold=" + threshold +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", dimFilter=" + dimFilter +
           ", granularity='" + granularity + '\'' +
           ", aggregatorSpecs=" + aggregatorSpecs +
           ", postAggregatorSpecs=" + postAggregatorSpecs +
           ", outputColumns=" + outputColumns +
           toString(POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT) +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    TopNQuery topNQuery = (TopNQuery) o;

    if (threshold != topNQuery.threshold) return false;
    if (aggregatorSpecs != null ? !aggregatorSpecs.equals(topNQuery.aggregatorSpecs) : topNQuery.aggregatorSpecs != null)
      return false;
    if (dimFilter != null ? !dimFilter.equals(topNQuery.dimFilter) : topNQuery.dimFilter != null) return false;
    if (dimensionSpec != null ? !dimensionSpec.equals(topNQuery.dimensionSpec) : topNQuery.dimensionSpec != null)
      return false;
    if (granularity != null ? !granularity.equals(topNQuery.granularity) : topNQuery.granularity != null) return false;
    if (postAggregatorSpecs != null ? !postAggregatorSpecs.equals(topNQuery.postAggregatorSpecs) : topNQuery.postAggregatorSpecs != null)
      return false;
    if (topNMetricSpec != null ? !topNMetricSpec.equals(topNQuery.topNMetricSpec) : topNQuery.topNMetricSpec != null)
      return false;
    if (!Objects.equals(virtualColumns, topNQuery.virtualColumns)) {
      return false;
    }
    if (outputColumns != null ? !outputColumns.equals(topNQuery.outputColumns) : topNQuery.outputColumns != null)
      return false;
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (dimensionSpec != null ? dimensionSpec.hashCode() : 0);
    result = 31 * result + (topNMetricSpec != null ? topNMetricSpec.hashCode() : 0);
    result = 31 * result + threshold;
    result = 31 * result + (dimFilter != null ? dimFilter.hashCode() : 0);
    result = 31 * result + (granularity != null ? granularity.hashCode() : 0);
    result = 31 * result + (aggregatorSpecs != null ? aggregatorSpecs.hashCode() : 0);
    result = 31 * result + (postAggregatorSpecs != null ? postAggregatorSpecs.hashCode() : 0);
    result = 31 * result + (virtualColumns != null ? virtualColumns.hashCode() : 0);
    result = 31 * result + (outputColumns != null ? outputColumns.hashCode() : 0);
    return result;
  }
}
