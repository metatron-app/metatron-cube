/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.kmeans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.ValueType;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 */
public class KMeansQuery
    extends BaseQuery<Centroid>
    implements Query.RewritingQuery<Centroid>, Query.IteratingQuery<CentroidDesc, Centroid>, Query.VCSupport<Centroid>
{
  private static final int DEFAULT_MAX_ITERATION = 10;
  private static final double DEFAULT_DELTA_THRESHOLD = 0.01;

  private final List<VirtualColumn> virtualColumns;
  private final List<String> metrics;
  private final int numK;
  private final int maxIteration;
  private final double deltaThreshold;

  private final List<Range<Double>> ranges;
  private final List<Centroid> centroids;

  public KMeansQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("numK") int numK,
      @JsonProperty("maxIteration") Integer maxIteration,
      @JsonProperty("deltaThreshold") Double deltaThreshold,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(
        dataSource,
        querySegmentSpec,
        virtualColumns,
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        null,
        null,
        context
    );
  }

  public KMeansQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      List<VirtualColumn> virtualColumns,
      List<String> metrics,
      int numK,
      Integer maxIteration,
      Double deltaThreshold,
      List<Range<Double>> ranges,
      List<Centroid> centroids,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.metrics = Preconditions.checkNotNull(metrics, "metric cannot be null");
    this.numK = numK;
    Preconditions.checkArgument(maxIteration == null || maxIteration > 0);
    this.maxIteration = maxIteration == null ? DEFAULT_MAX_ITERATION : maxIteration;
    Preconditions.checkArgument(deltaThreshold == null || (deltaThreshold > 0 && deltaThreshold < 1));
    this.deltaThreshold = deltaThreshold == null ? DEFAULT_DELTA_THRESHOLD : deltaThreshold;
    this.ranges = ranges;
    this.centroids = centroids;
    this.virtualColumns = virtualColumns;
    Preconditions.checkArgument(numK > 0, "K should be greater than zero");
    Preconditions.checkArgument(!metrics.isEmpty(), "metric cannot be empty");
    if (centroids != null) {
      for (Centroid centroid : centroids) {
        Preconditions.checkArgument(metrics.size() == centroid.getCentroid().length);
      }
    }
  }

  @Override
  public String getType()
  {
    return "kmeans";
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public int getNumK()
  {
    return numK;
  }

  @JsonProperty
  public int getMaxIteration()
  {
    return maxIteration;
  }

  @JsonProperty
  public double getDeltaThreshold()
  {
    return deltaThreshold;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<Range<Double>> getRanges()
  {
    return ranges;
  }

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public List<Centroid> getCentroids()
  {
    return centroids;
  }

  @Override
  public Query<Centroid> withDataSource(DataSource dataSource)
  {
    return new KMeansQuery(
        dataSource,
        getQuerySegmentSpec(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getContext()
    );
  }

  @Override
  public Query<Centroid> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new KMeansQuery(
        getDataSource(),
        spec,
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getContext()
    );
  }

  @Override
  public Query<Centroid> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public VCSupport<Centroid> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getContext()
    );
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(),
        new ListColumnIncluderator(getMetrics()),
        true,
        Queries.extractContext(this, BaseQuery.QUERYID),
        EnumSet.of(SegmentMetadataQuery.AnalysisType.MINMAX),
        false,
        false
    );

    @SuppressWarnings("unchecked")
    List<SegmentAnalysis> sequence = Sequences.toList(
        metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<SegmentAnalysis>newArrayList()
    );
    if (sequence == null || sequence.isEmpty()) {
      throw new IllegalArgumentException("invalid span " + getDataSource() + ", " + getQuerySegmentSpec());
    }
    Map<String, ColumnAnalysis> columns = sequence.get(0).getColumns();

    List<Range<Double>> ranges = Lists.newArrayList();
    for (String metric : metrics) {
      ColumnAnalysis analysis = Preconditions.checkNotNull(columns.get(metric), "missing metric " + metric);
      Preconditions.checkArgument(ValueType.of(analysis.getType()).isNumeric());
      double min = ((Number) analysis.getMinValue()).doubleValue();
      double max = ((Number) analysis.getMaxValue()).doubleValue();
      ranges.add(Range.closed(min, max));
    }
    Random random = new Random();
    List<Centroid> centroids = Lists.newArrayList();
    for (int i = 0; i < numK; i++) {
      double[] centroid = new double[metrics.size()];
      for (int j = 0; j < centroid.length; j++) {
        centroid[j] = internalNextDouble(random, ranges.get(j).lowerEndpoint(), ranges.get(j).upperEndpoint());
      }
      centroids.add(new Centroid(centroid));
    }
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        ranges,
        centroids,
        getContext()
    );
  }

  // copied from java.util.Random
  private double internalNextDouble(Random random, double origin, double bound)
  {
    double r = random.nextDouble();
    if (origin < bound) {
      r = r * (bound - origin) + origin;
      if (r >= bound) // correct for rounding
      {
        r = Double.longBitsToDouble(Double.doubleToLongBits(bound) - 1);
      }
    }
    return r;
  }

  private int iteration;

  @Override
  public Pair<Sequence<Centroid>, Query<CentroidDesc>> next(Sequence<CentroidDesc> sequence, Query<CentroidDesc> prev)
  {
    if (sequence == null) {
      return Pair.<Sequence<Centroid>, Query<CentroidDesc>>of(
          Sequences.<Centroid>empty(),
          new FindNearestQuery(
              getDataSource(),
              getQuerySegmentSpec(),
              getVirtualColumns(),
              getMetrics(),
              getCentroids(),
              getContext()
          )
      );
    }
    List<Centroid> prevCentroids = ((FindNearestQuery) prev).getCentroids();
    List<CentroidDesc> descs = Sequences.toList(sequence, Lists.<CentroidDesc>newArrayList());

    boolean underThreshold = true;
    List<Centroid> newCentroids = Lists.newArrayList();
    for (int i = 0; i < descs.size(); i++) {
      Centroid newCentroid = descs.get(i).newCenter();
      underThreshold &= isUnderThreshold(prevCentroids.get(i), newCentroid);
      newCentroids.add(newCentroid);
    }
    if (underThreshold || ++iteration >= maxIteration) {
      return Pair.of(Sequences.simple(newCentroids), null);
    }
    return Pair.<Sequence<Centroid>, Query<CentroidDesc>>of(
        Sequences.<Centroid>empty(),
        new FindNearestQuery(
            getDataSource(),
            getQuerySegmentSpec(),
            getVirtualColumns(),
            getMetrics(),
            newCentroids,
            getContext()
        )
    );
  }

  private boolean isUnderThreshold(Centroid prev, Centroid current)
  {
    for (int i = 0; i < ranges.size(); i++) {
      Range<Double> range = ranges.get(i);
      double[] prevCoords = prev.getCentroid();
      double[] currCoords = current.getCentroid();
      double d = range.upperEndpoint() - range.lowerEndpoint();
      if (d == 0 && prevCoords[i] == currCoords[i]) {
        continue;
      }
      if (Math.abs(prevCoords[i] - currCoords[i]) / d > deltaThreshold) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64);
    builder.append(getType()).append('{')
           .append("dataSource='").append(getDataSource()).append('\'')
           .append(", querySegmentSpec=").append(getQuerySegmentSpec())
           .append(", metrics=").append(getMetrics())
           .append(", centroids=").append(getCentroids());

    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
