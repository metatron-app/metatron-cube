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

package io.druid.query.kmeans;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueType;
import io.druid.query.BaseQuery;
import io.druid.query.Classifier;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.filter.DimFilter;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 */
public class KMeansQuery
    extends BaseQuery<Centroid>
    implements Query.RewritingQuery<Centroid>,
    Query.IteratingQuery<CentroidDesc, Centroid>,
    Query.FilterSupport<Centroid>,
    Query.ClassifierFactory<Centroid>,
    Query.ArrayOutputSupport<Centroid>,
    Query.LogProvider<Centroid>
{
  private static final Logger LOG = new Logger(KMeansQuery.class);

  static final int DEFAULT_MAX_ITERATION = 10;
  static final double DEFAULT_DELTA_THRESHOLD = 0.01;

  private final List<VirtualColumn> virtualColumns;
  private final DimFilter filter;
  private final List<String> metrics;
  private final int numK;
  private final int maxIteration;
  private final double deltaThreshold;

  private final List<Range<Double>> ranges;
  private final List<Centroid> centroids;
  private final String measure;

  public KMeansQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("numK") int numK,
      @JsonProperty("maxIteration") Integer maxIteration,
      @JsonProperty("deltaThreshold") Double deltaThreshold,
      @JsonProperty("measure") String measure,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(
        dataSource,
        querySegmentSpec,
        filter,
        virtualColumns,
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        null,
        null,
        measure,
        context
    );
  }

  public KMeansQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      DimFilter filter,
      List<VirtualColumn> virtualColumns,
      List<String> metrics,
      int numK,
      Integer maxIteration,
      Double deltaThreshold,
      List<Range<Double>> ranges,
      List<Centroid> centroids,
      String measure,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.filter = filter;
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
    this.measure = measure;
  }

  @Override
  public String getType()
  {
    return "kmeans";
  }

  @Override
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
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

  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  public String getMeasure()
  {
    return measure;
  }

  @Override
  public Query<Centroid> withDataSource(DataSource dataSource)
  {
    return new KMeansQuery(
        dataSource,
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getMeasure(),
        getContext()
    );
  }

  @Override
  public Query<Centroid> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new KMeansQuery(
        getDataSource(),
        spec,
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getMeasure(),
        getContext()
    );
  }

  @Override
  public Query<Centroid> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getMeasure(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public VCSupport<Centroid> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        virtualColumns,
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getMeasure(),
        getContext()
    );
  }

  @Override
  public FilterSupport<Centroid> withFilter(DimFilter filter)
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        filter,
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        getRanges(),
        getCentroids(),
        getMeasure(),
        getContext()
    );
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final Map<String, Object> context = BaseQuery.copyContextForMeta(this);
    context.put(QueryContextKeys.USE_CACHE, false);
    context.put(QueryContextKeys.POPULATE_CACHE, false);
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getVirtualColumns(),
        null,
        getMetrics(),
        true,
        context,
        EnumSet.of(SegmentMetadataQuery.AnalysisType.MINMAX),
        false,
        false
    );

    List<SegmentAnalysis> sequence = Sequences.toList(QueryRunners.run(metaQuery, segmentWalker));
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
    Random random = getContextValue("$seed") != null ? new Random(getContextInt("$seed", 1)) : new Random();
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
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        ranges,
        centroids,
        getMeasure(),
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
    Map<String, Object> context = BaseQuery.copyContextForMeta(this);
    if (sequence == null) {
      return Pair.<Sequence<Centroid>, Query<CentroidDesc>>of(
          Sequences.<Centroid>empty(),
          new FindNearestQuery(
              getDataSource(),
              getQuerySegmentSpec(),
              getFilter(),
              getVirtualColumns(),
              getMetrics(),
              getCentroids(),
              getMeasure(),
              context
          )
      );
    }
    List<Centroid> prevCentroids = ((FindNearestQuery) prev).getCentroids();
    List<CentroidDesc> descs = Sequences.toList(sequence);

    boolean underThreshold = true;
    List<Centroid> newCentroids = Lists.newArrayList();
    for (int i = 0; i < descs.size(); i++) {
      Centroid newCentroid = descs.get(i).newCenter();
      underThreshold &= isUnderThreshold(prevCentroids.get(i), newCentroid);
      newCentroids.add(newCentroid);
    }
    if (underThreshold || ++iteration >= maxIteration) {
      LOG.info("Centroid decided in %d iteration", iteration);
      return Pair.of(Sequences.simple(newCentroids), null);
    }
    return Pair.<Sequence<Centroid>, Query<CentroidDesc>>of(
        Sequences.<Centroid>empty(),
        new FindNearestQuery(
            getDataSource(),
            getQuerySegmentSpec(),
            getFilter(),
            getVirtualColumns(),
            getMetrics(),
            newCentroids,
            getMeasure(),
            context
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
      if (d == 0 || prevCoords[i] == currCoords[i]) {
        continue;
      }
      if (Math.abs(prevCoords[i] - currCoords[i]) / d > deltaThreshold) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Classifier toClassifier(Sequence<Centroid> sequence, String tagColumn)
  {
    Centroid[] centroids = Sequences.toArray(sequence, Centroid.class);
    return new KMeansClassifier(metrics, centroids, DistanceMeasure.of(measure), tagColumn);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return Arrays.asList("center");
  }

  @Override
  public Sequence<Object[]> array(Sequence<Centroid> sequence)
  {
    return Sequences.map(sequence, new Function<Centroid, Object[]>()
    {
      @Override
      public Object[] apply(Centroid input)
      {
        final double[] point = input.getPoint();
        final Object[] array = new Object[point.length];
        for (int i = 0; i < point.length; i++) {
          array[i] = point[i];
        }
        return array;
      }
    });
  }

  @Override
  public Query<Centroid> forLog()
  {
    return new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        getFilter(),
        getVirtualColumns(),
        getMetrics(),
        getNumK(),
        getMaxIteration(),
        getDeltaThreshold(),
        ranges,
        ImmutableList.<Centroid>of(),
        getMeasure(),
        getContext()
    );
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

    if (measure != null) {
      builder.append(", measure=").append(measure);
    }
    if (virtualColumns != null && !virtualColumns.isEmpty()) {
      builder.append(", virtualColumns=").append(virtualColumns);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
