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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.BaseQuery;
import io.druid.query.ClassifyPostProcessor;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.UnionAllQuery;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KMeansTaggingQuery extends BaseQuery<Object[]>
    implements Query.RewritingQuery<Object[]>, Query.ArrayOutputSupport<Object[]>
{
  private final List<String> metrics;
  private final int numK;
  private final int maxIteration;
  private final double deltaThreshold;
  private final double maxDistance;
  private final int minCount;

  private final List<Range<Double>> ranges;
  private final List<Centroid> centroids;
  private final String measure;
  private final String tagColumn;

  private final Query<Object[]> query;

  public KMeansTaggingQuery(
      @JsonProperty("query") Query<Object[]> query,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("numK") int numK,
      @JsonProperty("maxIteration") Integer maxIteration,
      @JsonProperty("deltaThreshold") Double deltaThreshold,
      @JsonProperty("maxIteration") Integer minCount,
      @JsonProperty("measure") String measure,
      @JsonProperty("maxDistance") Double maxDistance,
      @JsonProperty("tagColumn") String tagColumn,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(query, metrics, numK, maxIteration, deltaThreshold, minCount, measure, maxDistance, tagColumn, null, null, context);
  }

  public KMeansTaggingQuery(
      Query<Object[]> query,
      List<String> metrics,
      int numK,
      Integer maxIteration,
      Double deltaThreshold,
      Integer minCount,
      String measure,
      Double maxDistance,
      String tagColumn,
      List<Range<Double>> ranges,
      List<Centroid> centroids,
      Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "not supported type " + query.getType());
    this.query = Preconditions.checkNotNull(query);
    this.metrics = Preconditions.checkNotNull(metrics, "metric cannot be null");
    this.numK = numK;
    Preconditions.checkArgument(maxIteration == null || maxIteration > 0);
    this.maxIteration = maxIteration == null ? KMeansQuery.DEFAULT_MAX_ITERATION : maxIteration;
    Preconditions.checkArgument(deltaThreshold == null || (deltaThreshold > 0 && deltaThreshold < 1));
    this.deltaThreshold = deltaThreshold == null ? KMeansQuery.DEFAULT_DELTA_THRESHOLD : deltaThreshold;
    this.minCount = minCount == null ? -1 : minCount;
    this.maxDistance = maxDistance == null ? -1 : maxDistance;
    this.ranges = ranges;
    this.centroids = centroids;
    Preconditions.checkArgument(numK > 0, "K should be greater than zero");
    Preconditions.checkArgument(!metrics.isEmpty(), "metric cannot be empty");
    if (centroids != null) {
      for (Centroid centroid : centroids) {
        Preconditions.checkArgument(metrics.size() == centroid.getCentroid().length);
      }
    }
    this.tagColumn = tagColumn;
    this.measure = measure;
  }

  @Override
  public String getType()
  {
    return "kmeans.tagging";
  }

  @JsonProperty
  public Query getQuery()
  {
    return query;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
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
  public int getMinCount()
  {
    return minCount;
  }

  @JsonProperty
  public double getMaxDistance()
  {
    return maxDistance;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Range<Double>> getRanges()
  {
    return ranges;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Centroid> getCentroids()
  {
    return centroids;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public String getMeasure()
  {
    return measure;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public String getTagColumn()
  {
    return tagColumn;
  }

  @Override
  public KMeansTaggingQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new KMeansTaggingQuery(
        query,
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        tagColumn,
        ranges,
        centroids,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public KMeansTaggingQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new KMeansTaggingQuery(
        query.withQuerySegmentSpec(spec),
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        tagColumn,
        ranges,
        centroids,
        getContext()
    );
  }

  @Override
  public KMeansTaggingQuery withDataSource(DataSource dataSource)
  {
    return new KMeansTaggingQuery(
        query.withDataSource(dataSource),
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        tagColumn,
        ranges,
        centroids,
        getContext()
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    Map<String, Object> context = getContext();
    Query kMeansQuery = new KMeansQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        BaseQuery.getDimFilter(query),
        Lists.newArrayList(BaseQuery.getVirtualColumns(query)),
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        ranges,
        centroids,
        context
    ).rewriteQuery(segmentWalker, queryConfig);

    Query source = query.getId() == null ? query.withId(getId()) : query;
    if (source instanceof Query.RewritingQuery) {
      source = ((Query.RewritingQuery) source).rewriteQuery(segmentWalker, queryConfig);
    }
    Map<String, Object> postProcessing = ImmutableMap.<String, Object>of(
        QueryContextKeys.POST_PROCESSING, new ClassifyPostProcessor(tagColumn)
    );
    return new UnionAllQuery(
        null,
        Arrays.asList(kMeansQuery, source),
        false,
        -1,
        1,
        computeOverriddenContext(postProcessing)
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> estimatedOutputColumns()
  {
    List<String> outputColumns = ((ArrayOutputSupport) query).estimatedOutputColumns();
    return outputColumns == null ? null : GuavaUtils.concat(outputColumns, tagColumn);
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder(64);
    builder.append(getType()).append('{')
           .append("dataSource='").append(getDataSource()).append('\'')
           .append(", querySegmentSpec=").append(getQuerySegmentSpec())
           .append(", numK=").append(getNumK())
           .append(", metrics=").append(getMetrics())
           .append(", centroids=").append(getCentroids());

    if (measure != null) {
      builder.append(", measure=").append(measure);
    }
    if (tagColumn != null) {
      builder.append(", tagColumn=").append(tagColumn);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
