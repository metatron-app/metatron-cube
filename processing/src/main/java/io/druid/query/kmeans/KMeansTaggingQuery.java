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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.query.BaseQuery;
import io.druid.query.ClassifyPostProcessor;
import io.druid.query.DataSource;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.Query.RewritingQuery;
import io.druid.query.Query.SchemaProvider;
import io.druid.query.Query.WrappingQuery;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowSignature;
import io.druid.query.UnionAllQuery;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("kmeans.tagging")
public class KMeansTaggingQuery extends BaseQuery<Object[]>
    implements RewritingQuery<Object[]>, Query.ArrayOutput, WrappingQuery<Object[]>, SchemaProvider
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
  private final String geomColumn;
  private final boolean appendConvexHull;
  private final String convexExpression;

  private final Query<Object[]> query;

  public KMeansTaggingQuery(
      @JsonProperty("query") Query<Object[]> query,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("numK") int numK,
      @JsonProperty("maxIteration") Integer maxIteration,
      @JsonProperty("deltaThreshold") Double deltaThreshold,
      @JsonProperty("minCount") Integer minCount,
      @JsonProperty("measure") String measure,
      @JsonProperty("maxDistance") Double maxDistance,
      @JsonProperty("tagColumn") String tagColumn,
      @JsonProperty("geomColumn") String geomColumn,
      @JsonProperty("appendConvexHull") boolean appendConvexHull,
      @JsonProperty("convexExpression") String convexExpression,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(
        query,
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        tagColumn,
        geomColumn,
        null,
        null,
        appendConvexHull,
        convexExpression,
        context
    );
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
      String geomColumn,
      List<Range<Double>> ranges,
      List<Centroid> centroids,
      boolean appendConvexHull,
      String convexExpression,
      Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    Preconditions.checkArgument(query instanceof ArrayOutputSupport, "not supported type %s", query.getType());
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
    this.tagColumn = Preconditions.checkNotNull(tagColumn, "'tagColumn' cannot be null");
    this.geomColumn = geomColumn;
    this.measure = measure;
    this.appendConvexHull = appendConvexHull;
    this.convexExpression = appendConvexHull ? convexExpression : null;
    Preconditions.checkArgument(!appendConvexHull || geomColumn != null, "'geomColumn' is needed for appendConvexHull");
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

  public String getTagColumn()
  {
    return tagColumn;
  }

  public String getGeomColumn()
  {
    return geomColumn;
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
        geomColumn,
        ranges,
        centroids,
        appendConvexHull,
        convexExpression,
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
        geomColumn,
        ranges,
        centroids,
        appendConvexHull,
        convexExpression,
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
        geomColumn,
        ranges,
        centroids,
        appendConvexHull,
        convexExpression,
        getContext()
    );
  }

  @Override
  public List<Query> getQueries()
  {
    return Arrays.asList(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public KMeansTaggingQuery withQueries(List<Query> list)
  {
    return new KMeansTaggingQuery(
        Iterables.getOnlyElement(list),
        metrics,
        numK,
        maxIteration,
        deltaThreshold,
        minCount,
        measure,
        maxDistance,
        tagColumn,
        geomColumn,
        ranges,
        centroids,
        appendConvexHull,
        convexExpression,
        getContext()
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
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
        BaseQuery.copyContextForMeta(getContext())
    ).rewriteQuery(segmentWalker, queryConfig);

    Query source = query;
    if (source instanceof RewritingQuery) {
      source = ((RewritingQuery) source).rewriteQuery(segmentWalker, queryConfig);
    }
    ObjectMapper mapper = segmentWalker.getMapper();

    Map<String, Object> context = PostProcessingOperators.append(getContext(), new ClassifyPostProcessor(tagColumn));
    if (appendConvexHull) {
      Map<String, Object> convexHull = ImmutableMap.of(
          "type", "convexHull",
          "columns", estimatedOutputColumns(),
          "tagColumn", tagColumn,
          "geomColumn", geomColumn,
          "convexExpression", Strings.nullToEmpty(convexExpression)
      );
      context = PostProcessingOperators.append(context, PostProcessingOperators.convert(mapper, convexHull));
    }
    return new UnionAllQuery(null, Arrays.asList(kMeansQuery, source), false, -1, 1, context);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    List<String> outputColumns = query.estimatedOutputColumns();
    return outputColumns == null ? null : GuavaUtils.concat(outputColumns, tagColumn);
  }

  @Override
  public RowSignature schema(QuerySegmentWalker segmentWalker)
  {
    final RowSignature signature = Queries.relaySchema(query, segmentWalker);
    List<String> columnNames = GuavaUtils.concat(signature.getColumnNames(), tagColumn);
    List<ValueDesc> columnTypes = GuavaUtils.concat(signature.getColumnTypes(), ValueDesc.LONG);
    return RowSignature.of(columnNames, columnTypes);
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
    if (geomColumn != null) {
      builder.append(", convexHullColumn=").append(geomColumn);
    }
    builder.append(toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT));
    return builder.append('}').toString();
  }
}
