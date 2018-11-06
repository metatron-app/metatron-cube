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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.lucene.ShapeFormat;

import java.util.List;
import java.util.Map;

@JsonTypeName("choropleth")
public class ChoroplethMapQuery extends BaseQuery<Object[]> implements Query.RewritingQuery<Object[]>, Query.ArrayOutputSupport<Object[]>
{
  private final GroupByQuery query;
  private final String pointColumn;
  private final StreamRawQuery boundary;
  private final String boundaryColumn;
  private final Map<String, String> boundaryJoin;

  public ChoroplethMapQuery(
      @JsonProperty("query") GroupByQuery query,
      @JsonProperty("pointColumn") String pointColumn,
      @JsonProperty("boundary") StreamRawQuery boundary,
      @JsonProperty("boundaryColumn") String boundaryColumn,
      @JsonProperty("boundaryJoin") Map<String, String> boundaryJoin,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    this.query = query;
    this.pointColumn = Preconditions.checkNotNull(pointColumn, "pointColumn");
    this.boundary = boundary;
    this.boundaryColumn = boundaryColumn;
    this.boundaryJoin = boundaryJoin == null ? ImmutableMap.<String, String>of() : boundaryJoin;
    if (boundaryColumn == null) {
      Preconditions.checkArgument(boundary.getColumns().size() == 1, "boundaryColumn");
    } else {
      Preconditions.checkArgument(boundary.getColumns().contains(boundaryColumn), "boundaryColumn");
    }
  }

  @Override
  public String getType()
  {
    return "choropleth";
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPointColumn()
  {
    return pointColumn;
  }

  @JsonProperty
  public StreamRawQuery getBoundary()
  {
    return boundary;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getBoundaryColumn()
  {
    return boundaryColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getBoundaryJoin()
  {
    return boundaryJoin;
  }

  @Override
  public ChoroplethMapQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new ChoroplethMapQuery(
        query,
        pointColumn,
        boundary,
        boundaryColumn,
        boundaryJoin,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public ChoroplethMapQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new ChoroplethMapQuery(
        query.withQuerySegmentSpec(spec),
        pointColumn,
        boundary,
        boundaryColumn,
        boundaryJoin,
        getContext()
    );
  }

  @Override
  public ChoroplethMapQuery withDataSource(DataSource dataSource)
  {
    return new ChoroplethMapQuery(
        query.withDataSource(dataSource),
        pointColumn,
        boundary,
        boundaryColumn,
        boundaryJoin,
        getContext()
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    final List<String> columns = boundary.getColumns();
    final int geomIndex = boundaryColumn == null ? 0 : columns.indexOf(boundaryColumn);
    final Map<String, Integer> joinMapping = Maps.newHashMap();
    for (Map.Entry<String, String> join : boundaryJoin.entrySet()) {
      int index = columns.indexOf(join.getValue());
      if (index >= 0) {
        joinMapping.put(join.getKey(), index);
      }
    }
    Map<String, Object> context = BaseQuery.copyContextForMeta(getContext());
    LuceneLatLonPolygonFilter polygonFilter = new LuceneLatLonPolygonFilter(pointColumn, ShapeFormat.WKT, "");
    DimFilter filter = query.getDimFilter();
    List<Query> queries = Lists.newArrayList();
    for (final Object[] row : Sequences.toList(boundary.run(segmentWalker, context))) {
      String boundary = String.valueOf(Preconditions.checkNotNull(row[geomIndex]));
      if (boundary.isEmpty()) {
        continue;
      }
      GroupByQuery filtered = query.withDimFilter(DimFilters.and(filter, polygonFilter.withWKT(boundary)));
      if (!joinMapping.isEmpty()) {
        Map<String, Object> copy = Maps.newHashMap(context);
        copy.put(QueryContextKeys.POST_PROCESSING, new TransformPostProcessor(
            new Function<Row, Row>()
            {
              @Override
              public Row apply(Row input)
              {
                Row.Updatable updatable = Rows.toUpdatable(input);
                for (Map.Entry<String, Integer> entry : joinMapping.entrySet()) {
                  updatable.set(entry.getKey(), row[entry.getValue()]);
                }
                return updatable;
              }
            }
        ));
        filtered = filtered.withOverriddenContext(copy);
      }
      queries.add(filtered);
    }
    Map<String, Object> copy = Maps.newHashMap(context);
    copy.put(QueryContextKeys.POST_PROCESSING, new RowToArray(estimatedOutputColumns()));
    return new UnionAllQuery(null, queries, false, -1, 3, copy);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return GuavaUtils.concat(
        Preconditions.checkNotNull(query.estimatedOutputColumns()),
        Lists.newArrayList(boundaryJoin.keySet())
    );
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  public String toString()
  {
    return "ChoroplethMapQuery{" +
           "query=" + query +
           ", pointColumn='" + pointColumn + '\'' +
           ", boundary=" + boundary +
           ", boundaryColumn='" + boundaryColumn + '\'' +
           ", boundaryJoin=" + boundaryJoin +
           '}';
  }
}
