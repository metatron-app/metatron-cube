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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.util.List;
import java.util.Map;

@JsonTypeName("geo.boundary")
public class GeoBoundaryFilterQuery extends BaseQuery<Object[]>
    implements Query.RewritingQuery<Object[]>, Query.ArrayOutputSupport<Object[]>
{
  private final Query.ArrayOutputSupport<?> query;
  private final String pointColumn;
  private final String shapeColumn;

  private final StreamQuery boundary;
  private final String boundaryColumn;

  public GeoBoundaryFilterQuery(
      @JsonProperty("query") Query.ArrayOutputSupport query,
      @JsonProperty("pointColumn") String pointColumn,
      @JsonProperty("shapeColumn") String shapeColumn,
      @JsonProperty("boundary") StreamQuery boundary,
      @JsonProperty("boundaryColumn") String boundaryColumn,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    this.query = query;
    this.pointColumn = pointColumn;
    this.shapeColumn = shapeColumn;
    this.boundary = Preconditions.checkNotNull(boundary, "boundary");
    this.boundaryColumn = boundaryColumn;
    Preconditions.checkArgument(
        pointColumn == null ^ shapeColumn == null,
        "Must have a valid, non-null pointColumn xor shapeColumn"
    );
    if (boundaryColumn == null) {
      Preconditions.checkArgument(boundary.getColumns().size() == 1, "boundaryColumn");
    } else {
      Preconditions.checkArgument(boundary.getColumns().contains(boundaryColumn), "boundaryColumn");
    }
  }

  @Override
  public String getType()
  {
    return "boundary";
  }

  @JsonProperty
  public Query.ArrayOutputSupport getQuery()
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getShapeColumn()
  {
    return shapeColumn;
  }

  @JsonProperty
  public StreamQuery getBoundary()
  {
    return boundary;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getBoundaryColumn()
  {
    return boundaryColumn;
  }

  @Override
  public GeoBoundaryFilterQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new GeoBoundaryFilterQuery(
        query,
        pointColumn,
        shapeColumn,
        boundary,
        boundaryColumn,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public GeoBoundaryFilterQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new UnsupportedOperationException("withQuerySegmentSpec");
  }

  @Override
  public GeoBoundaryFilterQuery withDataSource(DataSource dataSource)
  {
    throw new UnsupportedOperationException("withDataSource");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final List<String> columns = boundary.getColumns();
    final int geomIndex = boundaryColumn == null ? 0 : columns.indexOf(boundaryColumn);

    final List<Geometry> geometries = Lists.newArrayList();
    final ShapeReader reader = new WKTReader(JtsSpatialContext.GEO, null);
    for (Object[] row : Sequences.toList(boundary.run(segmentWalker, BaseQuery.copyContextForMeta(getContext())))) {
      final String boundary = String.valueOf(Preconditions.checkNotNull(row[geomIndex]));
      if (!StringUtils.isNullOrEmpty(boundary)) {
        geometries.add(((JtsGeometry) reader.readIfSupported(boundary)).getGeom());
      }
    }
    ObjectMapper mapper = segmentWalker.getObjectMapper();
    Geometry union = new CascadedPolygonUnion(geometries).union();
    Map<String, Object> filterMap;
    if (pointColumn != null) {
      filterMap = ImmutableMap.<String, Object>of(
          "type", "lucene.latlon.polygon",
          "field", pointColumn,
          "shapeFormat", "WKT",
          "shapeString", union.toText()
      );
    } else {
      filterMap = ImmutableMap.<String, Object>of(
          "type", "lucene.spatial",
          "operation", "CONTAINS",
          "field", shapeColumn,
          "shapeFormat", "WKT",
          "shapeString", union.toText()
      );
    }
    DimFilter filter = Preconditions.checkNotNull(mapper.convertValue(filterMap, DimFilter.class));
    Query.DimFilterSupport filterSupport = (Query.DimFilterSupport) query;
    return filterSupport.withDimFilter(DimFilters.and(filterSupport.getDimFilter(), filter));
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return query.estimatedOutputColumns();
  }

  @Override
  public Sequence<Object[]> array(Sequence<Object[]> sequence)
  {
    return sequence;
  }

  @Override
  public String toString()
  {
    return "BoundaryFilterQuery{" +
           "query=" + query +
           ", boundary=" + boundary +
           ", boundaryColumn='" + boundaryColumn + '\'' +
           '}';
  }
}
