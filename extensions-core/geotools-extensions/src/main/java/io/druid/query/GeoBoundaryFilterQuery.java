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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query.ArrayOutputSupport;
import io.druid.query.Query.FilterSupport;
import io.druid.query.Query.RewritingQuery;
import io.druid.query.Query.SchemaProvider;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.Schema;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.lucene.SpatialOperations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.operation.union.CascadedPolygonUnion;
import org.locationtech.spatial4j.io.ShapeReader;
import org.locationtech.spatial4j.shape.Shape;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("geo.boundary")
public class GeoBoundaryFilterQuery extends BaseQuery<Object[]>
    implements RewritingQuery<Object[]>, ArrayOutputSupport<Object[]>, FilterSupport<Object[]>, SchemaProvider
{
  private static final int DEFAULT_PARALLELISM = 2;

  private final ArrayOutputSupport<?> query;
  private final String queryColumn;
  private final String pointColumn;
  private final String shapeColumn;

  private final ArrayOutputSupport boundary;
  private final String boundaryColumn;
  private final boolean boundaryUnion;
  private final List<String> boundaryJoin;
  private final SpatialOperations operation;
  private final Integer parallelism;

  public GeoBoundaryFilterQuery(
      @JsonProperty("query") ArrayOutputSupport<?> query,
      @JsonProperty("pointColumn") String pointColumn,
      @JsonProperty("shapeColumn") String shapeColumn,
      @JsonProperty("queryColumn") String queryColumn,
      @JsonProperty("boundary") ArrayOutputSupport<?> boundary,
      @JsonProperty("boundaryColumn") String boundaryColumn,
      @JsonProperty("boundaryUnion") Boolean boundaryUnion,
      @JsonProperty("boundaryJoin") List<String> boundaryJoin,
      @JsonProperty("operation") SpatialOperations operation,
      @JsonProperty("parallelism") Integer parallelism,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    this.query = query;
    this.pointColumn = pointColumn;
    this.shapeColumn = shapeColumn;
    this.queryColumn = queryColumn;
    this.boundary = Preconditions.checkNotNull(boundary, "boundary");
    this.boundaryColumn = boundaryColumn;
    this.boundaryUnion = boundaryUnion == null || boundaryUnion;
    this.boundaryJoin = boundaryJoin == null ? ImmutableList.<String>of() : boundaryJoin;
    Preconditions.checkArgument(query instanceof FilterSupport, "'query' should support filters");
    Preconditions.checkArgument(
        queryColumn != null ^ pointColumn != null ^ shapeColumn != null,
        "Must have a valid, non-null 'queryColumn' xor 'pointColumn' xor 'shapeColumn'"
    );
    List<String> boundaryColumns = boundary.estimatedOutputColumns();
    if (boundaryColumn == null) {
      Preconditions.checkArgument(boundaryColumns.size() == 1, "invalid 'boundaryColumn'");
    } else {
      Preconditions.checkArgument(boundaryColumns.contains(boundaryColumn), "invalid 'boundaryColumn'");
    }
    if (pointColumn != null && operation != null) {
      Preconditions.checkArgument(
          operation == SpatialOperations.COVEREDBY, "cannot apply %s on point colunm", operation
      );
    }
    this.operation = operation;
    this.parallelism = parallelism;
  }

  @Override
  public String getType()
  {
    return "geo.boundary";
  }

  @JsonProperty
  public ArrayOutputSupport getQuery()
  {
    return query;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getQueryColumn()
  {
    return queryColumn;
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
  public ArrayOutputSupport getBoundary()
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
  public boolean isBoundaryUnion()
  {
    return boundaryUnion;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getBoundaryJoin()
  {
    return boundaryJoin;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public SpatialOperations getOperation()
  {
    return operation;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getParallelism()
  {
    return parallelism;
  }


  @Override
  public List<VirtualColumn> getVirtualColumns()
  {
    return ((FilterSupport<?>) query).getVirtualColumns();
  }

  @Override
  public VCSupport<Object[]> withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new GeoBoundaryFilterQuery(
        (ArrayOutputSupport) ((FilterSupport<?>) query).withVirtualColumns(virtualColumns),
        pointColumn,
        shapeColumn,
        queryColumn,
        boundary,
        boundaryColumn,
        boundaryUnion,
        boundaryJoin,
        operation,
        parallelism,
        getContext()
    );
  }

  @Override
  public DimFilter getFilter()
  {
    return ((FilterSupport<?>) query).getFilter();
  }

  @Override
  public FilterSupport<Object[]> withFilter(DimFilter filter)
  {
    return new GeoBoundaryFilterQuery(
        (ArrayOutputSupport) ((FilterSupport<?>) query).withFilter(filter),
        pointColumn,
        shapeColumn,
        queryColumn,
        boundary,
        boundaryColumn,
        boundaryUnion,
        boundaryJoin,
        operation,
        parallelism,
        getContext()
    );
  }

  @Override
  public GeoBoundaryFilterQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new GeoBoundaryFilterQuery(
        query,
        pointColumn,
        shapeColumn,
        queryColumn,
        boundary,
        boundaryColumn,
        boundaryUnion,
        boundaryJoin,
        operation,
        parallelism,
        computeOverriddenContext(contextOverride)
    );
  }

  public GeoBoundaryFilterQuery withBoundaryUnion(boolean boundaryUnion)
  {
    return new GeoBoundaryFilterQuery(
        query,
        pointColumn,
        shapeColumn,
        queryColumn,
        boundary,
        boundaryColumn,
        boundaryUnion,
        boundaryJoin,
        operation,
        parallelism,
        getContext()
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
  public Schema schema(QuerySegmentWalker segmentWalker)
  {
    final List<String> names = Lists.newArrayList();
    final List<ValueDesc> types = Lists.newArrayList();

    final Schema querySchema = Queries.relaySchema(query, segmentWalker);
    final List<String> queryColumns = querySchema.getColumnNames();
    final List<ValueDesc> queryTypes = querySchema.getColumnTypes();
    for (String column : query.estimatedOutputColumns()) {
      int index = queryColumns.indexOf(column);
      if (index >= 0) {
        names.add(column);
        types.add(queryTypes.get(index));
      }
    }
    if (!boundaryJoin.isEmpty()) {
      final Schema boundarySchema = Queries.relaySchema(boundary, segmentWalker);
      final List<String> boundayColumns = boundarySchema.getColumnNames();
      final List<ValueDesc> boundayTypes = boundarySchema.getColumnTypes();
      for (String column : boundaryJoin) {
        int index = boundayColumns.indexOf(column);
        if (index >= 0) {
          names.add(column);
          types.add(boundayTypes.get(index));
        }
      }
    }
    return Schema.of(GuavaUtils.zipAsMap(names, types));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final ObjectMapper mapper = segmentWalker.getObjectMapper();
    final int executor = parallelism == null ? DEFAULT_PARALLELISM : parallelism;
    final List<String> columns = boundary.estimatedOutputColumns();
    final String boundaryColumn = this.boundaryColumn == null ? columns.get(0) : this.boundaryColumn;
    final int geomIndex = columns.indexOf(boundaryColumn);

    final Map<String, Integer> joinMapping = Maps.newLinkedHashMap();
    for (String column : boundaryJoin) {
      if (column.equals(boundaryColumn)) {
        joinMapping.put(column, -1);
        continue;
      }
      int index = columns.indexOf(column);
      if (index >= 0) {
        joinMapping.put(column, index);
      }
    }
    final List<Object[]> rows = Lists.newArrayList();
    final List<Geometry> geometries = Lists.newArrayList();
    final ShapeReader reader = ShapeUtils.newWKTReader();
    final Map<String, Object> context = BaseQuery.copyContextForMeta(getContext());
    final ArrayOutputSupport runner = (ArrayOutputSupport) boundary.withOverriddenContext(context);
    final Sequence<Object[]> array = runner.array(QueryRunners.run(runner, segmentWalker));
    for (Object[] row : Sequences.toList(array)) {
      String boundary = Objects.toString(row[geomIndex], null);
      if (!StringUtils.isNullOrEmpty(boundary)) {
        Shape shape = reader.readIfSupported(boundary);
        if (shape != null) {
          geometries.add(Preconditions.checkNotNull(
              ShapeUtils.toGeometry(shape), "cannot convert shape [%s] to geometry", shape
          ));
          rows.add(row);
        }
      }
    }
    if (geometries.isEmpty()) {
      return DummyQuery.instance();
    }
    if (boundaryUnion) {
      // use first row as joinRow.. apply aggregator?
      Geometry union = new CascadedPolygonUnion(geometries).union();
      if (union instanceof GeometryCollection && union.getNumGeometries() > 1) {
        List<Query> queries = Lists.newArrayList();
        for (int i = 0; i < union.getNumGeometries(); i++) {
          Geometry geometry = union.getGeometryN(i);
          Object[] joinRow = null;
          if (!joinMapping.isEmpty()) {
            for (int j = 0; j < geometries.size(); j++) {
              if (!geometries.get(j).disjoint(geometry)) {
                joinRow = rows.get(j);
                break;
              }
            }
            if (joinRow == null) {
              throw new IllegalStateException("cannot find geometry in " + geometry.toText());
            }
          }
          queries.add(makeQuery(mapper, geometry, joinMapping, joinRow, context));
        }
        return UnionAllQuery.union(queries, this, segmentWalker);
      }
      return makeQuery(mapper, union, joinMapping, rows.get(0), context);
    }
    List<Query> queries = Lists.newArrayList();
    for (int i = 0; i < geometries.size(); i++) {
      queries.add(makeQuery(mapper, geometries.get(i), joinMapping, rows.get(i), context));
    }
    return UnionAllQuery.union(queries, this, segmentWalker);
  }

  private Query makeQuery(
      ObjectMapper mapper,
      Geometry geometry,
      Map<String, Integer> joinMapping,
      Object[] joinRow,
      Map<String, Object> context
  )
  {
    String geometryWKT = geometry.toText();
    DimFilter filter = Preconditions.checkNotNull(mapper.convertValue(makeFilter(geometryWKT), DimFilter.class));
    FilterSupport filterSupport = (FilterSupport) query;
    Query filtered = filterSupport.withFilter(DimFilters.and(filterSupport.getFilter(), filter))
                                  .withOverriddenContext(context);
    return filtered.withOverriddenContext(
        Query.POST_PROCESSING, new SequenceMapProcessor(proc(query, geometryWKT, joinMapping, joinRow))
    );
  }

  private <T> Function<Sequence<T>, Sequence<Object[]>> proc(
      final ArrayOutputSupport<T> query,
      final String geometryWKT,
      final Map<String, Integer> joinMapping,
      final Object[] row
  )
  {
    if (GuavaUtils.isNullOrEmpty(joinMapping)) {
      return new Function<Sequence<T>, Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> apply(Sequence<T> input)
        {
          return query.array(input);
        }
      };
    }
    return new Function<Sequence<T>, Sequence<Object[]>>()
    {
      @Override
      public Sequence<Object[]> apply(Sequence<T> input)
      {
        return Sequences.map(
            query.array(input),
            new Function<Object[], Object[]>()
            {
              @Override
              public Object[] apply(final Object[] array)
              {
                final Object[] updatable = Arrays.copyOf(array, array.length + joinMapping.size());
                int i = 0;
                for (Map.Entry<String, Integer> entry : joinMapping.entrySet()) {
                  updatable[array.length + i++] = entry.getValue() < 0 ? geometryWKT : row[entry.getValue()];
                }
                return updatable;
              }
            }
        );
      }
    };
  }

  private Map<String, Object> makeFilter(String geometryWKT)
  {
    SpatialOperations op = operation == null ? SpatialOperations.COVEREDBY : operation;
    if (shapeColumn != null) {
      return ImmutableMap.<String, Object>of(
          "type", "lucene.spatial",
          "operation", op.getName(),
          "field", shapeColumn,
          "shapeFormat", "WKT",
          "shapeString", geometryWKT
      );
    } else if (pointColumn != null) {
      // only supports 'coveredBy'
      Preconditions.checkArgument(op == SpatialOperations.COVEREDBY);
      return ImmutableMap.<String, Object>of(
          "type", "lucene.latlon.polygon",
          "field", pointColumn,
          "shapeFormat", "WKT",
          "shapeString", geometryWKT
      );
    } else {
      Preconditions.checkNotNull(queryColumn);
      return ImmutableMap.<String, Object>of(
          "type", "lucene.shape",
          "operation", op.getName(),
          "field", queryColumn,
          "shapeFormat", "WKT",
          "shapeString", geometryWKT
      );
    }
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return GuavaUtils.concat(
        Preconditions.checkNotNull(query.estimatedOutputColumns()), boundaryJoin
    );
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
    GeoBoundaryFilterQuery that = (GeoBoundaryFilterQuery) o;
    return boundaryUnion == that.boundaryUnion &&
           Objects.equals(query, that.query) &&
           Objects.equals(pointColumn, that.pointColumn) &&
           Objects.equals(shapeColumn, that.shapeColumn) &&
           Objects.equals(queryColumn, that.queryColumn) &&
           Objects.equals(boundary, that.boundary) &&
           Objects.equals(boundaryColumn, that.boundaryColumn) &&
           Objects.equals(boundaryUnion, that.boundaryUnion) &&
           Objects.equals(boundaryJoin, that.boundaryJoin) &&
           Objects.equals(operation, that.operation) &&
           Objects.equals(parallelism, that.parallelism);
  }

  @Override
  public String toString()
  {
    return "GeoBoundaryFilterQuery{" +
           "query=" + query +
           (pointColumn == null ? "" : ", pointColumn=" + pointColumn) +
           (shapeColumn == null ? "" : ", shapeColumn=" + shapeColumn) +
           (queryColumn == null ? "" : ", queryColumn=" + queryColumn) +
           ", boundary=" + boundary +
           (boundaryColumn == null ? "" : ", boundaryColumn=" + boundaryColumn) +
           ", boundaryUnion='" + boundaryUnion + '\'' +
           ", boundaryJoin=" + boundaryJoin +
           ", operation=" + (operation == null ? SpatialOperations.COVEREDBY : operation) +
           (parallelism == null ? "" : ", parallelism=" + parallelism) +
           '}';
  }
}
