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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.GeomUtils;
import io.druid.query.GeometryDeserializer;
import io.druid.query.PostProcessingOperator.ReturnsArray;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Map;

@JsonTypeName("convexHull")
public class GeomConvexHullPostProcessor extends ReturnsArray<Object[]>
{
  private final List<String> columns;
  private final String tagColumn;
  private final String geomColumn;
  private final String convexExpression;

  @JsonCreator
  public GeomConvexHullPostProcessor(
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("tagColumn") String tagColumn,
      @JsonProperty("geomColumn") String geomColumn,
      @JsonProperty("convexExpression") String convexExpression
  )
  {
    this.columns = Preconditions.checkNotNull(columns, "'columns cannot be null");
    this.tagColumn = tagColumn;
    this.geomColumn = Preconditions.checkNotNull(geomColumn, "'geomColumn' can not be null");
    this.convexExpression = Strings.emptyToNull(convexExpression);
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public String getTagColumn()
  {
    return tagColumn;
  }

  @JsonProperty
  public String getGeomColumn()
  {
    return geomColumn;
  }

  @JsonProperty
  public String getConvexExpression()
  {
    return convexExpression;
  }

  @Override
  public QueryRunner<Object[]> postProcess(final QueryRunner<Object[]> baseRunner)
  {
    if (columns.indexOf(geomColumn) < 0 || (tagColumn != null && columns.indexOf(tagColumn) < 0)) {
      return baseRunner;
    }
    final int tagIx = tagColumn == null ? -1 : columns.indexOf(tagColumn);
    final int geomIx = columns.indexOf(geomColumn);
    Preconditions.checkArgument(tagIx != geomIx);

    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        final Sequence<Object[]> sequence = baseRunner.run(query, responseContext);

        final Expr expr = Strings.isNullOrEmpty(convexExpression) ? null : Parser.parse(convexExpression);
        final Map<Object, BatchConvexHull> convexHulls = Maps.newHashMap();
        final Sequence<Object[]> append = Sequences.lazy(columns, () -> {
          final List<Object[]> result = Lists.newArrayList();
          for (Map.Entry<Object, BatchConvexHull> entry : convexHulls.entrySet()) {
            Object[] row = new Object[columns.size()];
            if (tagIx >= 0) {
              row[tagIx] = entry.getKey();
            }
            Object convexHull = entry.getValue().getConvexHull();
            if (expr != null) {
              convexHull = Evals.evalValue(expr, Parser.returnAlways(ExprEval.of(convexHull, GeomUtils.GEOM_TYPE)));
            }
            row[geomIx] = convexHull;
            result.add(row);
          }
          convexHulls.clear();
          return Sequences.simple(result);
        });
        return Sequences.concat(
            new Sequences.PeekingSequence<Object[]>(sequence)
            {
              @Override
              protected Object[] peek(Object[] row)
              {
                if (row[geomIx] instanceof byte[]) {
                  row[geomIx] = GeometryDeserializer.deserialize((byte[]) row[geomIx]);
                }
                if (row[geomIx] instanceof Geometry) {
                  Object key = tagIx < 0 ? null : row[tagIx];
                  convexHulls.computeIfAbsent(key, k -> new BatchConvexHull()).add((Geometry) row[geomIx]);
                }
                return row;
              }
            }, append
        );
      }
    };
  }
}
