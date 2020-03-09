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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.GeomUtils;
import io.druid.query.RowResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.lucene.PointQueryType;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.segment.lucene.SpatialOperations;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.spatial")
public class LuceneSpatialFilter extends DimFilter.LuceneFilter implements DimFilter.LogProvider
{
  public static LuceneSpatialFilter convert(LucenePointFilter filter, String field)
  {
    // todo native hand-over
    final double[] latitudes = filter.getLatitudes();
    final double[] longitudes = filter.getLongitudes();
    if (filter.getQuery() == PointQueryType.BBOX) {
      StringBuilder builder = new StringBuilder();
      builder.append("POLYGON((");
      builder.append(longitudes[0]).append(' ').append(latitudes[0]).append(',');
      builder.append(longitudes[0]).append(' ').append(latitudes[1]).append(',');
      builder.append(longitudes[1]).append(' ').append(latitudes[1]).append(',');
      builder.append(longitudes[1]).append(' ').append(latitudes[0]).append(',');
      builder.append(longitudes[0]).append(' ').append(latitudes[0]);
      builder.append("))");
      return new LuceneSpatialFilter(field, SpatialOperations.COVEREDBY, ShapeFormat.WKT, builder.toString());
    } else if (filter.getQuery() != PointQueryType.POLYGON) {
      StringBuilder builder = new StringBuilder();
      builder.append("POLYGON((");
      for (int i = 0; i < latitudes.length; i++) {
        if (i > 0) {
          builder.append(",");
        }
        builder.append(longitudes[i]).append(' ').append(latitudes[i]);
      }
      builder.append("))");
      return new LuceneSpatialFilter(field, SpatialOperations.COVEREDBY, ShapeFormat.WKT, builder.toString());
    }
    return null;
  }

  private final String field;
  private final SpatialOperations operation;
  private final ShapeFormat shapeFormat;
  private final String shapeString;

  @JsonCreator
  public LuceneSpatialFilter(
      @JsonProperty("field") String field,
      @JsonProperty("operation") SpatialOperations operation,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("shapeString") String shapeString
  )
  {
    this.field = Preconditions.checkNotNull(field, "field can not be null");
    this.operation = operation == null ? SpatialOperations.COVEREDBY : operation;
    this.shapeFormat = shapeFormat == null ? ShapeFormat.WKT : shapeFormat;
    this.shapeString = Preconditions.checkNotNull(shapeString, "shapeString can not be null");
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public SpatialOperations getOperation()
  {
    return operation;
  }

  @JsonProperty
  public ShapeFormat getShapeFormat()
  {
    return shapeFormat;
  }

  @JsonProperty
  public String getShapeString()
  {
    return shapeString;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheHelper.LUCENE_SPATIAL_CACHE_ID)
                  .append(field)
                  .append(operation)
                  .append(shapeFormat)
                  .append(shapeString);
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    String replaced = mapping.get(field);
    if (replaced == null || replaced.equals(field)) {
      return this;
    }
    return new LuceneSpatialFilter(replaced, operation, shapeFormat, shapeString);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(field);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, ImmutableBitmap baseBitmap)
      {
        // column-name.field-name or field-name (regarded same with column-name)
        String columnName = field;
        String fieldName = field;
        LuceneIndex lucene = selector.getLuceneIndex(columnName);
        for (int index = field.indexOf('.'); lucene == null && index > 0; index = field.indexOf('.', index + 1)) {
          columnName = field.substring(0, index);
          fieldName = field.substring(index + 1);
          lucene = selector.getLuceneIndex(columnName);
        }
        Preconditions.checkNotNull(lucene, "no lucene index for [%s]", field);

        JtsSpatialContext ctx = JtsSpatialContext.GEO;
        try {
          SpatialPrefixTree grid = new GeohashPrefixTree(ctx, GeohashUtils.MAX_PRECISION);
          SpatialStrategy strategy = new RecursivePrefixTreeStrategy(grid, fieldName);
          return lucene.filterFor(strategy.makeQuery(makeSpatialArgs(ctx)), baseBitmap);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        throw new UnsupportedOperationException("value matcher");
      }

      @Override
      public String toString()
      {
        return LuceneSpatialFilter.this.toString();
      }
    };
  }

  private SpatialArgs makeSpatialArgs(JtsSpatialContext ctx) throws IOException, ParseException
  {
    final Shape shape = shapeFormat.newReader(ctx).read(shapeString);
    if (operation.isLuceneNative()) {
      return new SpatialArgs(operation.op(), shape);
    }
    final Rectangle boundingBox = shape.getBoundingBox();
    if (boundingBox.getMinX() > boundingBox.getMaxX()) {
      // strange result for (180.0 -90.0), (-180.0 90.0)
      boundingBox.reset(boundingBox.getMaxX(), boundingBox.getMinX(), boundingBox.getMinY(), boundingBox.getMaxY());
    }
    switch (operation) {
      case BBOX_INTERSECTS:
        return new SpatialArgs(SpatialOperation.Intersects, boundingBox);
      case BBOX_WITHIN:
        return new SpatialArgs(SpatialOperation.IsWithin, boundingBox);
      case EQUALTO:
      case OVERLAPS:
    }
    throw new UnsupportedOperationException(operation + " is not supported yet");
  }

  @Override
  public DimFilter toExprFilter(RowResolver resolver, String columnName, String fieldName, String descriptor)
  {
    String format = getShapeFormat(descriptor);
    ShapeFormat shapeFormat = format == null ? this.shapeFormat : ShapeFormat.fromString(format);

    String column = fieldName == null ? columnName : String.format("%s.%s", columnName, fieldName);
    String columnReader = GeomUtils.fromColumn(shapeFormat, column);
    String shapeReader = GeomUtils.fromString(shapeFormat, shapeString);
    if (operation == SpatialOperations.BBOX_WITHIN || operation == SpatialOperations.BBOX_INTERSECTS) {
      shapeReader = String.format("geom_bbox(%s)", shapeReader);
    }
    return new MathExprFilter(
        String.format("%s(%s, %s)", toShapeOp(operation), columnReader, shapeReader)
    );
  }

  // I'm not sure of this
  private static String toShapeOp(SpatialOperations operation)
  {
    switch (operation) {
      case INTERSECTS:
      case BBOX_INTERSECTS:
        return "geom_intersects";
      case BBOX_WITHIN:
        return "geom_within";
      case COVERS:
        return "geom_covers";
      case COVEREDBY:
        return "geom_coveredBy";
      case EQUALTO:
        return "geom_equals";
      case OVERLAPS:
        return "geom_overlaps";
      default:
        throw new UnsupportedOperationException("cannot find compatible expression for " + operation);
    }
  }

  @Override
  public DimFilter forLog()
  {
    return new LuceneSpatialFilter(field, operation, shapeFormat, StringUtils.forLog(shapeString));
  }

  @Override
  public String toString()
  {
    return "LuceneSpatialFilter{" +
           "field='" + field + '\'' +
           ", operation=" + operation +
           ", shapeFormat=" + shapeFormat +
           ", shapeString=" + StringUtils.forLog(shapeString) +
           '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, operation, shapeFormat, shapeString);
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

    LuceneSpatialFilter that = (LuceneSpatialFilter) o;

    if (!field.equals(that.field)) {
      return false;
    }
    if (!operation.equals(that.operation)) {
      return false;
    }
    if (!shapeFormat.equals(that.shapeFormat)) {
      return false;
    }
    if (!shapeString.equals(that.shapeString)) {
      return false;
    }

    return true;
  }
}
