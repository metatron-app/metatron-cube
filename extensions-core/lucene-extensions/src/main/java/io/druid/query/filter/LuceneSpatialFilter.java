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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.utils.StringUtils;
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
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Shape;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("lucene.spatial")
public class LuceneSpatialFilter implements DimFilter.LuceneFilter
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
    this.operation = Preconditions.checkNotNull(operation, "operation can not be null");
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

  public LuceneSpatialFilter withShapeString(String shapeString)
  {
    return new LuceneSpatialFilter(field, operation, shapeFormat, shapeString);
  }

  public Shape create(SpatialContext context) throws IOException, ParseException
  {
    return shapeFormat.newReader(context).read(shapeString);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldBytes = StringUtils.toUtf8(field);
    byte[] shapeStringBytes = StringUtils.toUtf8(shapeString);
    return ByteBuffer.allocate(3 + fieldBytes.length + shapeStringBytes.length)
                     .put(DimFilterCacheHelper.LUCENE_SPATIAL_CACHE_ID)
                     .put(fieldBytes)
                     .put((byte) operation.ordinal())
                     .put((byte) shapeFormat.ordinal())
                     .put(shapeStringBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
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
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(
          BitmapIndexSelector selector,
          EnumSet<BitmapType> using,
          ImmutableBitmap baseBitmap
      )
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
    switch (operation) {
      case BBOX_INTERSECTS:
        return new SpatialArgs(SpatialOperation.Intersects, shape.getBoundingBox());
      case BBOX_WITHIN:
        return new SpatialArgs(SpatialOperation.IsWithin, shape.getBoundingBox());
      case EQUALTO:
      case OVERLAPS:
    }
    throw new UnsupportedOperationException(operation + " is not supported yet");
  }

  @Override
  public String toString()
  {
    return "LuceneSpatialFilter{" +
           "field='" + field + '\'' +
           ", operation=" + operation +
           ", shapeFormat=" + shapeFormat +
           ", shapeString=" + shapeString +
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
