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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.math.expr.Expression;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = MathExprFilter.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "and", value = AndDimFilter.class),
    @JsonSubTypes.Type(name = "or", value = OrDimFilter.class),
    @JsonSubTypes.Type(name = "not", value = NotDimFilter.class),
    @JsonSubTypes.Type(name = "selector", value = SelectorDimFilter.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimFilter.class),
    @JsonSubTypes.Type(name = "regex", value = RegexDimFilter.class),
    @JsonSubTypes.Type(name = "search", value = SearchQueryDimFilter.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptDimFilter.class),
    @JsonSubTypes.Type(name = "spatial", value = SpatialDimFilter.class),
    @JsonSubTypes.Type(name = "in", value = InDimFilter.class),
    @JsonSubTypes.Type(name = "bound", value = BoundDimFilter.class),
    @JsonSubTypes.Type(name = "math", value = MathExprFilter.class),
    @JsonSubTypes.Type(name = "true", value = DimFilters.All.class),
    @JsonSubTypes.Type(name = "false", value = DimFilters.None.class),
    @JsonSubTypes.Type(name = "lucene.query", value = LuceneQueryFilter.class),
    @JsonSubTypes.Type(name = "lucene.point", value = LucenePointFilter.class),
    @JsonSubTypes.Type(name = "lucene.nearest", value = LuceneNearestFilter.class),
    @JsonSubTypes.Type(name = "lucene.geojson", value = LuceneGeoJsonPolygonFilter.class),
    @JsonSubTypes.Type(name = "like", value = LikeDimFilter.class),
    @JsonSubTypes.Type(name = "bloom", value = BloomDimFilter.class),
    @JsonSubTypes.Type(name = "bloom.factory", value = BloomDimFilter.Factory.class),
})
public interface DimFilter extends Expression, Cacheable
{
  /**
   * @return Returns an optimized filter.
   * returning the same filter can be a straightforward default implementation.
   * @param segment
   */
  public DimFilter optimize(@Nullable Segment segment);

  /**
   * replaces referencing column names for optimized filtering
   */
  public DimFilter withRedirection(Map<String, String> mapping);

  /**
   * @param handler accumulate dependent dimensions
   */
  public void addDependent(Set<String> handler);

  /**
   * Returns a Filter that implements this DimFilter. This does not generally involve optimizing the DimFilter,
   * so it does make sense to optimize first and then call toFilter on the resulting DimFilter.
   *
   * @param resolver
   *
   * @return a Filter that implements this DimFilter, or null if this DimFilter is a no-op.
   */
  public Filter toFilter(TypeResolver resolver);

  abstract class Abstract implements DimFilter
  {
    @Override
    public DimFilter optimize(Segment segment)
    {
      return this;
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler)
    {
      throw new UnsupportedOperationException("addDependent");
    }

    @Override
    public Filter toFilter(TypeResolver resolver)
    {
      throw new UnsupportedOperationException("toFilter");
    }

    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      throw new UnsupportedOperationException("getCacheKey");
    }
  }

  abstract class NotOptimizable implements DimFilter
  {
    @Override
    public final DimFilter optimize(Segment segment)
    {
      return this;
    }
  }

  class Factory implements Expression.Factory<DimFilter>
  {
    @Override
    public DimFilter or(List<DimFilter> children)
    {
      return DimFilters.or(children);
    }

    @Override
    public DimFilter and(List<DimFilter> children)
    {
      return DimFilters.and(children);
    }

    @Override
    public DimFilter not(DimFilter expression)
    {
      return new NotDimFilter(expression);
    }
  }

  // uses lucene index
  abstract class LuceneFilter implements DimFilter
  {
    public abstract String getField();

    @Override
    public DimFilter optimize(Segment segment)
    {
      if (segment == null) {
        return this;
      }
      String field = getField();
      StorageAdapter adapter = segment.asStorageAdapter(false);
      String columnName = field;
      String fieldName = field;
      Map<String, String> descriptor = adapter.getColumnDescriptor(columnName);
      for (int index = field.indexOf('.'); descriptor == null && index > 0; index = field.indexOf('.', index + 1)) {
        columnName = field.substring(0, index);
        fieldName = field.substring(index + 1);
        descriptor = adapter.getColumnDescriptor(columnName);
      }
      if (descriptor != null && fieldName != null) {
        DimFilter optimized = toOptimizedFilter(descriptor, fieldName);
        if (!segment.isIndexed() && optimized instanceof LuceneFilter) {
          optimized = ((LuceneFilter) optimized).toExprFilter(columnName);
        }
        return optimized;
      }
      columnName = field;
      ColumnCapabilities capabilities = adapter.getColumnCapabilities(columnName);
      for (int index = field.indexOf('.'); capabilities == null && index > 0; index = field.indexOf('.', index + 1)) {
        columnName = field.substring(0, index);
        capabilities = adapter.getColumnCapabilities(columnName);
      }
      return columnName == null ? DimFilters.NONE : toExprFilter(columnName);
    }

    // just best-effort conversion.. instead of 'no lucene index' exception
    protected DimFilter toExprFilter(@NotNull String columnName)
    {
      // return MathExprFilter with shape or esri expressions
      throw new UnsupportedOperationException(String.format("not supports rewritting %s", this));
    }

    protected DimFilter toOptimizedFilter(@NotNull Map<String, String> descriptor, @NotNull String fieldName)
    {
      return this;
    }
  }

  interface RangeFilter extends DimFilter
  {
    boolean possible(TypeResolver resolver);

    List<Range> toRanges(TypeResolver resolver);
  }

  interface BooleanColumnSupport extends DimFilter
  {
    ImmutableBitmap toBooleanFilter(TypeResolver resolver, BitmapIndexSelector selector);
  }

  // marker.. does not use index
  interface ValueOnly extends DimFilter
  {
    @Override
    Filter.ValueOnly toFilter(TypeResolver resolver);
  }

  interface LogProvider extends DimFilter
  {
    DimFilter forLog();
  }

  interface Rewriting extends DimFilter
  {
    DimFilter rewrite(QuerySegmentWalker walker, Query parent);
  }
}
