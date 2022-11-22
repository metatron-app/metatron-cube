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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowResolver;
import io.druid.segment.AttachmentVirtualColumn;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    @JsonSubTypes.Type(name = "regex.match", value = RegexMatchFilter.class),
    @JsonSubTypes.Type(name = "search", value = SearchQueryDimFilter.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptDimFilter.class),
    @JsonSubTypes.Type(name = "spatial", value = SpatialDimFilter.class),
    @JsonSubTypes.Type(name = "in", value = InDimFilter.class),
    @JsonSubTypes.Type(name = "bound", value = BoundDimFilter.class),
    @JsonSubTypes.Type(name = "math", value = MathExprFilter.class),
    @JsonSubTypes.Type(name = "true", value = DimFilters.All.class),
    @JsonSubTypes.Type(name = "false", value = DimFilters.None.class),
    @JsonSubTypes.Type(name = "ins", value = InDimsFilter.class),
    @JsonSubTypes.Type(name = "like", value = LikeDimFilter.class),
    @JsonSubTypes.Type(name = "bloom", value = BloomDimFilter.class),
    @JsonSubTypes.Type(name = "bloom.factory", value = BloomDimFilter.Factory.class),
    @JsonSubTypes.Type(name = "prefix", value = PrefixDimFilter.class),
    @JsonSubTypes.Type(name = "in.compressed", value = CompressedInFilter.class),
    @JsonSubTypes.Type(name = "ins.compressed", value = CompressedInsFilter.class),
})
public interface DimFilter extends Expression, Cacheable
{
  /**
   * @return Returns an optimized filter.
   */
  default DimFilter optimize()
  {
    return this;
  }

  /**
   * @return Returns a specialized filter.
   *
   * @param segment
   * @param virtualColumns
   */
  default DimFilter specialize(Segment segment, @Nullable List<VirtualColumn> virtualColumns)
  {
    return this;
  }

  /**
   * replaces referencing column names for optimized filtering
   */
  default DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  /**
   * @param handler accumulate dependent dimensions
   */
  void addDependent(Set<String> handler);

  /**
   * Returns a Filter that implements this DimFilter. This does not generally involve optimizing the DimFilter,
   * so it does make sense to optimize first and then call toFilter on the resulting DimFilter.
   *
   * @param resolver
   *
   * @return a Filter that implements this DimFilter, or null if this DimFilter is a no-op.
   */
  Filter toFilter(TypeResolver resolver);

  abstract class SingleInput implements DimFilter
  {
    protected abstract String getDimension();

    protected abstract DimFilter withDimension(String dimension);

    @Override
    public final void addDependent(Set<String> handler)
    {
      handler.add(getDimension());
    }

    @Override
    public final DimFilter withRedirection(Map<String, String> mapping)
    {
      String field = getDimension();
      String replaced = mapping.get(field);
      if (replaced != null && !replaced.equals(field)) {
        return withDimension(replaced);
      }
      return this;
    }
  }

  abstract class FilterFactory implements DimFilter
  {
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

  // uses lucene index
  public abstract class LuceneFilter implements BestEffort, VCInflator
  {
    public final String field;
    public final String scoreField;

    protected LuceneFilter(String field, String scoreField)
    {
      this.field = Preconditions.checkNotNull(field, "field can not be null");
      this.scoreField = scoreField;
    }

    @JsonProperty
    public String getField()
    {
      return field;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getScoreField()
    {
      return scoreField;
    }

    @Override
    public void addDependent(Set<String> handler)
    {
      handler.add(field);
    }

    @Override
    public VirtualColumn inflate()
    {
      return scoreField != null ? new AttachmentVirtualColumn(scoreField, ValueDesc.FLOAT) : null;
    }

    @Override
    public DimFilter specialize(Segment segment, List<VirtualColumn> virtualColumns)
    {
      String field = getField();
      StorageAdapter adapter = segment.asStorageAdapter(false);

      String columnName = field;
      String fieldName = null;
      String descriptor = null;

      DimFilter optimized = null;
      if (optimized == null) {
        Map<String, String> descriptors = adapter.getColumnDescriptor(columnName);
        if (descriptors != null && descriptors.containsKey(columnName)) {
          optimized = toOptimizedFilter(columnName, null, descriptor = descriptors.get(columnName));
        }
      }
      if (optimized == null) {
        for (int index = field.indexOf('.'); optimized == null && index > 0; index = field.indexOf('.', index + 1)) {
          columnName = field.substring(0, index);
          fieldName = field.substring(index + 1);
          Map<String, String> descriptors = adapter.getColumnDescriptor(columnName);
          if (descriptors != null && descriptors.containsKey(fieldName)) {
            optimized = toOptimizedFilter(columnName, fieldName, descriptor = descriptors.get(fieldName));
          }
        }
      }
      // regard invalid field name (calcite replaces struct to first field of struct. what the..)
      if (optimized == null) {
        for (int index = field.length(); optimized == null && index > 0; index = field.lastIndexOf('.', index - 1)) {
          columnName = field.substring(0, index);
          for (Map.Entry<String, String> entry : GuavaUtils.optional(adapter.getColumnDescriptor(columnName))) {
            fieldName = columnName.equals(entry.getKey()) ? null : entry.getKey();
            optimized = toOptimizedFilter(columnName, fieldName, descriptor = entry.getValue());
            if (optimized != null) {
              break;
            }
          }
        }
      }
      if (optimized != null) {
        if (!segment.isIndexed() && optimized instanceof LuceneFilter) {
          RowResolver resolver = RowResolver.of(adapter, virtualColumns);
          optimized = ((LuceneFilter) optimized).toExprFilter(resolver, columnName, fieldName, descriptor);
        }
        return optimized;
      }
      // find any column exists
      columnName = field;
      ColumnCapabilities capabilities = adapter.getColumnCapabilities(columnName);
      for (int index = field.indexOf('.'); capabilities == null && index > 0; index = field.indexOf('.', index + 1)) {
        columnName = field.substring(0, index);
        capabilities = adapter.getColumnCapabilities(columnName);
      }
      RowResolver resolver = RowResolver.of(adapter, virtualColumns);
      return toExprFilter(resolver, columnName == null ? field : columnName, null, null);
    }

    // just best-effort conversion.. instead of 'no lucene index' exception

    protected DimFilter toOptimizedFilter(
        @NotNull String columnName, @Nullable String fieldName, @NotNull String descriptor
    )
    {
      return this;
    }

    protected DimFilter toExprFilter(
        @NotNull RowResolver resolver,
        @NotNull String columnName,
        @Nullable String fieldName,
        @Nullable String descriptor
    )
    {
      // return MathExprFilter with shape or esri expressions
      throw new UnsupportedOperationException(String.format("not supports rewritting %s", getClass().getSimpleName()));
    }

    // see LatLonPointIndexingStrategy : point(latitude=%s,longitude=%s)
    protected static final Pattern LATLON_PATTERN = Pattern.compile("point\\(latitude=([^,]+),longitude=([^,]+)\\)");

    protected String toPointExpr(RowResolver resolver, String columnName, String fieldName, String descriptor)
    {
      if (descriptor != null) {
        Matcher matcher = LATLON_PATTERN.matcher(descriptor);
        if (matcher.find()) {
          return String.format(
              "geom_fromLatLon(\"%s.%s\", \"%s.%s\")", columnName, matcher.group(1), columnName, matcher.group(2)
          );
        }
     }
      String field = fieldName == null || fieldName.equals(columnName)
                     ? String.format("\"%s\"", columnName)
                     : String.format("\"%s.%s\"", columnName, fieldName);
      if (ValueDesc.isGeometry(Parser.parse(field, resolver).returns())) {
        return field;
      }
      return String.format("geom_fromLatLon(%s)", field);
    }

    // see ShapeIndexingStrategy : shape(format=%s)
    static final Pattern SHAPE_PATTERN = Pattern.compile("shape\\(format=([^,]+)\\)");

    protected String getShapeFormat(String descriptor)
    {
      if (descriptor != null) {
        Matcher matcher = SHAPE_PATTERN.matcher(descriptor);
        if (matcher.find()) {
          return matcher.group(1);
        }
      }
      return null;
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

  // marker.. skip trying with index
  interface MathcherOnly extends DimFilter
  {
    @Override
    Filter.MatcherOnly toFilter(TypeResolver resolver);
  }

  // marker.. logics in Filters should be moved into each filters
  interface BestEffort extends DimFilter
  {
  }

  // marker
  interface IndexedIDSupport extends DimFilter
  {
  }

  interface LogProvider extends DimFilter
  {
    DimFilter forLog();
  }

  interface Rewriting extends DimFilter
  {
    DimFilter rewrite(QuerySegmentWalker walker, Query parent);
  }

  interface Compressible extends LogProvider
  {
    DimFilter compress(Query parent);
  }

  interface Compressed extends LogProvider
  {
    DimFilter decompress(Query parent);
  }

  interface VCInflator extends DimFilter
  {
    VirtualColumn inflate();
  }

  enum OP {AND, OR}

  interface Mergeable extends DimFilter
  {
    boolean supports(OP op, DimFilter other);

    DimFilter merge(OP op, DimFilter other);
  }
}
