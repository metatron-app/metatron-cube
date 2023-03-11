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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.DSuppliers;
import io.druid.data.TypeResolver;
import io.druid.data.input.BytesInputStream;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.LogProvider;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.InFilter;
import io.druid.segment.filter.MatcherContext;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DecompressedInFilter implements LogProvider
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final int valueLen;
  private final byte[] values;
  private final byte[] hash;

  private final boolean containsNull;
  private final Supplier<List<String>> strings;
  private final Supplier<List<BinaryRef>> bytes;

  @JsonCreator
  public DecompressedInFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("valueLen") int valueLen,
      @JsonProperty("values") byte[] values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JsonProperty("hash") byte[] hash
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    this.valueLen = valueLen;
    this.values = values;
    this.extractionFn = extractionFn;
    this.hash = hash;
    this.containsNull = new BytesInputStream(values).readUnsignedVarInt() == 0;
    this.bytes = DSuppliers.memoize(() -> new BytesInputStream(values).readVarSizeRaw(valueLen));
    this.strings = DSuppliers.memoize(() -> new BytesInputStream(values).readVarSizeUTFs(valueLen));
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public int getValueLen()
  {
    return valueLen;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getValues()
  {
    return values;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getHash()
  {
    return hash;
  }

  @Override
  public DimFilter forLog()
  {
    return new DecompressedInFilter(dimension, valueLen, null, extractionFn, hash);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    builder.append(DimFilterCacheKey.IN_CACHE_ID)
           .append(dimension).sp()
           .append(extractionFn);
    if (hash != null) {
      builder.append(hash);
    } else {
      builder.append(values);
    }
    return builder;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.add(dimension);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        ColumnCapabilities capabilities = context.getCapabilities(dimension);
        if (capabilities == null) {
          return BitmapHolder.exact(context.indexSelector().createBoolean(containsNull));
        }
        if (extractionFn == null) {
          if (capabilities.isDictionaryEncoded()) {
            return InFilter.unionBitmapsRaw(DecompressedInFilter.this, dimension, bytes, containsNull, context);
          }
          return InFilter.unionBitmaps(DecompressedInFilter.this, dimension, strings, containsNull, context);
        }
        if (capabilities.isDictionaryEncoded()) {
          Set<String> set = Sets.newHashSet(strings.get());
          return BitmapHolder.exact(
              Filters.matchPredicate(dimension, v -> set.contains(Strings.nullToEmpty(extractionFn.apply(v))), context)
          );
        }
        return null;
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
      {
        return new InFilter(DecompressedInFilter.this, dimension, strings.get(), extractionFn).makeMatcher(context, factory);
      }
    };
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

    DecompressedInFilter that = (DecompressedInFilter) o;

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!Objects.equals(extractionFn, that.extractionFn)) {
      return false;
    }
    if (valueLen != that.valueLen) {
      return false;
    }
    return Arrays.equals(hash, that.hash);
  }

  private static final int THRESHOLD = 10;

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, extractionFn, valueLen, Arrays.hashCode(hash));
  }

  @Override
  public String toString()
  {
    return "DecompressedInFilter{" +
           "dimension='" + dimension + '\'' +
           ", valueLen=" + valueLen +
           (extractionFn == null ? "" : ", extractionFn=" + extractionFn) +
           (values == null ? "" : ", sourceLen=" + values.length) +
           '}';
  }

  @Override
  public boolean isHeavy()
  {
    return valueLen > THRESHOLD;
  }
}
