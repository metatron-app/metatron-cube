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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.IntList;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.query.Query;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.InFilter;
import io.druid.segment.filter.MatcherContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("ins")
public class InDimsFilter implements DimFilter.BestEffort, DimFilter.LogProvider, DimFilter.Compressible
{
  private final List<List<String>> values;
  private final List<String> dimensions;
  private final byte[] hash;

  private final Supplier<List<List<String>>> deduped;
  private final Supplier<Map<String, int[]>> mappings;

  @JsonCreator
  public InDimsFilter(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("values") List<List<String>> values,
      @JsonProperty("hash") byte[] hash
  )
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(dimensions), "dimensions can not be empty");
    Preconditions.checkArgument(dimensions.size() == values.size(), "number of dimensions and values is not match");
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(values), "values can not be empty");
    this.dimensions = dimensions;
    this.values = values;
    this.hash = hash;
    this.deduped = DSuppliers.memoize(() -> {
      final List<List<String>> converted = Lists.newArrayListWithCapacity(values.size());
      for (int i = 0; i < values.size(); i++) {
        converted.add(i == 0 ? GuavaUtils.dedupSorted(values.get(i)) : GuavaUtils.sortAndDedup(values.get(i)));
      }
      return converted;
    });
    this.mappings = DSuppliers.memoize(() -> {
      final IntList stash = new IntList();
      final Map<String, int[]> mapping = Maps.newHashMap();
      final List<String> strings = values.get(0);
      String prev = null;
      for (int i = 0; i < strings.size(); i++) {
        final String current = strings.get(i);
        if (prev != null && !current.equals(prev)) {
          mapping.put(prev, stash.array());
          stash.clear();
        }
        prev = current;
        stash.add(i);
      }
      if (!stash.isEmpty()) {
        mapping.put(prev, stash.array());
      }
      return mapping;
    });
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    handler.addAll(dimensions);
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<List<String>> getValues()
  {
    return values;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getHash()
  {
    return hash;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    builder.append(DimFilterCacheKey.INS_CACHE_ID)
           .append(dimensions).sp();
    if (hash != null) {
      builder.append(hash);
    } else {
      builder.appendAll(values);
    }
    return builder;
  }

  @Override
  public DimFilter optimize()
  {
    if (dimensions.size() == 1) {
      return new InDimFilter(dimensions.get(0), null, values.get(0), hash).optimize();
    }
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    List<String> mapped = GuavaUtils.transform(dimensions, d -> mapping.getOrDefault(d, d));
    if (!mapped.equals(dimensions)) {
      return new InDimsFilter(mapped, values, hash);
    }
    return this;
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        final List<List<String>> values = deduped.get();
        final List<BitmapHolder> holders = Lists.newArrayList();
        for (int i = 0; i < dimensions.size(); i++) {
          final List<String> dedup = values.get(i);
          final BitmapHolder holder = InFilter.unionBitmaps(InDimsFilter.this, dimensions.get(i), dedup, context);
          if (holder != null) {
            holders.add(holder);
          }
        }
        return holders.isEmpty() ? null : BitmapHolder.intersection(context, holders, false);
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
      {
        final Map<String, int[]> mapping = mappings.get();
        final ObjectColumnSelector[] selectors = dimensions.stream()
                                                           .map(d -> factory.makeObjectColumnSelector(d))
                                                           .toArray(v -> new ObjectColumnSelector[v]);
        // todo: optimize this
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            // todo: multi-value dimensions?
            final int[] indices = mapping.get(selectors[0].get());
            if (indices == null) {
              return false;
            }
            for (int x = 0; x < indices.length; x++) {
              if (matches(indices[x], 1)) {
                return true;
              }
            }
            return false;
          }

          private boolean matches(int index, int order)
          {
            final String expected = values.get(order).get(index);
            final String value = Objects.toString(selectors[order].get(), "");
            if (Objects.equals(expected, value)) {
              return order == selectors.length - 1 || matches(index, order + 1);
            }
            return false;
          }
        };
      }
    };
  }

  @Override
  public double cost(FilterContext context)
  {
    double cost = 0;
    for (int i = 0; i < values.size(); i++) {
      String dimension = dimensions.get(i);
      ColumnCapabilities capabilities = context.getCapabilities(dimension);
      if (capabilities == null) {
        return cost;    // should not be happened
      }
      cost += capabilities.isDictionaryEncoded() ? PICK * values.get(i).size() * 1.2 : FULLSCAN;
    }
    return cost;
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

    InDimsFilter that = (InDimsFilter) o;

    if (!Objects.equals(dimensions, that.dimensions)) {
      return false;
    }
    if (values.get(0).size() != that.values.get(0).size()) {
      return false;
    }
    if (!Objects.equals(values, that.values)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, values.size(), values.get(0).size(), GuavaUtils.sublist(values.get(0), 10));
  }

  @Override
  public String toString()
  {
    final int size = values.get(0).size();
    final StringBuilder builder = new StringBuilder().append('[');
    for (int i = 0; i < Math.min(10, size); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append('[');
      for (int j = 0; j < values.size(); j++) {
        if (j > 0) {
          builder.append(", ");
        }
        builder.append(values.get(j).get(i));
      }
      builder.append(']');
    }
    if (size > 10) {
      builder.append(String.format(", [..%d more]", size - 10));
    }
    builder.append(']');
    return "InDimsFilter{" +
           "dimensions=" + dimensions +
           ", values=" + builder +
           '}';
  }

  @Override
  public boolean isHeavy()
  {
    return values.get(0).size() * values.size() > 16;
  }

  @Override
  public DimFilter forLog()
  {
    final int size = values.get(0).size();
    if (size > 10) {
      final List<List<String>> cut = Lists.newArrayList();
      for (int i = 0; i < values.size(); i++) {
        List<String> subList = Lists.newArrayList(values.get(i).subList(0, 10));
        subList.add(i == 0 ? String.format("..%d more", values.get(i).size() - 10) : "");
        cut.add(subList);
      }
      return new InDimsFilter(dimensions, cut, null);
    }
    return this;
  }

  @Override
  public DimFilter compress(Query parent)
  {
    return CompressedInsFilter.build(this);
  }
}
