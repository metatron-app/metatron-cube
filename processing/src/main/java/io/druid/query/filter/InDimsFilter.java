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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.IntList;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.query.Query;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
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

  @JsonCreator
  public InDimsFilter(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("values") List<List<String>> values
  )
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(dimensions), "dimensions can not be empty");
    Preconditions.checkArgument(dimensions.size() == values.size(), "number of dimensions and values is not match");
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(values), "values can not be empty");
    this.dimensions = dimensions;
    this.values = values;
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

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.INS_CACHE_ID)
                  .append(dimensions).sp()
                  .appendAll(values);
  }

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    if (dimensions.size() == 1) {
      return new InDimFilter(dimensions.get(0), values.get(0), null).optimize(segment, virtualColumns);
    }
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    List<String> mapped = GuavaUtils.transform(dimensions, d -> mapping.getOrDefault(d, d));
    if (!mapped.equals(dimensions)) {
      return new InDimsFilter(mapped, values);
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
        final List<BitmapHolder> holders = Lists.newArrayList();
        for (int i = 0; i < dimensions.size(); i++) {
          final List<String> value = values.get(i);
          final List<String> dedup = i == 0 ? GuavaUtils.dedupSorted(value) : GuavaUtils.sortAndDedup(value);
          final BitmapHolder holder = InFilter.unionBitmaps(dimensions.get(i), dedup, context.indexSelector());
          if (holder != null) {
            holders.add(holder);
          }
        }
        return holders.isEmpty() ? null : BitmapHolder.intersection(context.bitmapFactory(), holders, false);
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
      {
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
    if (!Objects.equals(values, that.values)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, values);
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
  public DimFilter forLog()
  {
    final int size = values.get(0).size();
    if (size > 10) {
      final List<List<String>> cut = Lists.newArrayList();
      for (int i = 0; i < values.size(); i++) {
        List<String> subList = Lists.newArrayList(values.get(i).subList(0, 10));
        subList.add(i == 0 ? String.format("..%d more", values.size() - 10) : "");
        cut.add(subList);
      }
      return new InDimsFilter(dimensions, cut);
    }
    return this;
  }

  @Override
  public DimFilter compress(Query parent)
  {
    return CompressedInsFilter.build(this);
  }
}
