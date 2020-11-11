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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.InFilter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class InDimsFilter implements DimFilter.BestEffort
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
    for (List<String> value : values) {

    }
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
                  .append(values);
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
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        final List<BitmapHolder> holders = Lists.newArrayList();
        for (int i = 0; i < dimensions.size(); i++) {
          final String dimension = dimensions.get(i);
          final BitmapHolder holder = new InFilter(dimension, values.get(i), null).getBitmapIndex(context);
          if (holder != null) {
            holders.add(holder);
          }
        }
        return holders.isEmpty() ? null : BitmapHolder.intersection(context.bitmapFactory(), holders);
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
      {
        final List<ValueMatcher> matchers = Lists.newArrayList();
        for (int i = 0; i < dimensions.size(); i++) {
          final String dimension = dimensions.get(i);
          final Set<String> valueSet = Sets.newHashSet(values.get(i));
          final ValueMatcher matcher = new InFilter(dimension, valueSet, null).makeMatcher(factory);
          if (matcher != null) {
            matchers.add(matcher);
          }
        }
        return AndFilter.makeMatcher(matchers);
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
    return "InDimsFilter{" +
           "dimensions=" + dimensions +
           ", values=" + StringUtils.limit(values.toString(), 40) +
           '}';
  }
}
