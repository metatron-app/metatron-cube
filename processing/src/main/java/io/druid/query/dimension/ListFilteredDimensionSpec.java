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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.DimensionSelector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class ListFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x3;

  private final Set<String> values;
  private final boolean isWhitelist;

  public ListFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("values") Set<String> values,
      @JsonProperty("isWhitelist") Boolean isWhitelist
  )
  {
    super(delegate);

    Preconditions.checkArgument(values != null && values.size() > 0, "values list must be non-empty");
    this.values = values;

    this.isWhitelist = isWhitelist == null ? true : isWhitelist.booleanValue();
  }

  @JsonProperty
  public Set<String> getValues()
  {
    return values;
  }

  @JsonProperty("isWhitelist")
  public boolean isWhitelist()
  {
    return isWhitelist;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector, TypeResolver resolver)
  {
    if (selector == null) {
      return selector;
    }

    int selectorCardinality = selector.getValueCardinality();
    if (selectorCardinality < 0) {
      throw new UnsupportedOperationException("cannot use ListFilteredDimensionSpec on " + delegate.getDimension());
    }
    int cardinality = isWhitelist ? values.size() : selectorCardinality - values.size();

    int count = 0;
    final Map<Integer, Integer> forwardMapping = new HashMap<>(cardinality);
    final int[] reverseMapping = new int[cardinality];

    if (isWhitelist) {
      for (String value : values) {
        int i = selector.lookupId(value);
        if (i >= 0) {
          forwardMapping.put(i, count);
          reverseMapping[count++] = i;
        }
      }
    } else {
      for (int i = 0; i < selectorCardinality; i++) {
        if (!values.contains(Objects.toString(selector.lookupName(i), ""))) {
          forwardMapping.put(i, count);
          reverseMapping[count++] = i;
        }
      }
    }

    return BaseFilteredDimensionSpec.decorate(selector, forwardMapping, reverseMapping);
  }

  @Override
  public DimensionSpec withOutputName(String outputName)
  {
    return new ListFilteredDimensionSpec(delegate.withOutputName(outputName), values, isWhitelist);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(delegate)
                  .append(isWhitelist)
                  .append(values);
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

    ListFilteredDimensionSpec that = (ListFilteredDimensionSpec) o;

    if (isWhitelist != that.isWhitelist) {
      return false;
    }
    return values.equals(that.values);

  }

  @Override
  public int hashCode()
  {
    int result = values.hashCode();
    result = 31 * result + (isWhitelist ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ListFilteredDimensionSpec{" +
           "values=" + values +
           ", isWhitelist=" + isWhitelist +
           '}';
  }
}
