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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Ranges;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.filter.InFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class InDimFilter extends SingleInput implements RangeFilter, DimFilter.LogProvider
{
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final List<String> values;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("values") Collection<String> values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    this(
        dimension,
        extractionFn,
        ImmutableList.copyOf(ImmutableSortedSet.copyOf(Iterables.transform(values, s -> Strings.nullToEmpty(s))))
    );
  }

  public InDimFilter(String dimension, ExtractionFn extractionFn, List<String> values)
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    this.extractionFn = extractionFn;
    this.values = Preconditions.checkNotNull(values, "values can not be null");
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  protected DimFilter withDimension(String dimension)
  {
    return new InDimFilter(dimension, extractionFn, values);
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.IN_CACHE_ID)
                  .append(dimension).sp()
                  .append(values).sp()
                  .append(extractionFn);
  }

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    InDimFilter inFilter = optimizeLookup();
    if (inFilter.values.size() == 1) {
      return new SelectorDimFilter(inFilter.dimension, inFilter.values.get(0), inFilter.getExtractionFn());
    }
    return inFilter;
  }

  private InDimFilter optimizeLookup()
  {
    if (extractionFn instanceof LookupExtractionFn
        && ((LookupExtractionFn) extractionFn).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) extractionFn;
      LookupExtractor lookup = exFn.getLookup();

      final List<String> keys = new ArrayList<>();
      for (String value : values) {

        // We cannot do an unapply()-based optimization if the selector value
        // and the replaceMissingValuesWith value are the same, since we have to match on
        // all values that are not present in the lookup.
        final String convertedValue = Strings.emptyToNull(value);
        if (!exFn.isRetainMissingValue() && Objects.equals(convertedValue, exFn.getReplaceMissingValueWith())) {
          return this;
        }
        for (Object key : lookup.unapply(convertedValue)) {
          if (key instanceof String) {
            keys.add((String) key);
          }
        }

        // If retainMissingValues is true and the selector value is not in the lookup map,
        // there may be row values that match the selector value but are not included
        // in the lookup map. Match on the selector value as well.
        // If the selector value is overwritten in the lookup map, don't add selector value to keys.
        if (exFn.isRetainMissingValue() && lookup.apply(convertedValue) == null) {
          keys.add(convertedValue);
        }
      }

      if (keys.isEmpty()) {
        return this;
      } else {
        return new InDimFilter(dimension, null, keys);
      }
    }
    return this;
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new InFilter(dimension, values, extractionFn);
  }

  @Override
  public boolean possible(TypeResolver resolver)
  {
    return extractionFn == null;
  }

  @Override
  public List<Range> toRanges(TypeResolver resolver)
  {
    Preconditions.checkArgument(extractionFn == null, "extractionFn");
    ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING);
    if (resolved.isStringOrDimension()) {
      resolved = ValueDesc.STRING;
    }
    List<Range> ranges = Lists.newArrayList();
    for (String value : values) {
      ranges.add(Ranges.of((Comparable) resolved.type().cast(value), "=="));
    }
    return ranges;
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

    InDimFilter that = (InDimFilter) o;

    if (values != null ? !values.equals(that.values) : that.values != null) {
      return false;
    }
    if (!dimension.equals(that.dimension)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = values != null ? values.hashCode() : 0;
    result = 31 * result + dimension.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    Collection<String> logging = values;
    if (values.size() > 10) {
      logging = GuavaUtils.concat(Iterables.limit(values, 10), String.format("..%d more", values.size() - 10));
    }
    return "InDimFilter{" +
           "dimension='" + dimension + '\'' +
           (extractionFn == null ? "" : ", extractionFn=" + extractionFn) +
           ", values=" + logging +
           '}';
  }

  @Override
  public DimFilter forLog()
  {
    if (values.size() > 100) {
      return new InDimFilter(
          dimension,
          extractionFn,
          GuavaUtils.concat(Iterables.limit(values, 100), String.format("..%d more", values.size() - 100))
      );
    }
    return this;
  }
}
