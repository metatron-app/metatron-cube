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
import com.fasterxml.jackson.annotation.JsonTypeName;
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
import io.druid.java.util.common.ISE;
import io.druid.query.Query;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.filter.DimFilter.Compressible;
import io.druid.query.filter.DimFilter.IndexedIDSupport;
import io.druid.query.filter.DimFilter.LogProvider;
import io.druid.query.filter.DimFilter.Mergeable;
import io.druid.query.filter.DimFilter.RangeFilter;
import io.druid.query.filter.DimFilter.SingleInput;
import io.druid.segment.filter.InFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName("in")
public class InDimFilter extends SingleInput
    implements RangeFilter, LogProvider, Compressible, Mergeable, IndexedIDSupport
{
  public static InDimFilter of(String dimension, String... values)
  {
    return new InDimFilter(dimension, Arrays.asList(values), null, null);
  }

  private final String dimension;
  private final ExtractionFn extractionFn;
  private final List<String> values;
  private final byte[] hash;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("values") Collection<String> values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn,
      @JsonProperty("hash") byte[] hash
  )
  {
    this(
        dimension,
        extractionFn,
        ImmutableList.copyOf(ImmutableSortedSet.copyOf(Iterables.transform(values, s -> Strings.nullToEmpty(s)))),
        hash
    );
  }

  public InDimFilter(String dimension, Collection<String> values, ExtractionFn extractionFn)
  {
    this(dimension, values, extractionFn, null);
  }

  // values should be sorted
  public InDimFilter(String dimension, ExtractionFn extractionFn, List<String> values, byte[] hash)
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension can not be null");
    this.extractionFn = extractionFn;
    this.values = Preconditions.checkNotNull(values, "values can not be null");
    this.hash = hash;
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
    return new InDimFilter(dimension, extractionFn, values, hash);
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public byte[] getHash()
  {
    return hash;
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
  public DimFilter optimize()
  {
    InDimFilter optimized = optimizeLookup();
    if (optimized.values.size() == 1) {
      return new SelectorDimFilter(optimized.dimension, optimized.values.get(0), optimized.getExtractionFn());
    }
    return optimized;
  }

  private InDimFilter optimizeLookup()
  {
    List<String> rewritten = ExtractionFns.reverseMap(extractionFn, values);
    return rewritten == null ? this : new InDimFilter(dimension, rewritten, null);
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new InFilter(this, dimension, values, extractionFn);
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
    ValueDesc resolved = resolver.resolve(dimension, ValueDesc.STRING).unwrapDimension();
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

    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!Objects.equals(extractionFn, that.extractionFn)) {
      return false;
    }
    if (values.size() != that.values.size()) {
      return false;
    }
    return Objects.equals(values, that.values);
  }

  private static final int THRESHOLD = 10;

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, extractionFn, values.size(), GuavaUtils.sublist(values, THRESHOLD));
  }

  @Override
  public String toString()
  {
    List<String> logging = values;
    if (logging.size() > THRESHOLD) {
      logging = GuavaUtils.concat(
          GuavaUtils.sublist(logging, THRESHOLD), String.format("..%d more", logging.size() - THRESHOLD)
      );
    }
    return "InDimFilter{" +
           "dimension='" + dimension + '\'' +
           (extractionFn == null ? "" : ", extractionFn=" + extractionFn) +
           ", values=" + logging +
           '}';
  }

  @Override
  public boolean isHeavy()
  {
    return values.size() > THRESHOLD;
  }

  @Override
  public DimFilter forLog()
  {
    if (values.size() > THRESHOLD) {
      return new InDimFilter(
          dimension,
          extractionFn,
          GuavaUtils.concat(Iterables.limit(values, THRESHOLD), String.format("..%d more", values.size() - THRESHOLD)),
          null
      );
    }
    return this;
  }

  @Override
  public DimFilter compress(Query parent)
  {
    return CompressedInFilter.build(this);
  }

  @Override
  public boolean supports(OP op, DimFilter other)
  {
    if (other instanceof InDimFilter) {
      InDimFilter in = (InDimFilter) other;
      return dimension.equals(in.dimension) && Objects.equals(extractionFn, in.extractionFn);
    } else if (other instanceof SelectorDimFilter) {
      SelectorDimFilter select = (SelectorDimFilter) other;
      return dimension.equals(select.getDimension()) && Objects.equals(extractionFn, select.getExtractionFn());
    }
    return false;
  }

  @Override
  public DimFilter merge(OP op, DimFilter other)
  {
    if (other instanceof InDimFilter) {
      InDimFilter in = (InDimFilter) other;
      switch (op) {
        case AND:
          List<String> intersection = GuavaUtils.intersection(values, in.values);
          return intersection.isEmpty() ? DimFilters.NONE :
                 intersection.size() == 1 ? new SelectorDimFilter(dimension, intersection.get(0), extractionFn) :
                 new InDimFilter(dimension, extractionFn, intersection, null);
        case OR:
          return new InDimFilter(dimension, extractionFn, GuavaUtils.union(values, in.values), null);
      }
    } else if (other instanceof SelectorDimFilter) {
      SelectorDimFilter select = (SelectorDimFilter) other;
      int index = Collections.binarySearch(values, select.getValue());
      switch (op) {
        case AND:
          return index >= 0 ? other : DimFilters.NONE;
        case OR:
          if (index >= 0) {
            return this;
          }
          List<String> copy = Lists.newArrayList(values);
          copy.add(-index - 1, select.getValue());
          return new InDimFilter(dimension, extractionFn, copy, null);
      }
    }
    throw new ISE("merge?? %s %s %s", this, op, other);
  }
}
