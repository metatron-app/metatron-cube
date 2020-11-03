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

package io.druid.query.aggregation.bloomfilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("bloomFilter")
public class BloomFilterAggregatorFactory extends HashAggregatorFactory
{
  public static BloomFilterAggregatorFactory of(String name, List<String> fieldNames, int maxNumEntries)
  {
    return new BloomFilterAggregatorFactory(
        name,
        fieldNames,
        null,
        GroupingSetSpec.EMPTY,
        null,
        true,
        maxNumEntries,
        false
    );
  }

  private static final byte CACHE_TYPE_ID = 0x25;

  private final int maxNumEntries;
  private final boolean combine;  // not supports cause seemed useless

  @JsonCreator
  public BloomFilterAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("groupingSets") final GroupingSetSpec groupingSets,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("maxNumEntries") final int maxNumEntries,
      @JsonProperty("combine") final boolean combine
  )
  {
    super(name, predicate, fieldNames, fields, groupingSets, byRow);
    this.maxNumEntries = maxNumEntries;
    this.combine = combine;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      throw new UnsupportedOperationException("combining aggregator");
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = new int[][]{};
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new BloomFilterAggregator(matcher, selectors, grouping, byRow, maxNumEntries);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      throw new UnsupportedOperationException("combining buffer aggregator");
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = new int[][]{};
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new BloomFilterBufferAggregator(matcher, selectors, grouping, byRow, maxNumEntries);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<BloomKFilter>()
    {
      @Override
      public int compare(BloomKFilter lhs, BloomKFilter rhs)
      {
        return Long.compare(lhs.getNumSetBits(), rhs.getNumSetBits());
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<BloomKFilter> combiner()
  {
    return new Combiner<BloomKFilter>()
    {
      @Override
      public BloomKFilter combine(BloomKFilter param1, BloomKFilter param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        return param1.merge(param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BloomFilterAggregatorFactory(name, Arrays.asList(name), null, null, null, false, maxNumEntries, true);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof BloomKFilter) {
      return object;
    }
    final byte[] buffer;
    if (object instanceof byte[]) {
      buffer = (byte[]) object;
    } else if (object instanceof String) {
      buffer = StringUtils.decodeBase64((String) object);
    } else {
      throw new ISE("?? %s", object.getClass().getSimpleName());
    }
    return BloomKFilter.deserialize(buffer);
  }

  @JsonProperty
  public int getMaxNumEntries()
  {
    return maxNumEntries;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.getCacheKey(builder.append(CACHE_TYPE_ID)).append(maxNumEntries);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("bloomFilter");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return BloomKFilter.computeSizeBytes(maxNumEntries);
  }

  @Override
  public boolean equals(Object o)
  {
    return super.equals(o) && maxNumEntries == ((BloomFilterAggregatorFactory) o).maxNumEntries;
  }

  @Override
  public int hashCode()
  {
    return 31 * super.hashCode() + maxNumEntries;
  }

  @Override
  public String toString()
  {
    return "BloomFilterAggregatorFactory{" +
           "name='" + name + '\'' +
           (fieldNames == null ? "" : ", fieldNames=" + fieldNames) +
           (fields == null ? "" : ", fields=" + fields) +
           (groupingSets == null ? "" : ", groupingSets=" + groupingSets) +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           ", byRow=" + byRow +
           ", maxNumEntries=" + maxNumEntries +
           '}';
  }
}
