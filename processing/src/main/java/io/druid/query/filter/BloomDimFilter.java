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
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.aggregation.bloomfilter.BloomKFilter;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class BloomDimFilter implements DimFilter.ValueOnly, DimFilter.LogProvider
{
  public static BloomDimFilter of(List<String> fieldNames, BloomKFilter filter)
  {
    return new BloomDimFilter(fieldNames, null, GroupingSetSpec.EMPTY, filter.serialize());
  }

  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final GroupingSetSpec groupingSets;
  private final byte[] bloomFilter;
  private final HashCode hash;

  @JsonCreator
  public BloomDimFilter(
      @JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("fields") List<DimensionSpec> fields,
      @JsonProperty("groupingSets") GroupingSetSpec groupingSets,
      @JsonProperty("bloomFilter") byte[] bloomFilter
  )
  {
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.bloomFilter = Preconditions.checkNotNull(bloomFilter);
    this.hash = Hashing.sha512().hashBytes(bloomFilter);
  }

  @Override
  public byte[] getCacheKey()
  {
    return KeyBuilder.get()
                     .append(DimFilterCacheHelper.BLOOM_CACHE_ID)
                     .append(fieldNames)
                     .append(fields)
                     .append(groupingSets)
                     .append(hash.asBytes())
                     .build();
  }


  @Override
  public DimFilter optimize()
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
    if (fieldNames != null) {
      handler.addAll(fieldNames);
    } else {
      handler.addAll(DimensionSpecs.toInputNames(fields));
    }
  }

  @Override
  public Filter.ValueOnly toFilter(TypeResolver resolver)
  {
    // todo support bitmap for single dimension case
    return new Filter.ValueOnly()
    {
      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnFactory)
      {
        List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
        List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

        int[][] grouping = new int[][]{};
        if (groupingSets != null) {
          grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
        }
        final HashAggregator<BloomTest> aggregator = new HashAggregator<BloomTest>(selectors, grouping);
        return new ValueMatcher()
        {
          final BloomTest tester = new BloomTest(BloomKFilter.deserialize(bloomFilter));

          @Override
          public boolean matches()
          {
            return aggregator.aggregate(tester).status;
          }
        };
      }
    };
  }

  @Override
  public DimFilter forLog()
  {
    return new BloomDimFilter(fieldNames, fields, groupingSets, StringUtils.EMPTY_BYTES);
  }

  private static class BloomTest implements HashCollector
  {
    private final BloomKFilter filter;
    private boolean status;

    private BloomTest(BloomKFilter filter) {this.filter = filter;}

    @Override
    public void collect(Object[] values, BytesRef bytes)
    {
      status = filter.test(values, bytes);
    }
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSpec> getFields()
  {
    return fields;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
  }

  @JsonProperty
  public byte[] getBloomFilter()
  {
    return bloomFilter;
  }

  @Override
  public String toString()
  {
    return "BloomFilter{" +
           ", fieldNames=" + fieldNames +
           ", fields=" + fields +
           ", groupingSets=" + groupingSets +
           '}';
  }
}
