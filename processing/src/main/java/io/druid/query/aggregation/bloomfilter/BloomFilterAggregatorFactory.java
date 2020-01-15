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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import org.apache.commons.codec.binary.Base64;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("bloomFilter")
public class BloomFilterAggregatorFactory extends AggregatorFactory
{
  public static BloomFilterAggregatorFactory of(String name, List<String> fieldNames, int maxNumEntries)
  {
    return new BloomFilterAggregatorFactory(name, fieldNames, null, null, null, true, maxNumEntries, false);
  }

  private static final byte CACHE_TYPE_ID = 0x25;

  private final String name;
  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final GroupingSetSpec groupingSets;
  private final String predicate;
  private final boolean byRow;
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
    this.name = name;
    this.predicate = predicate;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.byRow = byRow;
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
    return BloomFilterBufferAggregator.iterator(selectors, matcher, grouping, byRow, maxNumEntries);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<BloomKFilter>()
    {
      @Override
      public int compare(BloomKFilter lhs, BloomKFilter rhs)
      {
        return Longs.compare(lhs.getNumSetBits(), rhs.getNumSetBits());
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
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    throw new UnsupportedOperationException("can't merge BloomFilterAggregatorFactory");
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
      buffer = Base64.decodeBase64(StringUtils.toUtf8((String) object));
    } else {
      throw new ISE("?? %s", object.getClass().getSimpleName());
    }
    return BloomKFilter.deserialize(buffer);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    List<String> required = Lists.newArrayList();
    if (fieldNames != null) {
      required.addAll(fieldNames);
    } else {
      required.addAll(DimensionSpecs.toInputNames(fields));
    }
    return required;
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
  public boolean isByRow()
  {
    return byRow;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
  }

  @JsonProperty
  public int getMaxNumEntries()
  {
    return maxNumEntries;
  }

  @Override
  public byte[] getCacheKey()
  {
    return KeyBuilder.get()
                     .append(CACHE_TYPE_ID)
                     .append(fieldNames)
                     .append(fields)
                     .append(groupingSets)
                     .append(predicate)
                     .append(byRow)
                     .append(maxNumEntries)
                     .build();
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BloomFilterAggregatorFactory that = (BloomFilterAggregatorFactory) o;

    if (byRow != that.byRow) {
      return false;
    }
    if (maxNumEntries != that.maxNumEntries) {
      return false;
    }
    if (!Objects.equals(fieldNames, that.fieldNames)) {
      return false;
    }
    if (!Objects.equals(fields, that.fields)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }
    if (!Objects.equals(groupingSets, that.groupingSets)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (fields != null ? fields.hashCode() : 0);
    result = 31 * result + Objects.hashCode(groupingSets);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + (byRow ? 1 : 0);
    result = 31 * result + maxNumEntries;
    return result;
  }

  @Override
  public String toString()
  {
    return "BloomFilterAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames='" + fieldNames + '\'' +
           ", fields=" + fields +
           ", groupingSets=" + groupingSets +
           ", predicate='" + predicate + '\'' +
           ", byRow=" + byRow +
           ", maxNumEntries=" + maxNumEntries +
           '}';
  }
}
