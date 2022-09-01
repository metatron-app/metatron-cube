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

package io.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.collections.IntList;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

// dummy aggregator factory for collecting dimension values
public class DimensionAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final String columnName;
  private final MultiValueHandling multiValueHandling;

  @JsonCreator
  public DimensionAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("extractHints") MultiValueHandling multiValueHandling
  )
  {
    this.name = Preconditions.checkNotNull(name == null ? columnName : name);
    this.columnName = Preconditions.checkNotNull(columnName == null ? name : columnName);
    this.multiValueHandling = multiValueHandling;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException("factorize");
  }

  public Aggregator factorize(IncrementalIndex.DimDim dimension, ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(columnName);
    return new Aggregator.Estimable<IntList>()
    {
      @Override
      public int estimateOccupation(IntList current)
      {
        return current == null ? Integer.BYTES : (1 + current.size()) * Integer.BYTES;
      }

      @Override
      @SuppressWarnings("unchecked")
      public IntList aggregate(IntList current)
      {
        final int x = dimension.add(Objects.toString(selector.get(), ""));
        if (current == null) {
          current = new IntList();
        }
        current.add(x);
        return current;
      }

      @Override
      public IntList get(IntList current)
      {
        return current;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException("factorizeBuffered");
  }

  @Override
  public boolean providesEstimation()
  {
    return true;
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException("getComparator");
  }

  @Override
  public BinaryFn.Identical<IntList> combiner()
  {
    return (param1, param2) -> param1.concat(param2);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DimensionAggregatorFactory(name, name, multiValueHandling);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public ValueType getValueType()
  {
    return ValueType.STRING;    // todo
  }

  @JsonProperty
  public MultiValueHandling getMultiValueHandling()
  {
    return multiValueHandling;
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.DIM_STRING;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(columnName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    throw new UnsupportedOperationException("getCacheKey");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    throw new UnsupportedOperationException("getMaxIntermediateSize");
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

    DimensionAggregatorFactory that = (DimensionAggregatorFactory) o;

    if (!columnName.equals(that.columnName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return Objects.equals(multiValueHandling, that.multiValueHandling);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, columnName, multiValueHandling);
  }

  @Override
  public String toString()
  {
    return "DimensionAggregatorFactory{" +
           "name='" + name + '\'' +
           ", columnName='" + columnName + '\'' +
           (multiValueHandling == null ? "" : ", multiValueHandling='" + multiValueHandling + '\'') +
           '}';
  }
}
