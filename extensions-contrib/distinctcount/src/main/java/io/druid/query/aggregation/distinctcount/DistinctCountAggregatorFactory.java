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

package io.druid.query.aggregation.distinctcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class DistinctCountAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x15;

  private final String name;
  private final String fieldName;
  private final String predicate;

  @JsonCreator
  public DistinctCountAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return new EmptyDistinctCountAggregator();
    } else {
      return new DistinctCountAggregator(
          selector,
          ColumnSelectors.toMatcher(predicate, columnFactory)
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return new EmptyDistinctCountBufferAggregator();
    } else {
      return new DistinctCountBufferAggregator(
          makeDimensionSelector(columnFactory), ColumnSelectors.toMatcher(predicate, columnFactory)
      );
    }
  }

  private DimensionSelector makeDimensionSelector(final ColumnSelectorFactory columnFactory)
  {
    return columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o, Object o1)
      {
        return Long.compare(((Number) o).longValue(), ((Number) o1).longValue());
      }
    };
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return (param1, param2) -> param1.longValue() + param2.longValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.LONG;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("distinctCount");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
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

    DistinctCountAggregatorFactory that = (DistinctCountAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DistinctCountAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           '}';
  }
}
