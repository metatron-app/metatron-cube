/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.distinctcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class DistinctCountAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 20;
  private static final BitMapFactory DEFAULT_BITMAP_FACTORY = new RoaringBitMapFactory();

  private final String name;
  private final String fieldName;
  private final String predicate;
  private final BitMapFactory bitMapFactory;

  @JsonCreator
  public DistinctCountAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("bitmapFactory") BitMapFactory bitMapFactory
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
    this.bitMapFactory = bitMapFactory == null ? DEFAULT_BITMAP_FACTORY : bitMapFactory;
  }

  public DistinctCountAggregatorFactory(String name, String fieldName, BitMapFactory bitMapFactory)
  {
    this(name, fieldName, null, bitMapFactory);
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
          bitMapFactory.makeEmptyMutableBitmap(),
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
        return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<Number> combiner()
  {
    return new Combiner<Number>()
    {
      @Override
      public Number combine(Number param1, Number param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        return param1.longValue() + param2.longValue();
      }
    };
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

  @JsonProperty("bitmapFactory")
  public BitMapFactory getBitMapFactory()
  {
    return bitMapFactory;
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
    List<String> required = Lists.newArrayList();
    required.add(fieldName);
    if (predicate != null) {
      required.addAll(Parser.findRequiredBindings(predicate));
    }
    return required;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] bitMapFactoryCacheKey = StringUtils.toUtf8(bitMapFactory.toString());
    return ByteBuffer.allocate(2 + fieldNameBytes.length + bitMapFactoryCacheKey.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(bitMapFactoryCacheKey)
                     .array();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("distinctCount");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
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
           '}';
  }
}
