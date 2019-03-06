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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DoubleColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("avg")
public class AverageAggregatorFactory extends AggregatorFactory
{
  private static final byte[] CACHE_KEY = new byte[]{0x0F};
  private final String name;
  private final String fieldName;
  private final String predicate;

  @JsonCreator
  public AverageAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");

    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new AverageAggregator(
        ColumnSelectors.toMatcher(predicate, metricFactory),
        getDoubleColumnSelector(metricFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new AverageBufferAggregator(
        ColumnSelectors.toMatcher(predicate, metricFactory),
        getDoubleColumnSelector(metricFactory)
    );
  }

  private DoubleColumnSelector getDoubleColumnSelector(ColumnSelectorFactory metricFactory)
  {
    return ColumnSelectors.getDoubleColumnSelector(metricFactory, fieldName, null);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new AverageAggregatorFactory(name, name, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<long[]> combiner()
  {
    return new Combiner<long[]>()
    {
      @Override
      public long[] combine(long[] param1, long[] param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        return new long[]{
            param1[0] + param2[0],
            Double.doubleToLongBits(
                Double.longBitsToDouble(param1[1]) + Double.longBitsToDouble(param2[1])
            )
        };
      }
    };
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<long[]>()
    {
      @Override
      public int compare(final long[] param1, final long[] param2)
      {
        if (param1 == null || param1[0] == 0L) {
          if (param2 == null || param2[0] == 0L) {
            return 0;
          }
          return -1;
        }
        if (param2 == null || param2[0] == 0L) {
          return 1;
        }
        return Double.compare(
            Double.longBitsToDouble(param1[1]) / param1[0],
            Double.longBitsToDouble(param2[1]) / param2[0]
        );
      }
    };
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof long[]) {
      return object;
    }
    if (object instanceof List) {
      return new long[]{Rows.parseLong(((List) object).get(0)), Rows.parseLong(((List) object).get(1))};
    }
    ByteBuffer buffer;
    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      buffer = (ByteBuffer) object;
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
    return new long[]{buffer.getLong(), buffer.getLong()};
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
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
    return ImmutableList.of(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length)
                     .put(CACHE_KEY)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public ValueDesc getInputType()
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.LONG_ARRAY;
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object == null) {
      return null;
    } else if (object instanceof long[]) {
      long[] param = (long[]) object;
      return param[0] == 0 ? null : Double.longBitsToDouble(param[1]) / param[0];
    } else {
      List param = (List) object;
      long p1 = Rows.parseLong(param.get(0));
      long p2 = Rows.parseLong(param.get(1));
      return p1 == 0 ? null : Double.longBitsToDouble(p2) / p1;
    }
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + Double.BYTES;
  }

  @Override
  public String toString()
  {
    return "AverageAggregatorFactory{" +
           "name='" + name + '\'' +
           "fieldName='" + fieldName + '\'' +
           (predicate == null ? "": ", predicate='" + predicate + '\'') +
           '}';
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

    AverageAggregatorFactory that = (AverageAggregatorFactory) o;

    if (!(Objects.equals(name, that.name))) {
      return false;
    }
    if (!(Objects.equals(fieldName, that.fieldName))) {
      return false;
    }
    if (!(Objects.equals(predicate, that.predicate))) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, predicate);
  }
}
