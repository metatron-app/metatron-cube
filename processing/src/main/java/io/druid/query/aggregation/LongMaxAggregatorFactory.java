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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.LongColumnSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class LongMaxAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0xA;

  private final String name;
  private final String fieldName;
  private final String fieldExpression;

  @JsonCreator
  public LongMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ fieldExpression == null,
        "Must have a valid, non-null fieldName or fieldExpression");

    this.name = name;
    this.fieldName = fieldName;
    this.fieldExpression = fieldExpression;
  }

  public LongMaxAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return LongMaxAggregator.create(getLongColumnSelector(metricFactory), null);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return LongMaxBufferAggregator.create(getLongColumnSelector(metricFactory), null);
  }

  private LongColumnSelector getLongColumnSelector(ColumnSelectorFactory metricFactory)
  {
    return ColumnSelectors.getLongColumnSelector(metricFactory, fieldName, fieldExpression);
  }

  @Override
  public Comparator getComparator()
  {
    return LongMaxAggregator.COMPARATOR;
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
        return Math.max(param1.longValue(), param2.longValue());
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongMaxAggregatorFactory(name, name, null);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public String getCubeName()
  {
    return "longMax";
  }

  @Override
  public String getPredicate()
  {
    return null;
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new LongMaxAggregatorFactory(name, inputField, null);
  }

  @JsonProperty
  public String getFieldExpression()
  {
    return fieldExpression;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null ? Arrays.asList(fieldName) : Parser.findRequiredBindings(fieldExpression);
  }

  @Override
  public byte[] getCacheKey()
  {
    return KeyBuilder.get()
                     .append(CACHE_TYPE_ID)
                     .append(fieldName, fieldExpression)
                     .build();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Byte.BYTES + Longs.BYTES;
  }

  @Override
  public String toString()
  {
    return "LongMaxAggregatorFactory{" +
           "name='" + name + '\'' +
           (fieldName == null ? "": ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "": ", fieldExpression='" + fieldExpression + '\'') +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LongMaxAggregatorFactory that = (LongMaxAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(fieldExpression, that.fieldExpression)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (fieldExpression != null ? fieldExpression.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
