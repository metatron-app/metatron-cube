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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.ValueType;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("variance")
public class VarianceAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x4;

  private final String fieldName;
  private final String name;

  @JsonCreator
  public VarianceAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public String getTypeName()
  {
    return "variance";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES + Doubles.BYTES + Doubles.BYTES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ValueType valueType = metricFactory.columnType(fieldName);
    if (valueType == null) {
      return Aggregators.noopAggregator();
    }
    if (valueType == ValueType.FLOAT) {
      return new VarianceAggregator.FloatInput(
          name,
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    }
    if (valueType == ValueType.LONG) {
      return new VarianceAggregator.LongInput(
          name,
          metricFactory.makeLongColumnSelector(fieldName)
      );
    }
    return new VarianceAggregator.ObjectInput(
        name,
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ValueType valueType = metricFactory.columnType(fieldName);
    if (valueType == null) {
      return Aggregators.noopBufferAggregator();
    }
    if (valueType == ValueType.FLOAT) {
      return new VarianceBufferAggregator.FloatInput(
          name,
          metricFactory.makeFloatColumnSelector(fieldName)
      );
    }
    if (valueType == ValueType.LONG) {
      return new VarianceBufferAggregator.LongInput(
          name,
          metricFactory.makeLongColumnSelector(fieldName)
      );
    }
    return new VarianceBufferAggregator.ObjectInput(
        name,
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new VarianceAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new VarianceAggregatorFactory(fieldName, fieldName));
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
  public Comparator getComparator()
  {
    return VarianceHolder.COMPARATOR;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return new VarianceHolder();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return VarianceHolder.combineValues(lhs, rhs);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((VarianceHolder) object).getVariance();
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return VarianceHolder.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return VarianceHolder.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return VarianceHolder.from(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
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

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
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

    VarianceAggregatorFactory that = (VarianceAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
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
    return Objects.hash(fieldName, name);
  }
}
