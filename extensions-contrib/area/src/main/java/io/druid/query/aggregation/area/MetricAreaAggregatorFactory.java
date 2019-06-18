/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query.aggregation.area;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MetricAreaAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x33;
  protected final String name;
  protected final String fieldName;

  @JsonCreator
  public MetricAreaAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new MetricAreaAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new MetricAreaBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return MetricAreaAggregator.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner()
    {
      @Override
      public Object combine(Object param1, Object param2)
      {
        return MetricAreaAggregator.combine(param1, param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new MetricAreaAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof MetricAreaAggregatorFactory) {
      return new MetricAreaAggregatorFactory(
          name,
          name
      );

    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return MetricArea.fromBytes((byte[])object);
    } else if (object instanceof ByteBuffer) {
      return MetricArea.fromBytes((ByteBuffer)object);
    } else if (object instanceof String) {
      return  MetricArea.fromBytes(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((MetricArea)object).getArea();
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.DOUBLE;
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

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length)
        .put(CACHE_TYPE_ID)
        .put(fieldNameBytes)
        .array();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("metricArea");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES + Ints.BYTES + Doubles.BYTES;
  }

}
