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

package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Doubles;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MetricRangeAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0x32;
  protected final String name;
  protected final String fieldName;

  @JsonCreator
  public MetricRangeAggregatorFactory(
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
    return new MetricRangeAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new MetricRangeBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return MetricRangeAggregator.COMPARATOR;
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
        return MetricRangeAggregator.combine(param1, param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new MetricRangeAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return MetricRange.fromBytes((byte[])object);
    } else if (object instanceof ByteBuffer) {
      return MetricRange.fromBytes((ByteBuffer)object);
    } else if (object instanceof String) {
      return  MetricRange.fromBytes(StringUtils.decodeBase64((String) object));
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object == null ? null : ((MetricRange)object).getRange();
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

  @Override
  public String getCubeName()
  {
    return "rangeAgg";
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
    return new MetricRangeAggregatorFactory(name, inputField);
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
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
    return ValueDesc.of("metricRange");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES * 2;
  }

}
