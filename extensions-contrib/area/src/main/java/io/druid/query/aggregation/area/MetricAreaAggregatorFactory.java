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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("areaAgg")
public class MetricAreaAggregatorFactory extends AggregatorFactory
{
  public static final ValueDesc TYPE = ValueDesc.of("metricArea");

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
  public BinaryFn.Identical combiner()
  {
    return (param1, param2) -> MetricAreaAggregator.combine(param1, param2);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new MetricAreaAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return MetricArea.fromBytes((byte[])object);
    } else if (object instanceof ByteBuffer) {
      return MetricArea.fromBytes((ByteBuffer)object);
    } else if (object instanceof String) {
      return MetricArea.fromBytes(StringUtils.decodeBase64((String) object));
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object == null ? null : ((MetricArea)object).getArea();
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES + Integer.BYTES + Double.BYTES;
  }

}
