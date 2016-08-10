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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.ValueType;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public class GenericMinAggregatorFactory extends GenericAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x0B;

  @JsonCreator
  public GenericMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("inputType") String inputType
  )
  {
    super(name, fieldName, inputType);
  }

  @Override
  protected final Aggregator factorize(ColumnSelectorFactory metricFactory, ValueType valueType)
  {
    switch (valueType) {
      case FLOAT:
        return new DoubleMinAggregator.FloatInput(name, metricFactory.makeFloatColumnSelector(fieldName));
      case DOUBLE:
        return new DoubleMinAggregator.DoubleInput(name, metricFactory.makeDoubleColumnSelector(fieldName));
      case LONG:
        return new LongMinAggregator(name, metricFactory.makeLongColumnSelector(fieldName));
    }
    throw new IllegalStateException();
  }

  @Override
  protected final BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueType valueType)
  {
    switch (valueType) {
      case FLOAT:
        return new DoubleMinBufferAggregator.FloatInput(metricFactory.makeFloatColumnSelector(fieldName));
      case DOUBLE:
        return new DoubleMinBufferAggregator.DoubleInput(metricFactory.makeDoubleColumnSelector(fieldName));
      case LONG:
        return new LongMinBufferAggregator(metricFactory.makeLongColumnSelector(fieldName));
    }
    throw new IllegalStateException();
  }

  @Override
  protected final AggregatorFactory withValue(String name, String fieldName, String inputType)
  {
    return new GenericMinAggregatorFactory(name, fieldName, inputType);
  }

  @Override
  protected final byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public final Object getAggregatorStartValue()
  {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public final Object combine(Object lhs, Object rhs)
  {
    return comparator.compare(lhs, rhs) < 0 ? lhs : rhs;
  }
}
