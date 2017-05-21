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
import io.druid.segment.ColumnSelectors;

/**
 */
public class GenericSumAggregatorFactory extends GenericAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x0A;

  @JsonCreator
  public GenericSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") String inputType
  )
  {
    super(name, fieldName, fieldExpression, predicate, inputType);
  }

  public GenericSumAggregatorFactory(String name, String fieldName, String inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  protected final Aggregator factorize(ColumnSelectorFactory metricFactory, ValueType valueType)
  {
    switch (valueType) {
      case FLOAT:
        return DoubleSumAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleSumAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case LONG:
        return LongSumAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      default:
        throw new IllegalArgumentException();
    }
  }

  protected final BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueType valueType)
  {
    switch (valueType) {
      case FLOAT:
        return DoubleSumBufferAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleSumBufferAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case LONG:
        return LongSumBufferAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
    }
    throw new IllegalStateException();
  }

  @Override
  public String getTypeName()
  {
    return valueType == ValueType.FLOAT ? ValueType.DOUBLE.name() : super.getTypeName();
  }

  @Override
  protected final AggregatorFactory withValue(String name, String fieldName, String inputType)
  {
    return new GenericSumAggregatorFactory(name, fieldName, inputType);
  }

  @Override
  protected final byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public final Object getAggregatorStartValue()
  {
    return 0D;
  }

  @Override
  public final Object combine(Object lhs, Object rhs)
  {
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
      case LONG:
        return ((Number) lhs).longValue() + ((Number) rhs).longValue();
    }
    throw new IllegalStateException();
  }
}
