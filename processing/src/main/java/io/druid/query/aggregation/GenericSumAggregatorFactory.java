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
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.serde.ComplexMetrics;

import java.math.BigDecimal;

/**
 */
public class GenericSumAggregatorFactory extends GenericAggregatorFactory
{
  public static GenericSumAggregatorFactory expr(String name, String expression, String inputType)
  {
    return new GenericSumAggregatorFactory(name, null, expression, null, inputType);
  }

  private static final byte CACHE_TYPE_ID = 0x0D;

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
    Preconditions.checkArgument(outputType == null || outputType.isNumeric(), "unsupported type " + outputType);
  }

  public GenericSumAggregatorFactory(String name, String fieldName, String inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  protected ValueDesc toOutputType(ValueDesc inputType)
  {
    ValueDesc elementType = super.toOutputType(inputType);
    return elementType.type() == ValueType.FLOAT ? ValueDesc.DOUBLE : elementType;
  }

  @Override
  protected final Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return DoubleSumAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleSumAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongSumAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if (valueType.isDecimal()) {
          return DecimalSumAggregator.create(
              ColumnSelectors.<BigDecimal>getObjectColumnSelector(
                  metricFactory,
                  fieldName,
                  fieldExpression
              ),
              ColumnSelectors.toMatcher(predicate, metricFactory)
          );
        }
    }
    throw new IllegalArgumentException("unsupported type " + valueType);
  }

  @Override
  protected final BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return DoubleSumBufferAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleSumBufferAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongSumBufferAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if (valueType.isDecimal()) {
          DecimalMetricSerde decimalSerde = (DecimalMetricSerde) ComplexMetrics.getSerdeForType(valueType);
          return DecimalSumBufferAggregator.create(
              ColumnSelectors.<BigDecimal>getObjectColumnSelector(
                  metricFactory,
                  fieldName,
                  fieldExpression
              ),
              ColumnSelectors.toMatcher(predicate, metricFactory),
              Preconditions.checkNotNull(decimalSerde, "unsupported type " + valueType)
          );
        }
    }
    throw new IllegalArgumentException("unsupported type " + valueType);
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
  public final Object deserialize(Object object)
  {
    if (object instanceof String && outputType.isDecimal()) {
      return new BigDecimal((String) object);
    }
    return super.deserialize(object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public final Combiner combiner()
  {
    switch (outputType.type()) {
      case FLOAT:
      case DOUBLE:
        return new Combiner<Number>()
        {
          @Override
          public Number combine(Number param1, Number param2)
          {
            return param1.doubleValue() + param2.doubleValue();
          }
        };
      case LONG:
        return new Combiner<Number>()
        {
          @Override
          public Number combine(Number param1, Number param2)
          {
            return param1.longValue() + param2.longValue();
          }
        };
      case COMPLEX:
        return new Combiner<BigDecimal>()
        {
          @Override
          public BigDecimal combine(BigDecimal param1, BigDecimal param2)
          {
            return param1.add(param2);
          }
        };
    }
    throw new IllegalStateException();
  }
}
