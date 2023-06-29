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
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ColumnStats;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.serde.ComplexMetrics;

import java.math.BigDecimal;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 */
public class GenericMaxAggregatorFactory extends GenericAggregatorFactory implements AggregatorFactory.Vectorizable
{
  public static GenericMaxAggregatorFactory of(String name, String fieldName)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, null, null, null);
  }

  public static GenericMaxAggregatorFactory of(String name, String fieldName, ValueDesc type)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, null, null, type);
  }

  public static GenericMaxAggregatorFactory ofLong(String name, String fieldName)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, null, null, ValueDesc.LONG);
  }

  public static GenericMaxAggregatorFactory ofDouble(String name, String fieldName)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, null, null, ValueDesc.DOUBLE);
  }

  public static GenericMaxAggregatorFactory expr(String name, String expression, ValueDesc inputType)
  {
    return new GenericMaxAggregatorFactory(name, null, expression, null, inputType);
  }

  private static final byte CACHE_TYPE_ID = 0x0C;

  @JsonCreator
  public GenericMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") ValueDesc inputType
  )
  {
    super(name, fieldName, fieldExpression, predicate, inputType);
    Preconditions.checkArgument(outputType == null || outputType.isNumeric(), "unsupported type %s", outputType);
  }

  public GenericMaxAggregatorFactory(String name, String fieldName, ValueDesc inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  protected String statKey()
  {
    return ColumnStats.MAX;
  }

  @Override
  protected Object evaluate(LongStream stream)
  {
    return stream.max();
  }

  @Override
  protected Object evaluate(DoubleStream stream)
  {
    return stream.max();
  }

  @Override
  protected final Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return FloatMaxAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleMaxAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongMaxAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if (valueType.isDecimal()) {
          return DecimalMaxAggregator.create(
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
        return FloatMaxBufferAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleMaxBufferAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongMaxBufferAggregator.create(
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
          return DecimalMaxBufferAggregator.create(
              ColumnSelectors.<BigDecimal>getObjectColumnSelector(
                  metricFactory,
                  fieldName,
                  fieldExpression
              ),
              ColumnSelectors.toMatcher(predicate, metricFactory),
              Preconditions.checkNotNull(decimalSerde, "unsupported type %s", valueType)
          );
        }
    }
    throw new IAE("unsupported type %s", valueType);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    switch (outputType.type()) {
      case FLOAT:
        return Byte.BYTES + Float.BYTES;
      case DOUBLE:
        return Byte.BYTES + Double.BYTES;
      case LONG:
        return Byte.BYTES + Long.BYTES;
      case COMPLEX:
        if (outputType.isDecimal()) {
          return 128;
        }
    }
    throw new IllegalStateException();
  }

  @Override
  public AggregatorFactory withPredicate(String predicate)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, fieldExpression, predicate, inputType);
  }

  @Override
  public AggregatorFactory withFieldExpression(String fieldExpression)
  {
    return new GenericMaxAggregatorFactory(name, null, fieldExpression, predicate, inputType);
  }

  @Override
  protected final AggregatorFactory withName(String name, String fieldName, ValueDesc inputType)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, inputType);
  }

  @Override
  protected final AggregatorFactory withExpression(String name, String fieldExpression, ValueDesc inputType)
  {
    return new GenericMaxAggregatorFactory(name, fieldName, fieldExpression, predicate, inputType);
  }

  @Override
  protected final byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  @SuppressWarnings("unchecked")
  public BinaryFn.Identical combiner()
  {
    switch (inputType.type()) {
      case FLOAT:
        return (param1, param2) -> ((Number) param1).floatValue() > ((Number) param2).floatValue() ? param1 : param2;
      case DOUBLE:
        return (param1, param2) -> ((Number) param1).doubleValue() > ((Number) param2).doubleValue() ? param1 : param2;
      case LONG:
        return (param1, param2) -> ((Number) param1).longValue() > ((Number) param2).longValue() ? param1 : param2;
    }
    return (param1, param2) -> comparator.compare(param1, param2) >= 0 ? param1 : param2;
  }

  @Override
  public String getCubeName()
  {
    return "max";
  }

  @Override
  public boolean supports(QueryableIndex index)
  {
    return isVectorizable(index);
  }

  @Override
  public Aggregator.Vectorized create(ColumnSelectorFactory factory)
  {
    switch (inputType.type()) {
      case LONG:
        return LongMaxAggregator.vectorize((LongColumnSelector.Scannable) factory.makeLongColumnSelector(fieldName));
      case FLOAT:
      case DOUBLE:
        return DoubleMaxAggregator.vectorize((DoubleColumnSelector.Scannable) factory.makeDoubleColumnSelector(fieldName));
      default:
        throw new ISE("%s", inputType);
    }
  }
}
