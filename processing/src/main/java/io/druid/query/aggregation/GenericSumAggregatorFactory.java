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
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.GenericColumn.DoubleType;
import io.druid.segment.column.GenericColumn.FloatType;
import io.druid.segment.column.GenericColumn.LongType;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.serde.ComplexMetrics;

import java.math.BigDecimal;

/**
 */
public class GenericSumAggregatorFactory extends GenericAggregatorFactory
{
  public static GenericSumAggregatorFactory of(String name, String fieldName)
  {
    return new GenericSumAggregatorFactory(name, fieldName, null, null, null);
  }

  public static GenericSumAggregatorFactory of(String name, String fieldName, ValueDesc type)
  {
    return new GenericSumAggregatorFactory(name, fieldName, null, null, type);
  }

  public static GenericSumAggregatorFactory ofLong(String name, String fieldName)
  {
    return new GenericSumAggregatorFactory(name, fieldName, null, null, ValueDesc.LONG);
  }

  public static GenericSumAggregatorFactory ofDouble(String name, String fieldName)
  {
    return new GenericSumAggregatorFactory(name, fieldName, null, null, ValueDesc.DOUBLE);
  }

  public static GenericSumAggregatorFactory expr(String name, String expression, ValueDesc inputType)
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
      @JsonProperty("inputType") ValueDesc inputType
  )
  {
    super(name, fieldName, fieldExpression, predicate, inputType);
    Preconditions.checkArgument(outputType == null || outputType.isNumeric(), "unsupported type %s", outputType);
  }

  public GenericSumAggregatorFactory(String name, String fieldName, ValueDesc inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  public AggregatorFactory evaluate(FilterContext context)
  {
    if (fieldName != null && inputType.isPrimitiveNumeric()) {
      BitmapIndexSelector selector = context.indexSelector();
      Column column = selector.getColumn(fieldName);
      if (column == null || !column.hasGenericColumn()) {
        return AggregatorFactory.constant(this, inputType.type().cast(0L));
      }
      GenericColumn generic = column.getGenericColumn();
      if (generic instanceof LongType) {
        return AggregatorFactory.constant(this, ((LongType) generic).stream(context.rowIterator()).sum());
      } else if (generic instanceof FloatType) {
        return AggregatorFactory.constant(this, ((FloatType) generic).stream(context.rowIterator()).sum());
      } else if (generic instanceof DoubleType) {
        return AggregatorFactory.constant(this, ((DoubleType) generic).stream(context.rowIterator()).sum());
      }
    }
    return this;
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
              Preconditions.checkNotNull(decimalSerde, "unsupported type %s", valueType)
          );
        }
    }
    throw new IAE("unsupported type %s", valueType);
  }

  @Override
  public AggregatorFactory withPredicate(String predicate)
  {
    return new GenericSumAggregatorFactory(name, fieldName, fieldExpression, predicate, inputType);
  }

  @Override
  public AggregatorFactory withFieldExpression(String fieldExpression)
  {
    return new GenericSumAggregatorFactory(name, null, fieldExpression, predicate, inputType);
  }

  @Override
  protected final AggregatorFactory withName(String name, String fieldName, ValueDesc inputType)
  {
    return new GenericSumAggregatorFactory(name, fieldName, inputType);
  }

  @Override
  protected AggregatorFactory withExpression(String name, String fieldExpression, ValueDesc inputType)
  {
    return new GenericSumAggregatorFactory(name, fieldName, fieldExpression, predicate, inputType);
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
      return Rows.parseDecimal((String) object);
    }
    return super.deserialize(object);
  }

  @Override
  public final BinaryFn.Identical combiner()
  {
    switch (outputType.type()) {
      case FLOAT:
      case DOUBLE:
        return DoubleSumAggregator.COMBINER;
      case LONG:
        return LongSumAggregator.COMBINER;
      case COMPLEX:
        if (outputType.isDecimal()) {
          return new BinaryFn.Identical<BigDecimal>()
          {
            @Override
            public BigDecimal apply(BigDecimal arg1, BigDecimal arg2)
            {
              return arg1.add(arg2);
            }
          };
        }
    }
    throw new IllegalStateException();
  }

  @Override
  public String getCubeName()
  {
    return "sum";
  }
}
