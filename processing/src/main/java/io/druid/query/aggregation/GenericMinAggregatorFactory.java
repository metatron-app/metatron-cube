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
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.serde.ComplexMetrics;

import java.math.BigDecimal;

/**
 */
public class GenericMinAggregatorFactory extends GenericAggregatorFactory
{
  public static GenericMinAggregatorFactory of(String name, String fieldName)
  {
    return new GenericMinAggregatorFactory(name, fieldName, null, null, null);
  }

  public static GenericMinAggregatorFactory ofLong(String name, String fieldName)
  {
    return new GenericMinAggregatorFactory(name, fieldName, null, null, ValueDesc.LONG);
  }

  public static GenericMinAggregatorFactory ofDouble(String name, String fieldName)
  {
    return new GenericMinAggregatorFactory(name, fieldName, null, null, ValueDesc.DOUBLE);
  }

  private static final byte CACHE_TYPE_ID = 0x0E;

  @JsonCreator
  public GenericMinAggregatorFactory(
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

  public GenericMinAggregatorFactory(String name, String fieldName, ValueDesc inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  protected final Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return FloatMinAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleMinAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongMinAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if (valueType.isDecimal()) {
          return DecimalMinAggregator.create(
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
        return FloatMinBufferAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return DoubleMinBufferAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return LongMinBufferAggregator.create(
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
          return DecimalMinBufferAggregator.create(
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
  protected final AggregatorFactory withName(String name, String fieldName, ValueDesc inputType)
  {
    return new GenericMinAggregatorFactory(name, fieldName, inputType);
  }

  @Override
  protected AggregatorFactory withExpression(String name, String fieldExpression, ValueDesc inputType)
  {
    return new GenericMinAggregatorFactory(name, fieldName, fieldExpression, predicate, inputType);
  }

  @Override
  protected final byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  @SuppressWarnings("unchecked")
  public final Combiner combiner()
  {
    return new Combiner.Abstract<Object>()
    {
      @Override
      protected Object _combine(Object lhs, Object rhs)
      {
        return comparator.compare(lhs, rhs) < 0 ? lhs : rhs;
      }
    };
  }

  @Override
  public String getCubeName()
  {
    return "min";
  }
}
