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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.metamx.common.StringUtils;
import io.druid.data.ValueType;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class GenericSumAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x3;

  private final String fieldName;
  private final String name;
  private final String fieldExpression;
  private final String predicate;
  private final ValueType inputType;

  @JsonCreator
  public GenericSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") ValueType inputType
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ fieldExpression == null,
        "Must have a valid, non-null fieldName or fieldExpression"
    );

    this.name = name;
    this.fieldName = fieldName;
    this.fieldExpression = fieldExpression;
    this.predicate = predicate;
    this.inputType = inputType == null ? ValueType.DOUBLE : inputType;
    Preconditions.checkArgument(ValueType.isNumeric(inputType));
  }

  public GenericSumAggregatorFactory(String name, String fieldName, ValueType inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    switch (inputType) {
      case FLOAT:
        return new DoubleSumAggregator.FloatInput(
            name,
            AggregatorUtil.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
      case DOUBLE:
        return new DoubleSumAggregator.DoubleInput(
            name,
            AggregatorUtil.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
      case LONG:
        return new LongSumAggregator(
            name,
            AggregatorUtil.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
    }
    throw new IllegalStateException();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    switch (inputType) {
      case FLOAT:
        return new DoubleSumBufferAggregator.FloatInput(
            AggregatorUtil.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
      case DOUBLE:
        return new DoubleSumBufferAggregator.DoubleInput(
            AggregatorUtil.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
      case LONG:
        return new LongSumBufferAggregator(
            AggregatorUtil.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            AggregatorUtil.toPredicate(predicate, metricFactory)
        );
    }
    throw new IllegalStateException();
  }

  @Override
  public Comparator getComparator()
  {
    return inputType.comparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    switch (inputType) {
      case FLOAT:
        return ((Number) lhs).floatValue() + ((Number) rhs).floatValue();
      case DOUBLE:
        return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
      case LONG:
        return ((Number) lhs).longValue() + ((Number) rhs).longValue();
    }
    throw new IllegalStateException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GenericSumAggregatorFactory(name, name, inputType);
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
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new GenericSumAggregatorFactory(fieldName, fieldName, inputType));
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
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

  @JsonProperty
  public String getInputType()
  {
    return inputType.toString();
  }

  @Override
  public List<String> requiredFields()
  {
    Set<String> required = Sets.newLinkedHashSet();
    if (fieldName != null) {
      required.add(fieldName);
    } else {
      required.addAll(Parser.findRequiredBindings(fieldExpression));
    }
    if (predicate != null) {
      required.addAll(Parser.findRequiredBindings(predicate));
    }
    return Lists.newArrayList(required);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] inputTypeBytes = StringUtils.toUtf8(inputType.name());

    return ByteBuffer.allocate(1 + fieldNameBytes.length + inputTypeBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return inputType.name().toLowerCase();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0D;
  }

  @Override
  public String toString()
  {
    return "DoubleSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           "inputType='" + inputType + '\'' +
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

    GenericSumAggregatorFactory that = (GenericSumAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(inputType, that.inputType)) {
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
    return Objects.hash(fieldName, inputType, name);
  }
}
