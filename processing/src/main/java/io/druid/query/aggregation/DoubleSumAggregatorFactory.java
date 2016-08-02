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
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class DoubleSumAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String name;
  private final String fieldName;
  private final String fieldExpression;
  private final String predicate;

  @JsonCreator
  public DoubleSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate
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
  }

  public DoubleSumAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleSumAggregator.FloatInput(
        name,
        getFloatColumnSelector(metricFactory),
        AggregatorUtil.toPredicate(predicate, metricFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleSumBufferAggregator.FloatInput(
        getFloatColumnSelector(metricFactory),
        AggregatorUtil.toPredicate(predicate, metricFactory)
    );
  }

  private FloatColumnSelector getFloatColumnSelector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.getFloatColumnSelector(metricFactory, fieldName, fieldExpression);
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleSumAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleSumAggregatorFactory(name, name);
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
    return Arrays.<AggregatorFactory>asList(
        new DoubleSumAggregatorFactory(
            fieldName,
            fieldName,
            fieldExpression,
            predicate
        )
    );
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

  @JsonProperty
  public String getFieldExpression()
  {
    return fieldExpression;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    Set<String> required = Sets.newHashSet();
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
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] fieldExpressionBytes = StringUtils.toUtf8WithNullToEmpty(fieldExpression);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);

    return ByteBuffer.allocate(1 + fieldNameBytes.length + fieldExpressionBytes.length + predicateBytes.length)
                     .put(CACHE_TYPE_ID).put(fieldNameBytes).put(fieldExpressionBytes).put(predicateBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "DoubleSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", fieldExpression='" + fieldExpression + '\'' +
           ", predicate='" + predicate + '\'' +
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

    DoubleSumAggregatorFactory that = (DoubleSumAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(fieldExpression, that.fieldExpression)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
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
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (fieldExpression != null ? fieldExpression.hashCode() : 0);
    result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
