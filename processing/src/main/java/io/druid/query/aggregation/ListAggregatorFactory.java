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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueType;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("list")
public class ListAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x0F;

  private final String name;
  private final String expression;
  private final String inputType;
  private final int limit;
  private final boolean dedup;

  @JsonIgnore
  private final ValueType valueType;
  @JsonIgnore
  private final boolean arrayInput;

  @JsonCreator
  public ListAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("inputType") String inputType,
      @JsonProperty("limit") int limit,
      @JsonProperty("dedup") boolean dedup
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.expression = Preconditions.checkNotNull(expression);
    this.inputType = Preconditions.checkNotNull(inputType);
    this.limit = limit;
    this.dedup = dedup;
    if (inputType.startsWith("array.")) {
      arrayInput = true;
      valueType = ValueType.of(inputType.substring(6));
    } else {
      arrayInput = false;
      valueType = ValueType.of(inputType);
    }
    Preconditions.checkArgument(valueType != ValueType.COMPLEX, "not support complex type");
  }

  private Collection<Object> createCollection()
  {
    return dedup ? Sets.newLinkedHashSet() : Lists.newArrayList();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (arrayInput) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(expression);
      return new Aggregators.AbstractEstimableAggregator()
      {
        private int estimation = 32;

        @Override
        public int estimateOccupation()
        {
          return estimation;
        }

        private final Collection<Object> list = createCollection();

        @Override
        public String getName()
        {
          return "list";
        }

        @Override
        public void aggregate()
        {
          List value = selector.get();
          list.addAll(value);
          if (limit > 0 && list.size() > limit) {
            throw new IllegalStateException("Exceeding limit " + limit);
          }
          estimation += estimate(value);
        }

        @Override
        public Object get()
        {
          return dedup ? Lists.newArrayList(list) : list;
        }
      };
    }
    final ExprEvalColumnSelector selector = metricFactory.makeMathExpressionSelector(expression);
    return new Aggregators.AbstractEstimableAggregator()
    {
      private int estimation = 32;

      @Override
      public int estimateOccupation()
      {
        return estimation;
      }

      private final Collection<Object> list = createCollection();

      @Override
      public String getName()
      {
        return "list";
      }

      @Override
      public void aggregate()
      {
        Object value = Evals.castTo(selector.get(), valueType);
        list.add(value);
        if (limit > 0 && list.size() > limit) {
          throw new IllegalStateException("Exceeding limit " + limit);
        }
        estimation += estimate(value);
      }

      @Override
      public Object get()
      {
        return dedup ? Lists.newArrayList(list) : list;
      }
    };
  }

  private int estimate(List value)
  {
    int estimate = 0;
    for (Object o : value) {
      estimate += estimate(o);
    }
    return estimate;
  }

  private int estimate(Object value)
  {
    if (value == null) {
      return 0;
    }
    switch (valueType) {
      case FLOAT:
        return Floats.BYTES;
      case LONG:
        return Longs.BYTES;
      case DOUBLE:
        return Doubles.BYTES;
      case STRING:
        return StringUtils.estimatedBinaryLengthAsUTF8((String) value);
    }
    return 0;
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (arrayInput) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(expression);
      return new BufferAggregator.Abstract()
      {
        private final List<Collection<Object>> lists = Lists.newArrayList();

        @Override
        public void init(ByteBuffer buf, int position)
        {
          buf.putInt(position, lists.size());
          lists.add(createCollection());
        }

        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          Collection<Object> list = lists.get(buf.getInt(position));
          list.addAll(selector.get());
          if (limit > 0 && list.size() > limit) {
            throw new IllegalStateException("Exceeding limit " + limit);
          }
        }

        @Override
        public Object get(ByteBuffer buf, int position)
        {
          Collection<Object> collection = lists.get(buf.getInt(position));
          return dedup ? Lists.newArrayList(collection) : collection;
        }
      };
    }
    final ExprEvalColumnSelector selector = metricFactory.makeMathExpressionSelector(expression);
    return new BufferAggregator.Abstract()
    {
      private final List<Collection<Object>> lists = Lists.newArrayList();

      @Override
      public void init(ByteBuffer buf, int position)
      {
        buf.putInt(position, lists.size());
        lists.add(createCollection());
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        Collection<Object> list = lists.get(buf.getInt(position));
        list.add(Evals.castTo(selector.get(), valueType));
        if (limit > 0 && list.size() > limit) {
          throw new IllegalStateException("Exceeding limit " + limit);
        }
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        Collection<Object> collection = lists.get(buf.getInt(position));
        return dedup ? Lists.newArrayList(collection) : collection;
      }
    };
  }

  @Override
  public Comparator getComparator()
  {
    return valueType.comparator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    ((List) lhs).addAll((List) rhs);
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    // takes array as input
    return new ListAggregatorFactory(name, name, arrayInput ? inputType : "array." + inputType, limit, dedup);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (getClass() == other.getClass() && other.getName().equals(getName())) {
      return getCombiningFactory();
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new ListAggregatorFactory(name, expression, inputType, limit, dedup));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public String getInputType()
  {
    return inputType;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  public boolean isDedup()
  {
    return dedup;
  }

  @Override
  public List<String> requiredFields()
  {
    return Parser.findRequiredBindings(expression);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] nameBytes = StringUtils.toUtf8WithNullToEmpty(name);
    byte[] expressionBytes = StringUtils.toUtf8WithNullToEmpty(expression);
    byte[] inputTypeBytes = StringUtils.toUtf8WithNullToEmpty(inputType);

    int length = 1
                 + nameBytes.length
                 + expressionBytes.length
                 + inputTypeBytes.length
                 + 5;

    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(nameBytes)
                     .put(expressionBytes)
                     .put(inputTypeBytes)
                     .putInt(limit)
                     .put(dedup ? (byte) 0x00 : (byte) 0x01)
                     .array();
  }

  @Override
  public String getInputTypeName()
  {
    return inputType;
  }

  @Override
  public String getTypeName()
  {
    return "array." + valueType.name().toLowerCase();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 4;   // we cannot pre-allocate.. use heap memory instead of byte buffer
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return Lists.newArrayList();
  }

  @Override
  public boolean providesEstimation()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "ListAggregatorFactory{" +
           "name='" + name + '\'' +
           "expression='" + expression + '\'' +
           "inputType='" + inputType + '\'' +
           "limit=" + limit +
           "dedup=" + dedup +
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

    ListAggregatorFactory that = (ListAggregatorFactory) o;

    if (!(Objects.equals(name, that.name))) {
      return false;
    }
    if (!(Objects.equals(expression, that.expression))) {
      return false;
    }
    if (!(Objects.equals(inputType, that.inputType))) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }
    if (dedup != that.dedup) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression, inputType, limit, dedup);
  }
}
