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
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ExprEvalColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
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
  private final ValueDesc inputType;
  private final int limit;
  private final boolean dedup;
  private final boolean sort;

  @JsonIgnore
  private final ValueDesc elementType;
  @JsonIgnore
  private final ValueDesc outputType;

  @JsonCreator
  public ListAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("inputType") String inputType,
      @JsonProperty("limit") int limit,
      @JsonProperty("dedup") boolean dedup,
      @JsonProperty("sort") boolean sort
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.expression = Preconditions.checkNotNull(expression);
    this.inputType = ValueDesc.of(Preconditions.checkNotNull(inputType));
    this.limit = limit;
    this.dedup = dedup;
    this.sort = sort;
    if (ValueDesc.isArray(inputType)) {
      elementType = ValueDesc.elementOfArray(inputType);
      outputType = this.inputType;
    } else {
      elementType = this.inputType;
      outputType = ValueDesc.ofArray(inputType);
    }
    Preconditions.checkArgument(ValueDesc.isPrimitive(elementType), "does not support complex type");
  }

  private Collection<Object> createCollection()
  {
    return dedup ? Sets.newLinkedHashSet() : Lists.newArrayList();
  }

  private Object finalizeCollection(Collection collection)
  {
    List finalized = dedup ? Lists.newArrayList(collection) : (List) collection;
    if (sort) {
      Collections.sort(finalized);
    }
    return finalized;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (ValueDesc.isArray(inputType)) {
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
          return finalizeCollection(list);
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
      public void aggregate()
      {
        Object value = Evals.castTo(selector.get(), elementType);
        list.add(value);
        if (limit > 0 && list.size() > limit) {
          throw new IllegalStateException("Exceeding limit " + limit);
        }
        estimation += estimate(value);
      }

      @Override
      public Object get()
      {
        return finalizeCollection(list);
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
    switch (elementType.type()) {
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
    if (ValueDesc.isArray(inputType)) {
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
          return finalizeCollection(collection);
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
        list.add(Evals.castTo(selector.get(), elementType));
        if (limit > 0 && list.size() > limit) {
          throw new IllegalStateException("Exceeding limit " + limit);
        }
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        Collection<Object> collection = lists.get(buf.getInt(position));
        return finalizeCollection(collection);
      }
    };
  }

  @Override
  public Comparator getComparator()
  {
    return outputType.type().comparator();  // always throws exception
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
    return new ListAggregatorFactory(name, name, outputType.typeName(), limit, dedup, sort);
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
    return inputType.typeName();
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

  @JsonProperty
  public boolean isSort()
  {
    return sort;
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
    byte[] inputTypeBytes = StringUtils.toUtf8WithNullToEmpty(inputType.typeName());

    int length = 1
                 + nameBytes.length
                 + expressionBytes.length
                 + inputTypeBytes.length
                 + 6;

    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(nameBytes)
                     .put(expressionBytes)
                     .put(inputTypeBytes)
                     .putInt(limit)
                     .put(dedup ? (byte) 0x01 : (byte) 0x00)
                     .put(sort ? (byte) 0x01 : (byte) 0x00)
                     .array();
  }

  @Override
  public String getInputTypeName()
  {
    return inputType.typeName();
  }

  @Override
  public String getTypeName()
  {
    return outputType.typeName();
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
           "sort=" + sort +
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
    if (sort != that.sort) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression, inputType, limit, dedup, sort);
  }
}
