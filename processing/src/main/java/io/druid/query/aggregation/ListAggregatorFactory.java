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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.ExprEval;
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
import java.util.Set;

/**
 */
@JsonTypeName("list")
public class ListAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x10;

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
      @JsonProperty("inputType") ValueDesc inputType,
      @JsonProperty("limit") int limit,
      @JsonProperty("dedup") boolean dedup,
      @JsonProperty("sort") boolean sort
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.expression = Preconditions.checkNotNull(expression);
    this.inputType = Preconditions.checkNotNull(inputType);
    this.limit = limit;
    this.dedup = dedup;
    this.sort = sort;
    if (inputType.isArray()) {
      elementType = inputType.subElement(ValueDesc.UNKNOWN);
      outputType = inputType;
    } else {
      elementType = inputType;
      outputType = ValueDesc.ofArray(inputType);
    }
    Preconditions.checkArgument(elementType.isPrimitive(), "does not support complex type");
  }

  private Collection<Object> createCollection()
  {
    return dedup ? Sets.newHashSet() : Lists.newArrayList();
  }

  private List finalizeCollection(Collection collection)
  {
    List finalized = Lists.newArrayList(collection);
    if (sort) {
      Collections.sort(finalized);
    }
    return finalized;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    if (inputType.isArray()) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(expression);
      return new Aggregator.Estimable<Collection<Object>>()
      {
        @Override
        public int estimateOccupation(Collection<Object> current)
        {
          int estimation = 32;
          if (current != null) {
            for (Object x : current) {
              estimation += estimate(x);
            }
          }
          return estimation;
        }

        @Override
        public Collection<Object> aggregate(Collection<Object> current)
        {
          List value = selector.get();
          if (value == null) {
            return current;
          }
          if (current == null) {
            current = createCollection();
          }
          current.addAll(value);
          if (limit > 0 && current.size() > limit) {
            throw new IllegalStateException("Exceeding limit " + limit);
          }
          return current;
        }

        @Override
        public Object get(Collection<Object> current)
        {
          return current == null ? null : finalizeCollection(current);
        }
      };
    }
    final ExprEvalColumnSelector selector = metricFactory.makeMathExpressionSelector(expression);
    return new Aggregator.Estimable<Collection<Object>>()
    {
      @Override
      public int estimateOccupation(Collection<Object> current)
      {
        int estimation = 32;
        if (current != null) {
          for (Object x : current) {
            estimation += estimate(x);
          }
        }
        return estimation;
      }

      @Override
      public Collection<Object> aggregate(Collection<Object> current)
      {
        final ExprEval eval = selector.get();
        if (eval.isNull()) {
          return current;
        }
        if (current == null) {
          current = createCollection();
        }
        Object value = Evals.castToValue(eval, elementType);
        current.add(value);
        if (limit > 0 && current.size() > limit) {
          throw new IllegalStateException("Exceeding limit " + limit);
        }
        return current;
      }

      @Override
      public Object get(Collection<Object> current)
      {
        return current == null ? null : finalizeCollection(current);
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
        return Float.BYTES;
      case LONG:
        return Long.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case STRING:
        return StringUtils.estimatedBinaryLengthAsUTF8((String) value);
    }
    return 0;
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    if (inputType.isArray()) {
      @SuppressWarnings("unchecked")
      final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(expression);
      return new BufferAggregator()
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
    return new BufferAggregator()
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
        list.add(Evals.castToValue(selector.get(), elementType));
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
    return outputType.comparator();  // always throws exception
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<List> combiner()
  {
    return new Combiner<List>()
    {
      @Override
      public List combine(List param1, List param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        if (dedup) {
          Set set = Sets.newHashSet();
          set.addAll(param1);
          set.addAll(param2);
          return finalizeCollection(set);
        } else {
          param1.addAll(param2);
          return finalizeCollection(param1);
        }
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    // takes array as input
    return new ListAggregatorFactory(name, name, outputType, limit, dedup, sort);
  }

  @Override
  public Object deserialize(Object object)
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
  @Override
  public ValueDesc getInputType()
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(expression)
                  .append(inputType)
                  .append(limit)
                  .append(dedup, sort);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return outputType;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 4;   // we cannot pre-allocate.. use heap memory instead of byte buffer
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
           ", expression='" + expression + '\'' +
           ", inputType='" + inputType + '\'' +
           ", limit=" + limit +
           ", dedup=" + dedup +
           ", sort=" + sort +
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
