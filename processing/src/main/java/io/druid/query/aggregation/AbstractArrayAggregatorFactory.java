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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Comparators;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.logger.Logger;

import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class AbstractArrayAggregatorFactory extends AggregatorFactory
{
  static final Logger logger = new Logger(AbstractArrayAggregatorFactory.class);

  static final int DEFAULT_LIMIT = 8;   // should be < 65536

  final String column;
  final AggregatorFactory delegate;
  final int limit;

  final ValueDesc elementType;

  public AbstractArrayAggregatorFactory(String column, AggregatorFactory delegate, int limit)
  {
    this.delegate = delegate;
    this.column = column == null ? Iterables.getOnlyElement(delegate.requiredFields()) : column;
    this.limit = limit <= 0 ? DEFAULT_LIMIT : limit;
    this.elementType = delegate.getOutputType();
  }

  @JsonProperty("column")
  public String getColumn()
  {
    return column;
  }

  @JsonProperty("aggregator")
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @JsonProperty("limit")
  public int getLimit()
  {
    return limit;
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.toListComparator(delegate.getComparator());
  }

  @Override
  public BinaryFn.Identical<List<Object>> combiner()
  {
    return new BinaryFn.Identical<List<Object>>()
    {
      final BinaryFn.Identical combiner = delegate.combiner();

      @Override
      @SuppressWarnings("unchecked")
      public List<Object> apply(final List<Object> left, final List<Object> right)
      {
        int min = Math.min(left.size(), right.size());
        int i = 0;
        for (; i < min; i++) {
          left.set(i, combiner.apply(left.get(i), right.get(i)));
        }
        for (; i < right.size(); i++) {
          left.add(right.get(i));
        }

        return left;
      }
    };
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null) {
      return null;
    }
    List value = (List) object;
    for (int i = 0; i < value.size(); i++) {
      value.set(i, delegate.deserialize(value.get(i)));
    }
    return value;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object == null) {
      return null;
    }
    List<Object> values = (List<Object>) object;
    for (int i = 0; i < values.size(); i++) {
      values.set(i, delegate.finalizeComputation(values.get(i)));
    }
    return values;
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.ofArray(delegate.finalizedType());
  }

  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(column);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(cacheTypeID())
                  .append(column)
                  .append(delegate)
                  .append(limit);
  }

  protected abstract byte cacheTypeID();

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.ofArray(delegate.getOutputType());
  }

  @Override
  public ValueDesc getInputType()
  {
    return ValueDesc.ofArray(delegate.getInputType());
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize() * limit;
  }

  @Override
  public boolean providesEstimation()
  {
    return true;
  }
}
