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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.data.ValueDesc;

import java.nio.ByteBuffer;
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
    this.elementType = ValueDesc.of(delegate.getTypeName());
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
    final Comparator comparator = delegate.getComparator();
    return new Comparator<List<Object>>()
    {
      @Override
      public int compare(List<Object> o1, List<Object> o2)
      {
        int min = Math.min(o1.size(), o2.size());
        for (int i = 0; i < min; i++) {
          int ret = comparator.compare(o1.get(i), o2.get(i));
          if (ret != 0) {
            return ret;
          }
        }
        return o1.size() - o2.size();
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    List<Object> left = (List<Object>) lhs;
    List<Object> right = (List<Object>) rhs;
    int min = Math.min(left.size(), right.size());
    int i = 0;
    for (; i < min; i++) {
      left.set(i, delegate.combine(left.get(i), right.get(i)));
    }
    for (; i < right.size(); i++) {
      left.add(right.get(i));
    }

    return left;
  }

  @Override
  public Object deserialize(Object object)
  {
    List value = (List)object;
    for (int i = 0; i < value.size(); i++ ) {
      value.set(i, delegate.deserialize(value.get(i)));
    }
    return value;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    List<Object> values = (List<Object>) object;
    for (int i = 0; i < values.size(); i++) {
      values.set(i, delegate.finalizeComputation(values.get(i)));
    }
    return values;
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
  public byte[] getCacheKey()
  {
    byte[] columnBytes = StringUtils.toUtf8(column);
    byte[] cacheKey = delegate.getCacheKey();
    return ByteBuffer.allocate(1 + columnBytes.length + cacheKey.length + Ints.BYTES)
                     .put(cacheTypeID())
                     .put(columnBytes)
                     .put(cacheKey)
                     .putInt(limit)
                     .array();
  }

  protected abstract byte cacheTypeID();

  @Override
  public String getTypeName()
  {
    return "array." + delegate.getTypeName();
  }

  @Override
  public String getInputTypeName()
  {
    return "array." + delegate.getInputTypeName();
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
