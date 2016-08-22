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

package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class RangeAggregatorFactory extends AggregatorFactory
{
  private static final Logger log = new Logger(RangeAggregatorFactory.class);
  private static final byte CACHE_TYPE_ID = 30;

  private final AggregatorFactory delegateFactory;
  private final int rangeStart;
  private final int rangeCount;

  public enum RangeState {
    beforeStart,
    beforeLimit,
    afterLimit
  }

  @JsonCreator
  public RangeAggregatorFactory(
      @JsonProperty("delegateFactory") AggregatorFactory delegateFactory,
      @JsonProperty("rangeStart") Integer offset,
      @JsonProperty("rangeCount") Integer size
  )
  {
    this.delegateFactory = Preconditions.checkNotNull(delegateFactory);
    this.rangeStart = Preconditions.checkNotNull(offset);
    Preconditions.checkNotNull(size);
    this.rangeCount = (size == -1) ? Integer.MAX_VALUE : size;
  }

  @JsonProperty
  public AggregatorFactory getDelegateFactory()
  {
    return delegateFactory;
  }

  @JsonProperty
  public Integer getRangeStart()
  {
    return rangeStart;
  }

  @JsonProperty
  public Integer getRangeCount()
  {
    return rangeCount;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    Aggregator delegate = delegateFactory.factorize(metricFactory);

    return new RangeAggregator(delegate, rangeStart, rangeCount);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    BufferAggregator delegate = delegateFactory.factorizeBuffered(metricFactory);

    return new RangeBufferAggregator(delegate, rangeStart, rangeCount);
  }

  @Override
  public Comparator getComparator()
  {
    return delegateFactory.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegateFactory.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegateFactory;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new RangeAggregatorFactory(delegateFactory, rangeStart, rangeCount));
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegateFactory.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return delegateFactory.finalizeComputation(object);
  }

  @Override
  public String getName()
  {
    return delegateFactory.getName();
  }

  @Override
  public String getInputTypeName()
  {
    return delegateFactory.getInputTypeName();
  }

  @Override
  public List<String> requiredFields()
  {
    return delegateFactory.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateBytes = delegateFactory.getCacheKey();
    return ByteBuffer.allocate(1 + delegateBytes.length + 1 + 4 + 1 + 4)
        .put(CACHE_TYPE_ID)
        .put(delegateBytes)
        .put(AggregatorUtil.STRING_SEPARATOR)
        .putInt(rangeStart)
        .put(AggregatorUtil.STRING_SEPARATOR)
        .putInt(rangeCount)
        .array();
  }

  @Override
  public String getTypeName()
  {
    return delegateFactory.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegateFactory.getMaxIntermediateSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return delegateFactory.getAggregatorStartValue();
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

    RangeAggregatorFactory that = (RangeAggregatorFactory)o;

    if (!delegateFactory.equals(that.delegateFactory)) {
      return false;
    }

    if (rangeStart != that.rangeStart) {
      return false;
    }
    if (rangeCount != that.rangeCount) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = 31 * rangeStart + rangeCount;
    result = 31 * result + delegateFactory.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "RangeAggregatorFactory{" +
           "delegate=" + delegateFactory +
           "rangeStart=" + rangeStart + "rangeCount=" + rangeCount +
           "}";
  }
}
