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
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 *
 */
public class LongSumAggregatorFactory extends NumericAggregatorFactory.LongType implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0x1;

  @JsonCreator
  public LongSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate
  )
  {
    super(name, fieldName, fieldExpression, predicate);
  }

  @Override
  protected Object evaluate(LongStream stream)
  {
    return stream.sum();
  }

  @Override
  protected Object evaluate(DoubleStream stream)
  {
    return stream.sum();
  }

  @Override
  protected Object nullValue()
  {
    return 0L;
  }

  @Override
  protected byte cacheKey()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public String getCubeName()
  {
    return "longSum";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  public LongSumAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  public LongSumAggregatorFactory(String fieldName)
  {
    this(fieldName, fieldName, null, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory factory)
  {
    return LongSumAggregator.create(getLongColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory)
  {
    return LongSumBufferAggregator.create(getLongColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return LongSumAggregator.COMBINER;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new LongSumAggregatorFactory(name, inputField);
  }

  @Override
  public Aggregator.Vectorized create(ColumnSelectorFactory factory)
  {
    return LongSumAggregator.vectorize((LongColumnSelector.Scannable) factory.makeLongColumnSelector(fieldName));
  }
}
