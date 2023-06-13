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
import io.druid.segment.DoubleColumnSelector;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 *
 */
public class DoubleSumAggregatorFactory extends NumericAggregatorFactory.DoubleType
    implements AggregatorFactory.CubeSupport, AggregatorFactory.Vectorizable
{
  private static final byte CACHE_TYPE_ID = 0x2;

  @JsonCreator
  public DoubleSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate
  )
  {
    super(name, fieldName, fieldExpression, predicate);
  }

  public DoubleSumAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
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
    return 0D;
  }

  @Override
  protected byte cacheKey()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public String getCubeName()
  {
    return "doubleSum";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory factory)
  {
    return DoubleSumAggregator.create(toFloatColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory)
  {
    return DoubleSumBufferAggregator.create(toFloatColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return DoubleSumAggregator.COMBINER;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleSumAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new DoubleSumAggregatorFactory(name, inputField);
  }

  @Override
  public Aggregator.Vectorized create(ColumnSelectorFactory factory)
  {
    return DoubleSumAggregator.vectorize((DoubleColumnSelector.Scannable) factory.makeDoubleColumnSelector(fieldName));
  }
}
