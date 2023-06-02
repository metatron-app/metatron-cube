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
import io.druid.query.aggregation.AggregatorFactory.CubeSupport;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnStats;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 */
public class DoubleMaxAggregatorFactory extends NumericAggregatorFactory.DoubleType implements CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0x3;

  @JsonCreator
  public DoubleMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate
  )
  {
    super(name, fieldName, fieldExpression, predicate);
  }

  public DoubleMaxAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  protected String statKey()
  {
    return ColumnStats.MAX;
  }

  @Override
  protected Object evaluate(LongStream stream)
  {
    return stream.max();
  }

  @Override
  protected Object evaluate(DoubleStream stream)
  {
    return stream.max();
  }

  @Override
  protected byte cacheKey()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public String getCubeName()
  {
    return "doubleMax";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Byte.BYTES + Double.BYTES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory factory)
  {
    return DoubleMaxAggregator.create(toFloatColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory)
  {
    return DoubleMaxBufferAggregator.create(toFloatColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return (param1, param2) -> Math.max(param1.doubleValue(), param2.doubleValue());
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMaxAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new DoubleMaxAggregatorFactory(name, inputField);
  }
}
