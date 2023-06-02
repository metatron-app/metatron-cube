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
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ColumnStats;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 */
public class DoubleMinAggregatorFactory extends NumericAggregatorFactory.DoubleType implements CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0x4;

  @JsonCreator
  public DoubleMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate
  )
  {
    super(name, fieldName, fieldExpression, predicate);
  }

  public DoubleMinAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, null);
  }

  @Override
  protected String statKey()
  {
    return ColumnStats.MIN;
  }

  @Override
  protected Object evaluate(LongStream stream)
  {
    return stream.min();
  }

  @Override
  protected Object evaluate(DoubleStream stream)
  {
    return stream.min();
  }

  @Override
  protected byte cacheKey()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public String getCubeName()
  {
    return "doubleMin";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Byte.BYTES + Double.BYTES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory factory)
  {
    return DoubleMinAggregator.create(toDoubleColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory)
  {
    return DoubleMinBufferAggregator.create(toDoubleColumnSelector(factory), predicateToMatcher(factory));
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return (param1, param2) -> Math.min(param1.doubleValue(), param2.doubleValue());
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMinAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new DoubleMinAggregatorFactory(name, inputField);
  }
}
