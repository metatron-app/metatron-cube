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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class RelayAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final String columnName;
  private final String typeName;

  @JsonCreator
  public RelayAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("typeName") String typeName
  )
  {
    this.name = name;
    this.columnName = columnName;
    this.typeName = typeName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return Aggregators.relayAggregator(metricFactory.makeObjectColumnSelector(columnName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException("factorizeBuffered");
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException("getComparator");
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    throw new UnsupportedOperationException("combine");
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new RelayAggregatorFactory(name, name, typeName);
  }

  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException("deserialize");
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
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(columnName);
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException("getCacheKey");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 0;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return null;
  }
}
