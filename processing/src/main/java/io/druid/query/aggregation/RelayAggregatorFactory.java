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
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class RelayAggregatorFactory extends AggregatorFactory
{
  public static AggregatorFactory ofTime()
  {
    return new RelayAggregatorFactory(Column.TIME_COLUMN_NAME, Column.TIME_COLUMN_NAME, ValueDesc.LONG_TYPE);
  }

  public static AggregatorFactory of(String name, ValueDesc type)
  {
    return new RelayAggregatorFactory(name, name, type.typeName());
  }

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
    this.name = Preconditions.checkNotNull(name == null ? columnName : name);
    this.columnName = Preconditions.checkNotNull(columnName == null ? name : columnName);
    this.typeName = Preconditions.checkNotNull(typeName);
  }

  public RelayAggregatorFactory(String name, String typeName)
  {
    this(name, name, typeName);
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
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other instanceof RelayAggregatorFactory) {
      RelayAggregatorFactory relay = (RelayAggregatorFactory)other;
      if (name.equals(relay.name) && typeName.equals(relay.typeName)) {
        return new RelayAggregatorFactory(name, name, typeName);
      }
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException("getComparator");
  }

  @Override
  public <T> Combiner<T> combiner()
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RelayAggregatorFactory that = (RelayAggregatorFactory) o;

    if (!columnName.equals(that.columnName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (!typeName.equals(that.typeName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + columnName.hashCode();
    result = 31 * result + typeName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "RelayAggregatorFactory{" +
           "name='" + name + '\'' +
           ", columnName='" + columnName + '\'' +
           ", typeName='" + typeName + '\'' +
           '}';
  }
}
