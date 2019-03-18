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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class RelayAggregatorFactory extends AggregatorFactory.TypeResolving
{
  private static final byte CACHE_TYPE_ID = 0x11;

  public static AggregatorFactory ofTime()
  {
    return new RelayAggregatorFactory(Column.TIME_COLUMN_NAME, Column.TIME_COLUMN_NAME, ValueDesc.LONG_TYPE, null);
  }

  public static AggregatorFactory of(String name, ValueDesc type)
  {
    return new RelayAggregatorFactory(name, name, type.typeName(), null);
  }

  public static AggregatorFactory first(String name)
  {
    return new RelayAggregatorFactory(name, name, null, "FIRST");
  }

  public static AggregatorFactory last(String name)
  {
    return new RelayAggregatorFactory(name, name, null, "LAST");
  }

  private final String name;
  private final String columnName;
  private final String typeName;
  private final String relayType;

  @JsonCreator
  public RelayAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("relayType") String relayType
      )
  {
    this.name = Preconditions.checkNotNull(name == null ? columnName : name);
    this.columnName = Preconditions.checkNotNull(columnName == null ? name : columnName);
    this.typeName = typeName;
    this.relayType = relayType;
  }

  public RelayAggregatorFactory(String name, ValueDesc type)
  {
    this(name, name, type.typeName(), null);
  }

  public RelayAggregatorFactory(String name, String columnName, String typeName)
  {
    this(name, columnName, typeName, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return Aggregators.relayAggregator(metricFactory.makeObjectColumnSelector(columnName), relayType);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return Aggregators.relayBufferAggregator(metricFactory.makeObjectColumnSelector(columnName), relayType);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other instanceof RelayAggregatorFactory) {
      RelayAggregatorFactory relay = (RelayAggregatorFactory)other;
      if (Objects.equals(name, relay.name) && Objects.equals(typeName, relay.typeName)) {
        return new RelayAggregatorFactory(name, name, relayType, typeName);
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
  @SuppressWarnings("unchecked")
  public <T> Combiner<T> combiner()
  {
    return Aggregators.relayCombiner(relayType);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new RelayAggregatorFactory(name, name, typeName, relayType);
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
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getRelayType()
  {
    return relayType;
  }

  @Override
  public ValueDesc getOutputType()
  {
    return typeName == null ? null : ValueDesc.of(typeName);
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(columnName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] columnNameBytes = StringUtils.toUtf8WithNullToEmpty(columnName);
    byte[] typeNameBytes = StringUtils.toUtf8WithNullToEmpty(typeName);
    byte[] relayTypeBytes = StringUtils.toUtf8WithNullToEmpty(relayType);

    int length = 1 + columnNameBytes.length + typeNameBytes.length + relayTypeBytes.length;
    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(columnNameBytes)
                     .put(typeNameBytes)
                     .put(relayTypeBytes)
                     .array();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES;
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
    if (!Objects.equals(typeName, that.typeName)) {
      return false;
    }
    if (!Objects.equals(relayType, that.relayType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, columnName, typeName, relayType);
  }

  @Override
  public String toString()
  {
    return "RelayAggregatorFactory{" +
           "name='" + name + '\'' +
           ", columnName='" + columnName + '\'' +
           (typeName == null ? "": ", typeName='" + typeName + '\'') +
           (relayType == null ? "": ", columnName='" + columnName + '\'') +
           '}';
  }

  @Override
  public boolean needResolving()
  {
    return typeName == null;
  }

  @Override
  public AggregatorFactory resolve(Supplier<? extends TypeResolver> resolver)
  {
    return new RelayAggregatorFactory(
        name,
        columnName,
        Preconditions.checkNotNull(resolver.get().resolve(columnName), "Failed to resolve " + columnName).typeName(),
        relayType
    );
  }
}
