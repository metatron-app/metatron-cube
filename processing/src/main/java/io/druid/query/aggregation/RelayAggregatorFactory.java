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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregators.RELAY_TYPE;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("relay")
public class RelayAggregatorFactory extends AggregatorFactory.TypeResolving implements AggregatorFactory.SQLSupport
{
  private static final byte CACHE_TYPE_ID = 0x11;

  @JsonTypeName("firstOf")
  public static class First extends RelayAggregatorFactory
  {
    public First(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "FIRST");
    }
  }

  @JsonTypeName("lastOf")
  public static class Last extends RelayAggregatorFactory
  {
    public Last(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "LAST");
    }
  }

  public static AggregatorFactory ofTime()
  {
    return new RelayAggregatorFactory(Column.TIME_COLUMN_NAME, Column.TIME_COLUMN_NAME, ValueDesc.LONG_TYPE, null);
  }

  public static AggregatorFactory of(String name, ValueDesc type)
  {
    return new RelayAggregatorFactory(name, name, type.typeName(), null);
  }

  public static AggregatorFactory first(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "FIRST");
  }

  public static AggregatorFactory last(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "LAST");
  }

  public static AggregatorFactory min(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "MIN");
  }

  public static AggregatorFactory max(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "MAX");
  }

  public static AggregatorFactory timeMin(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "TIME_MIN");
  }

  public static AggregatorFactory timeMax(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, "TIME_MAX");
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
  public AggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver)
  {
    String fieldName = Iterables.getOnlyElement(fieldNames, null);
    if (fieldName == null) {
      return null;
    }
    ValueDesc inputType = resolver.resolve(fieldName, ValueDesc.UNKNOWN);
    return new RelayAggregatorFactory(name, fieldName, inputType.typeName(), relayType);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return Aggregators.relayAggregator(metricFactory, columnName, relayType);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return Aggregators.relayBufferAggregator(metricFactory, columnName, relayType);
  }

  protected boolean isMergeable(AggregatorFactory other)
  {
    return getName().equals(other.getName()) && other instanceof RelayAggregatorFactory &&
           Objects.equals(typeName, ((RelayAggregatorFactory) other).typeName);
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
    final RELAY_TYPE relayType = RELAY_TYPE.fromString(this.relayType);
    if (relayType == RELAY_TYPE.TIME_MIN || relayType == RELAY_TYPE.TIME_MAX) {
      return ValueDesc.STRUCT;
    }
    return typeName == null ? null : ValueDesc.of(typeName);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object instanceof List) {
      return ((List) object).get(1);
    }
    return object;
  }

  public ValueDesc finalizedType()
  {
    return typeName == null ? null : ValueDesc.of(typeName);
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(columnName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(columnName)
                  .append(typeName)
                  .append(relayType);
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
        Preconditions.checkNotNull(resolver.get().resolve(columnName), "Failed to resolve %s", columnName).typeName(),
        relayType
    );
  }
}
