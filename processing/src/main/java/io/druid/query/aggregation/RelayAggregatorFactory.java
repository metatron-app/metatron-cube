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
import io.druid.common.utils.IOUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.AggregatorFactory.SQLSupport;
import io.druid.query.aggregation.AggregatorFactory.TypeResolving;
import io.druid.query.aggregation.Aggregators.RelayType;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnStats;
import io.druid.segment.Cursor;
import io.druid.segment.ScanContext;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.GenericColumn.DoubleType;
import io.druid.segment.column.GenericColumn.FloatType;
import io.druid.segment.column.GenericColumn.LongType;
import io.druid.segment.data.Dictionary;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 */
@JsonTypeName("relay")
public class RelayAggregatorFactory extends AggregatorFactory implements TypeResolving, SQLSupport
{
  private static final byte CACHE_TYPE_ID = 0x11;

  @JsonTypeName("firstOf")
  public static class TimeFirst extends RelayAggregatorFactory
  {
    public TimeFirst(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "TIME_MIN");
    }
  }

  @JsonTypeName("lastOf")
  public static class TimeLast extends RelayAggregatorFactory
  {
    public TimeLast(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "TIME_MAX");
    }
  }

  @JsonTypeName("minOf")
  public static class Min extends RelayAggregatorFactory
  {
    public Min(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "MIN");
    }
  }

  @JsonTypeName("maxOf")
  public static class Max extends RelayAggregatorFactory
  {
    public Max(String name, String columnName, String typeName)
    {
      super(name, columnName, typeName, "MAX");
    }
  }

  public static AggregatorFactory ofTime()
  {
    return new RelayAggregatorFactory(Column.TIME_COLUMN_NAME, Column.TIME_COLUMN_NAME, ValueDesc.LONG_TYPE);
  }

  public static AggregatorFactory of(String name, ValueDesc type)
  {
    return new RelayAggregatorFactory(name, name, type.typeName(), null, null);
  }

  public static AggregatorFactory first(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, RelayType.FIRST, null);
  }

  public static AggregatorFactory last(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, RelayType.LAST, null);
  }

  public static AggregatorFactory min(String name, String columnName)
  {
    return min(name, columnName, null);
  }

  public static AggregatorFactory min(String name, String columnName, String type)
  {
    return new RelayAggregatorFactory(name, columnName, type, RelayType.MIN, null);
  }

  public static AggregatorFactory max(String name, String columnName)
  {
    return max(name, columnName, null);
  }

  public static AggregatorFactory max(String name, String columnName, String type)
  {
    return new RelayAggregatorFactory(name, columnName, type, RelayType.MAX, null);
  }

  public static AggregatorFactory timeMin(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, RelayType.TIME_MIN, null);
  }

  public static AggregatorFactory timeMax(String name, String columnName)
  {
    return new RelayAggregatorFactory(name, columnName, null, RelayType.TIME_MAX, null);
  }

  private final String name;
  private final String columnName;
  private final String typeName;
  private final RelayType relayType;
  private final List<String> extractHints;

  @JsonCreator
  public RelayAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("relayType") RelayType relayType,
      @JsonProperty("extractHints") List<String> extractHints
  )
  {
    this.name = Preconditions.checkNotNull(name == null ? columnName : name);
    this.columnName = Preconditions.checkNotNull(columnName == null ? name : columnName);
    this.typeName = typeName;
    this.relayType = relayType == null ? RelayType.ONLY_ONE : relayType;
    this.extractHints = extractHints;
  }

  public RelayAggregatorFactory(String name, ValueDesc type)
  {
    this(name, name, type.typeName(), null, null);
  }

  public RelayAggregatorFactory(String name, String columnName, String typeName)
  {
    this(name, columnName, typeName, null, null);
  }

  public RelayAggregatorFactory(String name, String columnName, String typeName, String relayType)
  {
    this(name, columnName, typeName, RelayType.valueOf(relayType), null);
  }

  @Override
  public AggregatorFactory optimize(Cursor cursor)
  {
    if (relayType != RelayType.MIN && relayType != RelayType.MAX) {
      return this;  // todo
    }
    if (cursor.getColumnCapabilities(columnName) == null) {
      return this;
    }
    ValueDesc desc = ValueDesc.of(typeName);
    if (desc != null && desc.isPrimitive()) {
      Object constant = ColumnStats.get(cursor.getStats(columnName), desc.type(), relayType.toStatKey());
      if (constant != null) {
        return AggregatorFactory.constant(this, constant);
      }
    }
    return this;
  }

  @Override
  public AggregatorFactory evaluate(Cursor cursor, ScanContext context)
  {
    if (relayType != RelayType.MIN && relayType != RelayType.MAX) {
      return this;    // todo
    }
    Column column = cursor.getColumn(columnName);
    if (column == null) {
      return this;    // vc?
    }
    if (context.count() == 0) {
      return AggregatorFactory.constant(this, null);
    }
    Dictionary<String> dictionary = column.getDictionary();
    if (dictionary != null && dictionary.isSorted()) {
      if (dictionary.isEmpty()) {
        return AggregatorFactory.constant(this, null);
      }
      if (relayType == RelayType.MAX) {
        return AggregatorFactory.constant(this, dictionary.get(dictionary.size() - 1));
      }
      if (!dictionary.containsNull()) {
        return AggregatorFactory.constant(this, dictionary.get(0));
      } else if (dictionary.size() > 1) {
        return AggregatorFactory.constant(this, dictionary.get(1));
      } else {
        return AggregatorFactory.constant(this, null);
      }
    } else if (column.hasGenericColumn()) {
      GenericColumn generic = column.getGenericColumn();
      try {
        if (generic instanceof LongType) {
          LongStream stream = ((LongType) generic).stream(context.iterator());
          OptionalLong max = relayType == RelayType.MAX ? stream.max() : stream.min();
          return AggregatorFactory.constant(this, max.isPresent() ? max.getAsLong() : null);
        } else if (generic instanceof FloatType) {
          DoubleStream stream = ((FloatType) generic).stream(context.iterator());
          OptionalDouble max = relayType == RelayType.MAX ? stream.max() : stream.min();
          return AggregatorFactory.constant(this, max.isPresent() ? max.getAsDouble() : null);
        } else if (generic instanceof DoubleType) {
          DoubleStream stream = ((DoubleType) generic).stream(context.iterator());
          OptionalDouble max = relayType == RelayType.MAX ? stream.max() : stream.min();
          return AggregatorFactory.constant(this, max.isPresent() ? max.getAsDouble() : null);
        }
      } finally {
        IOUtils.closeQuietly(generic);
      }
      // todo: handle IndexedStringsGenericColumn
    }
    return this;
  }

  @Override
  public AggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver)
  {
    String fieldName = Iterables.getOnlyElement(fieldNames, null);
    if (fieldName == null) {
      return null;
    }
    ValueDesc inputType = resolver.resolve(fieldName, ValueDesc.UNKNOWN);
    return new RelayAggregatorFactory(name, fieldName, inputType.typeName(), relayType, extractHints);
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

  @Override
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
  public BinaryFn.Identical combiner()
  {
    return Aggregators.relayCombiner(relayType);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new RelayAggregatorFactory(name, name, typeName, relayType, null);
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
  public RelayType getRelayType()
  {
    return relayType;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getExtractHints()
  {
    return extractHints == null ? Arrays.asList() : extractHints;
  }

  @Override
  public ValueDesc getOutputType()
  {
    if (relayType == RelayType.TIME_MIN || relayType == RelayType.TIME_MAX) {
      return typeName == null ? ValueDesc.STRUCT : ValueDesc.of(String.format("struct(t:long,v:%s)", typeName));
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
                  .append(relayType)
                  .append(extractHints);
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
    if (!Objects.equals(extractHints, that.extractHints)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, columnName, typeName, relayType, extractHints);
  }

  @Override
  public String toString()
  {
    return "RelayAggregatorFactory{" +
           "name='" + name + '\'' +
           ", columnName='" + columnName + '\'' +
           (typeName == null ? "": ", typeName='" + typeName + '\'') +
           (relayType == null ? "": ", columnName='" + columnName + '\'') +
           (extractHints == null ? "": ", extractHints='" + extractHints + '\'') +
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
        relayType,
        extractHints
    );
  }
}
