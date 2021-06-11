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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("schema")
public class Schema extends RowSignature
{
  public static final Schema EMPTY = new Schema(
      Collections.<String>emptyList(),
      Collections.<ValueDesc>emptyList(),
      Collections.<String, AggregatorFactory>emptyMap(),
      Collections.<String, ColumnCapabilities>emptyMap(),
      Collections.<String, Map<String, String>>emptyMap()
  );

  @VisibleForTesting
  public static Schema of(Map<String, ValueDesc> columnAndTypes)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    Map<String, ColumnCapabilities> capabilities = Maps.newHashMap();
    for (Map.Entry<String, ValueDesc> entry : columnAndTypes.entrySet()) {
      String column = entry.getKey();
      columnNames.add(column);
      if (column.equals(Column.TIME_COLUMN_NAME)) {
        capabilities.put(column, ColumnCapabilities.of(ValueType.LONG));
        columnTypes.add(ValueDesc.LONG);
        continue;
      }
      ValueDesc type = entry.getValue();
      if (type.isDimension()) {
        type = type.unwrapDimension();
        capabilities.put(column, ColumnCapabilities.of(type.type()).setHasBitmapIndexes(true));
      }
      columnTypes.add(type);
    }
    return new Schema(columnNames, columnTypes, null, capabilities, null);
  }

  private final Map<String, AggregatorFactory> aggregators;
  private final Map<String, ColumnCapabilities> capabilities;
  private final Map<String, Map<String, String>> descriptors;

  @JsonCreator
  public Schema(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("columnTypes") List<ValueDesc> columnTypes,
      @JsonProperty("aggregators") Map<String, AggregatorFactory> aggregators,
      @JsonProperty("capabilities") Map<String, ColumnCapabilities> capabilities,
      @JsonProperty("descriptors") Map<String, Map<String, String>> descriptors
  )
  {
    super(columnNames, columnTypes);
    this.aggregators = aggregators == null ? ImmutableMap.<String, AggregatorFactory>of() : aggregators;
    this.capabilities = capabilities == null ? ImmutableMap.<String, ColumnCapabilities>of() : capabilities;
    this.descriptors = descriptors == null ? ImmutableMap.<String, Map<String, String>>of() : descriptors;
    Preconditions.checkArgument(columnNames.size() == columnTypes.size());
  }

  public Schema(List<String> columnNames, List<ValueDesc> columnTypes)
  {
    this(columnNames, columnTypes, null, null, null);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, ColumnCapabilities> getCapabilities()
  {
    return capabilities;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Map<String, String>> getDescriptors()
  {
    return descriptors;
  }

  public Schema merge(Schema other)
  {
    List<String> mergedColumns = Lists.newArrayList(columnNames);
    List<ValueDesc> mergedTypes = Lists.newArrayList(columnTypes);
    Map<String, AggregatorFactory> mergedAggregators = Maps.newHashMap(aggregators);
    Map<String, ColumnCapabilities> mergedCapabilities = Maps.newHashMap(capabilities);
    Map<String, Map<String, String>> mergedDescs = Maps.newLinkedHashMap(descriptors);

    final List<String> otherColumnNames = other.getColumnNames();
    final List<ValueDesc> otherColumnTypes = other.getColumnTypes();
    for (int i = 0; i < other.size(); i++) {
      final String otherColumn = otherColumnNames.get(i);
      final int index = mergedColumns.indexOf(otherColumn);
      if (index < 0) {
        mergedColumns.add(otherColumn);
        mergedTypes.add(otherColumnTypes.get(i));
        AggregatorFactory factory = other.aggregators.get(otherColumn);
        if (factory != null) {
          mergedAggregators.put(otherColumn, factory);
        }
        ColumnCapabilities capabilities = other.capabilities.get(otherColumn);
        if (capabilities != null) {
          mergedCapabilities.put(otherColumn, capabilities);
        }
        Map<String, String> descs = other.descriptors.get(otherColumn);
        if (descs != null) {
          mergedDescs.put(otherColumn, descs);
        }
      } else {
        ValueDesc type1 = resolve(otherColumn);
        ValueDesc type2 = other.resolve(otherColumn);
        if (!Objects.equals(type1, type2)) {
          mergedTypes.set(index, ValueDesc.toCommonType(type1, type2));
        }

        // strict on conflicts
        ColumnCapabilities capabilities1 = capabilities.get(otherColumn);
        ColumnCapabilities capabilities2 = other.capabilities.get(otherColumn);
        if (capabilities1 == null || !capabilities1.equals(capabilities2)) {
          mergedCapabilities.remove(otherColumn);
        }

        // strict on conflicts
        Map<String, String> desc1 = descriptors.get(otherColumn);
        Map<String, String> desc2 = other.descriptors.get(otherColumn);
        if (desc1 == null || !desc1.equals(desc2)) {
          mergedDescs.remove(otherColumn);
        }
      }
    }

    return new Schema(mergedColumns, mergedTypes, mergedAggregators, mergedCapabilities, mergedDescs);
  }

  @Override
  public String toString()
  {
    return "Schema{" +
           "columnNames=" + columnNames +
           ", columnTypes=" + columnTypes +
           ", aggregators=" + aggregators +
           ", descriptors=" + descriptors +
           ", capabilities=" + capabilities +
           '}';
  }
}
