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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 */
public class RowSignature implements TypeResolver
{
  protected final List<String> columnNames;
  protected final List<ValueDesc> columnTypes;

  @JsonCreator
  public RowSignature(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("columnTypes") List<ValueDesc> columnTypes
  )
  {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @JsonProperty
  public List<ValueDesc> getColumnTypes()
  {
    return columnTypes;
  }

  @Override
  public ValueDesc resolve(String column)
  {
    int index = columnNames.indexOf(column);
    if (index >= 0) {
      return columnTypes.get(index);
    }
    ValueDesc resolved = null;
    for (int x = column.lastIndexOf('.'); resolved == null && x > 0; x = column.lastIndexOf('.', x - 1)) {
      resolved = findElementOfStruct(column.substring(0, x), column.substring(x + 1));
    }
    return resolved;
  }

  public int size()
  {
    return getColumnNames().size();
  }

  public String columnName(int index)
  {
    return index < 0 ? null : getColumnNames().get(index);
  }

  public ValueDesc columnType(int index)
  {
    return index < 0 ? null : getColumnTypes().get(index);
  }

  public ValueDesc columnType(String name)
  {
    return columnType(getColumnNames().indexOf(name));
  }

  public Iterable<Pair<String, ValueDesc>> columnAndTypes()
  {
    return GuavaUtils.zip(getColumnNames(), getColumnTypes());
  }

  public Stream<Pair<String, ValueDesc>> stream()
  {
    return IntStream.range(0, columnNames.size()).mapToObj(x -> Pair.of(columnNames.get(x), columnTypes.get(x)));
  }

  public Iterable<Pair<String, ValueDesc>> dimensionAndTypes()
  {
    return columnAndTypes(type -> type.isDimension());
  }

  public Iterable<Pair<String, ValueDesc>> metricAndTypes()
  {
    return columnAndTypes(type -> !type.isDimension());
  }

  public List<Pair<String, ValueDesc>> columnAndTypes(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<Pair<String, ValueDesc>> columnAndTypes = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        columnAndTypes.add(Pair.of(columnNames.get(i), columnTypes.get(i)));
      }
    }
    return columnAndTypes;
  }

  public Iterable<Pair<String, ValueDesc>> columnAndTypes(Iterable<String> columns)
  {
    return Iterables.transform(columns, c -> Pair.of(c, resolve(c, ValueDesc.UNKNOWN)));
  }

  public Map<String, IntTagged<ValueDesc>> columnToIndexAndType()
  {
    Map<String, IntTagged<ValueDesc>> mapping = Maps.newHashMap();
    for (int i = 0; i < size(); i++) {
      mapping.put(columnName(i), IntTagged.of(i, columnType(i)));
    }
    return mapping;
  }

  public List<String> getDimensionNames()
  {
    return columnName(type -> type != null && type.isDimension());
  }

  public List<String> getMetricNames()
  {
    return columnName(type -> type == null || !type.isDimension());
  }

  public List<String> columnName(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<String> predicated = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        predicated.add(columnNames.get(i));
      }
    }
    return predicated;
  }

  public String asTypeString()
  {
    final StringBuilder s = new StringBuilder();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append(pair.lhs).append(':').append(ValueDesc.toTypeString(pair.rhs));
    }
    return s.toString();
  }

  public Map<String, ValueDesc> asTypeMap()
  {
    final Map<String, ValueDesc> map = Maps.newHashMap();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      map.put(pair.lhs, pair.rhs);
    }
    return map;
  }

  public boolean anyType(Predicate<ValueDesc> predicate)
  {
    return Iterables.any(getColumnTypes(), predicate);
  }

  public IntStream indexOf(Predicate<ValueDesc> predicate)
  {
    List<ValueDesc> types = getColumnTypes();
    return IntStream.range(0, size()).filter(x -> predicate.apply(types.get(x)));
  }

  public int indexOf(String column)
  {
    return getColumnNames().indexOf(column);
  }

  public static final RowSignature EMPTY = of(Arrays.asList(), Arrays.asList());

  public RowSignature explodeNested()
  {
    if (columnTypes.stream().noneMatch(x -> x.isStruct() || x.isMap())) {
      return this;
    }
    Builder builder = new Builder();
    for (int i = 0; i < columnNames.size(); i++) {
      append(columnNames.get(i), columnTypes.get(i), builder);
    }
    return builder.build();
  }

  private void append(String name, ValueDesc type, Builder builder)
  {
    builder.append(name, type);
    if (type.isStruct()) {
      appendStruct(builder, name + ".", type);
    } else if (type.isMap()) {
      appendMap(builder, name + ".", type);
    }
  }

  RowSignature.Builder appendStruct(RowSignature.Builder builder, String prefix, ValueDesc struct)
  {
    String[] description = TypeUtils.splitDescriptiveType(struct);
    if (description == null) {
      return builder;   // cannot
    }
    for (int i = 1; i < description.length; i++) {
      String element = description[i].trim();
      int index = element.indexOf(":");
      Preconditions.checkArgument(index > 0, "'fieldName:fieldType' for field declaration");
      String name = prefix + element.substring(0, index).trim();
      ValueDesc type = ValueDesc.of(element.substring(index + 1).trim());
      append(name, type, builder);
    }
    return builder;
  }

  RowSignature.Builder appendMap(RowSignature.Builder builder, String prefix, ValueDesc struct)
  {
    String[] description = TypeUtils.splitDescriptiveType(struct);
    if (description == null) {
      return builder;   // cannot
    }
    builder.append(prefix + Row.MAP_KEY, ValueDesc.of(description[1]));
    builder.append(prefix + Row.MAP_VALUE, ValueDesc.of(description[2]));
    return builder;
  }

  // this is needed to be implemented by all post processors, but let's do it step by step
  public static interface Evolving
  {
    List<String> evolve(List<String> schema);

    RowSignature evolve(RowSignature schema);

    interface Identical extends Evolving
    {
      @Override
      default List<String> evolve(List<String> schema) {return schema;}

      @Override
      default RowSignature evolve(RowSignature schema) {return schema;}
    }
  }

  public static RowSignature of(List<String> columnNames, List<ValueDesc> columnTypes)
  {
    return new RowSignature(columnNames, columnTypes);
  }

  public static RowSignature fromTypeString(String typeString)
  {
    return fromTypeString(typeString, null);
  }

  public static RowSignature fromTypeString(String typeString, ValueDesc defaultType)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    for (String column : StringUtils.split(typeString, ',')) {
      final int index = column.indexOf(':');
      if (index < 0) {
        if (defaultType == null) {
          throw new IAE("missing type for %s in typeString %s", column, typeString);
        }
        columnNames.add(column.trim());
        columnTypes.add(defaultType);
      } else {
        columnNames.add(column.substring(0, index).trim());
        columnTypes.add(ValueDesc.fromTypeString(column.substring(index + 1).trim()));
      }
    }
    return RowSignature.of(columnNames, columnTypes);
  }

  public boolean containsAll(RowSignature signature)
  {
    for (int i = 0; i < signature.size(); i++) {
      final String column = signature.columnName(i);
      final int ix = columnNames.indexOf(column);
      if (ix < 0 || !columnTypes.get(ix).equals(signature.columnType(i))) {
        return false;
      }
    }
    return true;
  }

  private ValueDesc findElementOfStruct(String column, String element)
  {
    int index = columnNames.indexOf(column);
    if (index >= 0) {
      return nested(columnTypes.get(index), element);
    }
    return null;
  }

  private ValueDesc nested(ValueDesc type, String element)
  {
    String[] description = type.getDescription();
    if (!type.isStruct() || description == null) {
      return null;
    }
    for (int i = 1; i < description.length; i++) {
      int split = description[i].indexOf(':');
      String fieldName = description[i].substring(0, split);
      if (element.equals(fieldName)) {
        return ValueDesc.of(description[i].substring(split + 1));
      }
      for (int x = element.lastIndexOf('.'); x > 0; x = element.lastIndexOf('.', x - 1)) {
        if (element.substring(0, x).equals(fieldName)) {
          ValueDesc resolved = nested(ValueDesc.of(description[i].substring(split + 1)), element.substring(x + 1));
          if (resolved != null) {
            return resolved;
          }
        }
      }
    }
    return null;
  }

  public io.druid.data.Pair<String, ValueDesc> ordinal(int ix)
  {
    if (ix < columnNames.size()) {
      return io.druid.data.Pair.of(columnNames.get(ix), columnTypes.get(ix));
    }
    return null;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnNames, columnTypes);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowSignature)) {
      return false;
    }

    RowSignature that = (RowSignature) o;

    if (!Objects.equals(columnTypes, that.getColumnTypes())) {
      return false;
    }
    return Objects.equals(columnNames, that.getColumnNames());
  }

  @Override
  public String toString()
  {
    final StringBuilder s = new StringBuilder("{");
    for (int i = 0; i < columnNames.size(); i++) {
      if (i > 0) {
        s.append(", ");
      }
      final String columnName = columnNames.get(i);
      final ValueDesc columnType = columnTypes.get(i);
      s.append(columnName).append(":").append(columnType);
    }
    return s.append("}").toString();
  }

  public List<String> extractDimensionCandidates()
  {
    List<String> candidates = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (columnTypes.get(i).isDimension()) {
        candidates.add(columnNames.get(i));
      }
    }
    if (!candidates.isEmpty()) {
      return candidates;
    }
    for (int i = 0; i < columnTypes.size(); i++) {
      if (columnTypes.get(i).isString() || columnTypes.get(i).isMultiValued()) {
        candidates.add(columnNames.get(i));
      }
    }
    return candidates;
  }

  // for streaming sub query.. we don't have any index
  public RowSignature replaceDimensionToMV()
  {
    List<ValueDesc> replaced = Lists.newArrayList(getColumnTypes());
    for (int i = 0; i < replaced.size(); i++) {
      if (ValueDesc.isDimension(replaced.get(i))) {
        replaced.set(i, ValueDesc.MV_STRING);
      }
    }
    return RowSignature.of(getColumnNames(), replaced);
  }

  public RowSignature retain(List<String> columns)
  {
    return RowSignature.of(columns, GuavaUtils.transform(columns, name -> resolve(name, ValueDesc.UNKNOWN)));
  }

  public RowSignature concat(RowSignature other)
  {
    return RowSignature.of(
        GuavaUtils.concat(getColumnNames(), other.getColumnNames()),
        GuavaUtils.concat(getColumnTypes(), other.getColumnTypes())
    );
  }

  public RowSignature append(String name, ValueDesc type)
  {
    return RowSignature.of(
        GuavaUtils.concat(getColumnNames(), name),
        GuavaUtils.concat(getColumnTypes(), type)
    );
  }

  public RowSignature append(List<String> names, List<ValueDesc> types)
  {
    return RowSignature.of(
        GuavaUtils.concat(getColumnNames(), names),
        GuavaUtils.concat(getColumnTypes(), types)
    );
  }

  public RowSignature merge(RowSignature other)
  {
    final List<String> mergedColumns = Lists.newArrayList(columnNames);
    final List<ValueDesc> mergedTypes = Lists.newArrayList(columnTypes);

    final List<String> otherColumnNames = other.getColumnNames();
    final List<ValueDesc> otherColumnTypes = other.getColumnTypes();
    for (int i = 0; i < other.size(); i++) {
      final String otherColumn = otherColumnNames.get(i);
      final int index = mergedColumns.indexOf(otherColumn);
      if (index < 0) {
        mergedColumns.add(otherColumn);
        mergedTypes.add(otherColumnTypes.get(i));
      } else {
        ValueDesc type1 = resolve(otherColumn);
        ValueDesc type2 = other.resolve(otherColumn);
        if (!Objects.equals(type1, type2)) {
          mergedTypes.set(index, ValueDesc.toCommonType(type1, type2));
        }
      }
    }
    return new RowSignature(mergedColumns, mergedTypes);
  }

  public RowSignature alias(Map<String, String> alias)
  {
    if (!alias.isEmpty() && GuavaUtils.containsAny(alias.keySet(), columnNames)) {
      return RowSignature.of(_alias(columnNames, alias), columnTypes);
    }
    return this;
  }

  public static List<String> alias(List<String> columnNames, Map<String, String> alias)
  {
    if (!alias.isEmpty() && GuavaUtils.containsAny(alias.keySet(), columnNames)) {
      return _alias(columnNames, alias);
    }
    return columnNames;
  }

  private static List<String> _alias(List<String> columnNames, Map<String, String> alias)
  {
    List<String> aliased = Lists.newArrayList();
    for (String name : columnNames) {
      aliased.add(alias.getOrDefault(name, name));
    }
    return aliased;
  }

  public RowSignature rename(List<String> columnNames)
  {
    Preconditions.checkArgument(columnNames.size() == columnTypes.size());
    return of(columnNames, columnTypes);
  }

  public RowSignature exclude(Set<String> exclude)
  {
    Builder builder = new Builder();
    stream().filter(p -> !exclude.contains(p.lhs)).forEach(p -> builder.append(p.lhs, p.rhs));
    return builder.build();
  }

  public static class Builder
  {
    private final List<String> columnNames;
    private final List<ValueDesc> columnTypes;

    public Builder()
    {
      this.columnNames = Lists.newArrayList();
      this.columnTypes = Lists.newArrayList();
    }

    public Builder(RowSignature source)
    {
      this.columnNames = Lists.newArrayList(source.columnNames);
      this.columnTypes = Lists.newArrayList(source.columnTypes);
    }

    public Builder override(String columnName, ValueDesc columnType)
    {
      final int index = columnNames.indexOf(columnName);
      if (index < 0) {
        columnNames.add(columnName);
        columnTypes.add(columnType);
      } else {
        columnTypes.set(index, columnType);
      }
      return this;
    }

    public Builder append(String columnName, ValueDesc columnType)
    {
      columnNames.add(columnName);
      columnTypes.add(columnType);
      return this;
    }

    public RowSignature build()
    {
      return of(columnNames, columnTypes);
    }
  }
}
