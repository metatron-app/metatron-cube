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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.query.select.Schema;
import io.druid.segment.StringArray;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class UnpivotSpec implements LateralViewSpec, Function<Map<String, Object>, Iterable<Map<String, Object>>>
{
  private final List<ColumnElement> elements;
  private final List<String> retains;
  private final List<String> excludes;
  private final String metricName;
  private final String separator;

  private final String[] columnNames;
  private final int[] nonNulls;
  private final int[] nulls;
  private final Set<String> excludeSet;
  private final Object[] retainer;

  @JsonCreator
  public UnpivotSpec(
      @JsonProperty("elements") List<ColumnElement> elements,
      @JsonProperty("retains") List<String> retains,
      @JsonProperty("excludes") List<String> excludes,
      @JsonProperty("separator") String separator,
      @JsonProperty("metricName") String metricName
  )
  {
    this.elements = elements;
    this.excludes = excludes;
    this.retains = retains == null ? Collections.<String>emptyList() : retains;
    this.separator = Strings.isNullOrEmpty(separator) ? "-" : separator;
    this.metricName = metricName;
    Preconditions.checkArgument(elements != null && !elements.isEmpty(), "'elements' should not be null or empty");
    List<String> columnNames = Lists.newArrayList();
    List<Integer> nonNulls = Lists.newArrayList();
    List<Integer> nulls = Lists.newArrayList();
    for (int i = 0; i < elements.size(); i++) {
      String columnName = elements.get(i) == null ? null : elements.get(i).getOutput();
      columnNames.add(columnName);
      if (columnName != null) {
        nonNulls.add(i);
      } else {
        nulls.add((i));
      }
    }
    this.columnNames = columnNames.toArray(new String[0]);
    this.nonNulls = Ints.toArray(nonNulls);
    this.nulls = Ints.toArray(nulls);
    this.retainer = new Object[retains == null ? 0 : retains.size()];
    Set<String> excludeSet = Sets.newHashSet();
    if (excludes != null) {
      excludeSet.addAll(excludes);
    }
    if (retains != null) {
      excludeSet.addAll(retains);
    }
    this.excludeSet = excludeSet.isEmpty() ? null : excludeSet;
  }

  @JsonProperty
  public List<ColumnElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public List<String> getRetains()
  {
    return retains;
  }

  @JsonProperty
  public List<String> getExcludes()
  {
    return excludes;
  }

  @JsonProperty
  public String getSeparator()
  {
    return separator;
  }

  @JsonProperty
  public String getMetricName()
  {
    return metricName;
  }

  @Override
  public Function<Map<String, Object>, Iterable<Map<String, Object>>> prepare()
  {
    return this;
  }

  @Override
  public Iterable<Map<String, Object>> apply(Map<String, Object> event)
  {
    for (int i = 0; i < retainer.length; i++) {
      retainer[i] = event.get(retains.get(i));
    }
    final Map<StringArray, Map<String, Object>> exploded = Maps.newLinkedHashMap();
    for (Map.Entry<String, Object> entry : event.entrySet()) {
      if (excludeSet != null && excludeSet.contains(entry.getKey())) {
        continue;
      }
      final String[] split = split(entry.getKey());
      if (split == null) {
        continue;
      }
      StringArray array = toPartitionKey(split);
      Map<String, Object> values = exploded.get(array);
      if (values == null) {
        exploded.put(array, values = Maps.newHashMap());
        for (int i = 0; i < retainer.length; i++) {
          values.put(retains.get(i), retainer[i]);
        }
      }
      for (int i = 0; i < columnNames.length; i++) {
        if (columnNames[i] != null) {
          values.put(columnNames[i], split[i]);
        }
      }
      values.put(toMetricName(split), entry.getValue());
    }
    return exploded.values();
  }

  private String[] split(String inputColumnName)
  {
    if (Strings.isNullOrEmpty(inputColumnName)) {
      return null;
    }
    String[] split = StringUtils.split(inputColumnName, separator);
    if (split.length < columnNames.length) {
      return null;
    }
    for (int i = 0; i < columnNames.length; i++) {
      if (elements.get(i) != null && !elements.get(i).accept(split[i])) {
        return null;
      }
    }
    return split;
  }

  private StringArray toPartitionKey(String[] split)
  {
    if (nonNulls.length == 0 && split.length == columnNames.length) {
      return new StringArray(split);
    }
    String[] partitionKey = new String[nonNulls.length];
    for (int i = 0; i < nonNulls.length; i++) {
      partitionKey[i] = split[nonNulls[i]];
    }
    return new StringArray(partitionKey);
  }

  private String toMetricName(String[] split)
  {
    if (nulls.length == 0) {
      if (split.length > columnNames.length) {
        return StringUtils.join(Arrays.copyOfRange(split, columnNames.length, split.length), separator);
      }
      return metricName;
    }
    StringBuilder b = new StringBuilder();
    for (int index : nulls) {
      if (b.length() > 0) {
        b.append(separator);
      }
      b.append(split[index]);
    }
    for (int i = columnNames.length; i < split.length; i++) {
      if (b.length() > 0) {
        b.append(separator);
      }
      b.append(split[i]);
    }
    return b.toString();
  }

  @Override
  public List<String> resolve(List<String> schema)
  {
    throw new UnsupportedOperationException("todo");
  }

  @Override
  public Schema resolve(Query query, Schema schema, ObjectMapper mapper)
  {
    throw new UnsupportedOperationException("todo");
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

    UnpivotSpec that = (UnpivotSpec) o;

    if (!elements.equals(that.elements)) {
      return false;
    }
    if (!retains.equals(that.retains)) {
      return false;
    }
    if (excludes != null ? !excludes.equals(that.excludes) : that.excludes != null) {
      return false;
    }
    if (metricName != null ? !metricName.equals(that.metricName) : that.metricName != null) {
      return false;
    }
    if (!separator.equals(that.separator)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = elements.hashCode();
    result = 31 * result + retains.hashCode();
    result = 31 * result + (excludes != null ? excludes.hashCode() : 0);
    result = 31 * result + (metricName != null ? metricName.hashCode() : 0);
    result = 31 * result + separator.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ExplodeSpec{" +
           "elements=" + elements +
           ", retains=" + retains +
           ", excludes=" + excludes +
           ", separator='" + separator + '\'' +
           ", metricName='" + metricName + '\'' +
           '}';
  }

  public static class ColumnElement implements Cacheable
  {
    private final String output;
    private final List<String> values;

    private final Set<String> valueSet;

    @JsonCreator
    public ColumnElement(
        @JsonProperty("output") String output,
        @JsonProperty("columnValues") List<String> values
    )
    {
      this.output = output;
      this.values = values;
      this.valueSet = values == null || values.isEmpty() ? null : Sets.newHashSet(values);
    }

    @JsonProperty
    public String getOutput()
    {
      return output;
    }

    @JsonProperty
    public List<String> getValues()
    {
      return values;
    }

    public boolean accept(String value)
    {
      return valueSet == null || valueSet.contains(value);
    }

    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(output).sp()
                    .append(values);
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

      ColumnElement that = (ColumnElement) o;

      if (output != null ? !output.equals(that.output) : that.output != null) {
        return false;
      }
      if (values != null ? !values.equals(that.values) : that.values != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(output, values);
    }

    @Override
    public String toString()
    {
      return "ExplodeElement{" +
             "column='" + output + '\'' +
             ", values=" + values +
             '}';
    }
  }
}
