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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.common.Cacheable;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class LateralViewSpec implements Cacheable, Function<Map<String, Object>, Iterable<Map<String, Object>>>
{
  private final List<LateralViewElement> elements;
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
  public LateralViewSpec(
      @JsonProperty("elements") List<LateralViewElement> elements,
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
    Preconditions.checkArgument(elements != null && !elements.isEmpty(), "'columns' should not be null or empty");
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
    this.columnNames = columnNames.toArray(new String[columnNames.size()]);
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
  public List<LateralViewElement> getElements()
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
  public byte[] getCacheKey()
  {
    byte[] elementsBytes = QueryCacheHelper.computeAggregatorBytes(elements);
    byte[] excludesBytes = QueryCacheHelper.computeCacheBytes(excludes);
    byte[] retainsBytes = QueryCacheHelper.computeCacheBytes(retains);
    byte[] separatorBytes = io.druid.common.utils.StringUtils.toUtf8(separator);
    byte[] metricNameBytes = io.druid.common.utils.StringUtils.toUtf8WithNullToEmpty(metricName);

    int length = 4
                 + elementsBytes.length
                 + excludesBytes.length
                 + retainsBytes.length
                 + separatorBytes.length
                 + metricNameBytes.length;

    return ByteBuffer.allocate(length)
                     .put(elementsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(excludesBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(retainsBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(separatorBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(metricNameBytes)
                     .array();
  }

  @Override
  public Iterable<Map<String, Object>> apply(Map<String, Object> event)
  {
    for (int i = 0; i < retainer.length; i++) {
      retainer[i] = event.get(retains.get(i));
    }
    final Map<ObjectArray, Map<String, Object>> exploded = Maps.newLinkedHashMap();
    for (Map.Entry<String, Object> entry : event.entrySet()) {
      if (excludeSet != null && excludeSet.contains(entry.getKey())) {
        continue;
      }
      final String[] split = split(entry.getKey());
      if (split == null) {
        continue;
      }
      ObjectArray array = toPartitionKey(split);
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

  private ObjectArray toPartitionKey(String[] split)
  {
    if (nonNulls.length == 0 && split.length == columnNames.length) {
      return new ObjectArray(split);
    }
    String[] partitionKey = new String[nonNulls.length];
    for (int i = 0; i < nonNulls.length; i++) {
      partitionKey[i] = split[nonNulls[i]];
    }
    return new ObjectArray(partitionKey);
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LateralViewSpec that = (LateralViewSpec) o;

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

  public static class LateralViewElement implements Cacheable
  {
    private final String output;
    private final List<String> values;

    private final Set<String> valueSet;

    @JsonCreator
    public LateralViewElement(
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
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LateralViewElement that = (LateralViewElement) o;

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

    @Override
    public byte[] getCacheKey()
    {
      byte[] outputBytes = io.druid.common.utils.StringUtils.toUtf8WithNullToEmpty(output);
      byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(values);

      int length = 1 + outputBytes.length + valuesBytes.length;
      return ByteBuffer.allocate(length)
                       .put(outputBytes)
                       .put(DimFilterCacheHelper.STRING_SEPARATOR)
                       .put(valuesBytes)
                       .array();
    }
  }
}
