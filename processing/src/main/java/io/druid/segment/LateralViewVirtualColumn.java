/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.common.guava.DSuppliers.HandOver;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.QueryCacheHelper;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 */
public class LateralViewVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x03;

  private final String outputName;
  private final List<String> excludes;
  private final List<String> values;
  private final String metricName;

  private final Set<String> valueSet;
  private final Set<String> excludeSet;
  private final LVIndexHolder indexer;

  @JsonCreator
  public LateralViewVirtualColumn(
      @JsonProperty("outputName") String outputName,
      @JsonProperty("metricName") String metricName,
      @JsonProperty("excludes") List<String> excludes,
      @JsonProperty("values") List<String> values
  )
  {
    Preconditions.checkArgument(outputName != null, "output name should not be null");
    Preconditions.checkArgument(metricName != null, "metric name should not be null");

    this.outputName = outputName;
    this.metricName = metricName;
    this.excludes = excludes;
    this.values = values;

    this.valueSet = values != null && !values.isEmpty() ? Sets.newHashSet(values) : null;
    Set<String> excludeSet = Sets.newHashSet();
    if (excludes != null) {
      excludeSet.addAll(excludes);
    }
    this.excludeSet = excludes != null && !excludeSet.isEmpty() ? Sets.newHashSet(excludeSet) : null;
    this.indexer = new LVIndexHolder();
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public String getMetricName()
  {
    return metricName;
  }

  @JsonProperty
  public List<String> getExcludes()
  {
    return excludes;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    if (column.equals(outputName)) {
      return ValueDesc.STRING;
    } else if (!column.equals(metricName)) {
      return ValueDesc.toCommonType(Iterables.transform(values, ValueDesc.resolving(types)), ValueDesc.UNKNOWN);
    } else {
      return types.resolve(column, ValueDesc.UNKNOWN);
    }
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, final ColumnSelectorFactory factory)
  {
    if (!dimension.equals(metricName)) {
      throw new ISE("This virtual columns provides only metric %s", metricName);
    }
    final ValueDesc commonType = ValueDesc.toCommonType(
        Iterables.transform(values, ValueDesc.resolving(factory)), ValueDesc.UNKNOWN
    );
    if (commonType == null || commonType.isUnknown()) {
      throw new ISE("Cannot resolve common type os values %s", values);
    }
    return new ObjectColumnSelector<Object>()
    {
      private transient ObjectColumnSelector[] selectors;

      @Override
      public ValueDesc type()
      {
        return commonType;
      }

      @Override
      public Object get()
      {
        if (selectors == null) {
          final List<String> columns = indexer.columnNames.get();
          if (columns == null) {
            throw new IllegalStateException("This virtual columns only can be used with group-by/top-N query");
          }
          selectors = new ObjectColumnSelector[columns.size()];
          for (int i = 0; i < selectors.length; i++) {
            selectors[i] = factory.makeObjectColumnSelector(columns.get(i));
          }
        }
        return selectors[indexer.index()].get();
      }
    };
  }

  @Override
  public FloatColumnSelector asFloatMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asFloat(asMetric(metricName, factory));
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asDouble(asMetric(metricName, factory));
  }

  @Override
  public LongColumnSelector asLongMetric(String column, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asLong(asMetric(metricName, factory));
  }

  @Override
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    if (!dimension.equals(outputName)) {
      throw new IllegalStateException("Only can be called as a group-by/top-N dimension");
    }
    if (extractionFn != null) {
      throw new UnsupportedOperationException("not supported yet");
    }
    final List<String> targetColumns = Lists.newArrayList();
    for (String column : factory.getColumnNames()) {
      if (excludeSet != null && excludes.contains(column)) {
        continue;
      }
      if (valueSet != null && !valueSet.contains(column)) {
        continue;
      }
      targetColumns.add(column);
    }
    indexer.columnNames.set(targetColumns);

    final Set<String> metricColumns = ImmutableSet.of(metricName);
    final DimensionSelector selector = VirtualColumns.toFixedDimensionSelector(targetColumns);

    return new IndexProvidingSelector.Delegated(selector)
    {
      @Override
      public final IndexedInts getRow()
      {
        return indexer.indexed(super.getRow());
      }

      @Override
      public final ColumnSelectorFactory wrapFactory(final ColumnSelectorFactory factory)
      {
        return new VirtualColumns.VirtualColumnAsColumnSelectorFactory(
            LateralViewVirtualColumn.this, factory, outputName, metricColumns
        );
      }

      @Override
      public Set<String> targetColumns()
      {
        return metricColumns;
      }
    };
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new LateralViewVirtualColumn(outputName, metricName, excludes, values);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] outputBytes = StringUtils.toUtf8(outputName);
    byte[] metricBytes = StringUtils.toUtf8(metricName);
    byte[] excludesBytes = QueryCacheHelper.computeCacheBytes(excludes);
    byte[] valuesBytes = QueryCacheHelper.computeCacheBytes(values);

    return ByteBuffer.allocate(4 + excludesBytes.length + valuesBytes.length + outputBytes.length + metricBytes.length)
                     .put(VC_TYPE_ID)
                     .put(outputBytes).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(metricBytes).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(excludesBytes).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valuesBytes)
                     .array();
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

    LateralViewVirtualColumn that = (LateralViewVirtualColumn) o;

    if (!outputName.equals(that.outputName)) {
      return false;
    }
    if (!metricName.equals(that.metricName)) {
      return false;
    }
    if (excludes != null ? !excludes.equals(that.excludes) : that.excludes != null) {
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
    int result = outputName.hashCode();
    result = 31 * result + metricName.hashCode();
    result = 31 * result + (excludes != null ? excludes.hashCode() : 0);
    result = 31 * result + (values != null ? values.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "LateralViewVirtualColumn{" +
           "outputName='" + outputName + '\'' +
           ", metricName='" + metricName + '\'' +
           ", excludes=" + excludes +
           ", values=" + values +
           '}';
  }

  private static class LVIndexHolder extends IndexProvidingSelector.IndexHolder
  {
    private final HandOver<List<String>> columnNames = new HandOver<>();
  }
}
