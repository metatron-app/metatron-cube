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

package io.druid.segment;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.BooleanGenericColumn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.BitSlicedBitmap;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class QueryableIndexSelector implements BitmapIndexSelector
{
  private final QueryableIndex index;
  private final TypeResolver resolver;
  private final BitmapFactory bitmapFactory;
  private final int numRows;
  private final Map<String, Map<Class, SecondaryIndex>> externalIndices = Maps.newHashMap();
  private final Map<String, HistogramBitmap> metricBitmaps = Maps.newHashMap();
  private final Map<String, BitSlicedBitmap> bitSlicedBitmapMaps = Maps.newHashMap();

  public QueryableIndexSelector(QueryableIndex index, TypeResolver resolver)
  {
    this.index = index;
    this.resolver = resolver;
    this.bitmapFactory = index.getBitmapFactoryForDimensions();
    this.numRows = index.getNumRows();
  }

  @Override
  public ValueDesc resolve(String column)
  {
    return resolver.resolve(column);
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public BitmapIndex getBitmapIndex(String dimension)
  {
    final Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      return column.getBitmapIndex();
    } else {
      return null;
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, String value)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      return makeBooleanBitmap(Strings.isNullOrEmpty(value));
    }
    if (column.getType().isBitSet() && Strings.isNullOrEmpty(value)) {
      return null;    // todo write null bitmap for complex column
    }
    final BitmapIndex bitmap = column.getBitmapIndex();
    return bitmap == null ? null : bitmap.getBitmap(bitmap.getIndex(value));
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, Boolean value)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      return makeBooleanBitmap(value == null);
    }
    final GenericColumn genericColumn = column.getGenericColumn();
    if (genericColumn instanceof BooleanGenericColumn) {
      final ImmutableBitmap nulls = genericColumn.getNulls();
      if (value == null) {
        return nulls;
      }
      ImmutableBitmap values = ((BooleanGenericColumn) genericColumn).getValues();
      if (!value) {
        values = DimFilters.complement(bitmapFactory, values, numRows);
      }
      if (nulls.isEmpty()) {
        return values;
      }
      return DimFilters.intersection(
          bitmapFactory, Arrays.asList(DimFilters.complement(bitmapFactory, nulls, numRows), values)
      );
    }
    return null;
  }

  @Override
  public ImmutableRTree getSpatialIndex(String dimension)
  {
    final Column column = index.getColumn(dimension);
    if (column == null || !column.getCapabilities().hasSpatialIndexes()) {
      return new ImmutableRTree();
    }

    return column.getSpatialIndex().getRTree();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends SecondaryIndex> T getExternalIndex(String dimension, Class<T> clazz)
  {
    Map<Class, SecondaryIndex> mapping = externalIndices.computeIfAbsent(dimension, k -> Maps.newHashMap());
    T secondary = (T) mapping.get(clazz);
    if (secondary == null) {
      Column column = index.getColumn(dimension);
      ExternalIndexProvider<T> provider = column == null ? null : column.getExternalIndex(clazz);
      if (provider != null) {
        mapping.put(clazz, secondary = provider.get());
      }
    }
    return secondary;
  }

  @Override
  public HistogramBitmap getMetricBitmap(String dimension)
  {
    HistogramBitmap metric = metricBitmaps.get(dimension);
    if (metric == null) {
      final Column column = index.getColumn(dimension);
      if (column == null || !column.getCapabilities().hasMetricBitmap()) {
        return null;
      }
      metricBitmaps.put(dimension, metric = column.getMetricBitmap());
    }
    return metric;
  }

  @Override
  public BitSlicedBitmap getBitSlicedBitmap(String dimension)
  {
    BitSlicedBitmap metric = bitSlicedBitmapMaps.get(dimension);
    if (metric == null) {
      final Column column = index.getColumn(dimension);
      if (column == null || !column.getCapabilities().hasBitSlicedBitmap()) {
        return null;
      }
      bitSlicedBitmapMaps.put(dimension, metric = column.getBitSlicedBitmap());
    }
    return metric;
  }

  @Override
  public ColumnCapabilities getCapabilities(String dimension)
  {
    Column column = index.getColumn(dimension);
    return column == null ? null : column.getCapabilities();
  }

  @Override
  public Column getColumn(String dimension)
  {
    return index.getColumn(dimension);
  }

  @Override
  public void close()
  {
    for (Map<Class, SecondaryIndex> indices : externalIndices.values()) {
      for (SecondaryIndex index : indices.values()) {
        CloseQuietly.close(index);
      }
    }
    for (SecondaryIndex bitmap : metricBitmaps.values()) {
      CloseQuietly.close(bitmap);
    }
    for (SecondaryIndex bitmap : bitSlicedBitmapMaps.values()) {
      CloseQuietly.close(bitmap);
    }
    externalIndices.clear();
    metricBitmaps.clear();
    bitSlicedBitmapMaps.clear();
  }

  @Override
  public QueryableIndex internal()
  {
    return index;
  }

  private ImmutableBitmap makeBooleanBitmap(boolean bool)
  {
    if (bool) {
      return DimFilters.makeTrue(bitmapFactory, numRows);
    } else {
      return DimFilters.makeFalse(bitmapFactory);
    }
  }
}
