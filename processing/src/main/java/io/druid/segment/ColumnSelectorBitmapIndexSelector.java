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

package io.druid.segment;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.guava.CloseQuietly;
import io.druid.data.ValueDesc;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ExternalBitmap;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.MetricBitmap;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class ColumnSelectorBitmapIndexSelector implements BitmapIndexSelector, Closeable
{
  private final BitmapFactory bitmapFactory;
  private final ColumnSelector index;
  private final Map<String, LuceneIndex> luceneIndices = Maps.newHashMap();
  private final Map<String, MetricBitmap> metricBitmaps = Maps.newHashMap();

  public ColumnSelectorBitmapIndexSelector(
      final BitmapFactory bitmapFactory,
      final ColumnSelector index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.index = index;
  }

  public ColumnSelectorBitmapIndexSelector(
      final BitmapFactory bitmapFactory,
      final String name,
      final Column column
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.index = new ColumnSelector()
    {
      @Override
      public Indexed<String> getColumnNames()
      {
        return new ListIndexed<String>(Arrays.asList(name), String.class);
      }

      @Override
      public Column getColumn(String columnName)
      {
        return name.equals(columnName) ? column : null;
      }

      @Override
      public ValueDesc getColumnType(String columnName)
      {
        return index.getColumnType(columnName);
      }
    };
  }

  @Override
  public Indexed<String> getDimensionValues(String dimension)
  {
    final Column columnDesc = index.getColumn(dimension);
    if (columnDesc == null || !columnDesc.getCapabilities().isDictionaryEncoded()) {
      return null;
    }
    final GenericIndexed<String> column = columnDesc.getDictionary();
    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return column.size();
      }

      @Override
      public String get(int index)
      {
        return column.get(index);
      }

      @Override
      public int indexOf(String value)
      {
        return column.indexOf(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public int getNumRows()
  {
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return column.length();
    }
    finally {
      CloseQuietly.close(column);
    }
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
      if (Strings.isNullOrEmpty(value)) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }

    if (!column.getCapabilities().hasBitmapIndexes()) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final BitmapIndex bitmapIndex = column.getBitmapIndex();
    return bitmapIndex.getBitmap(bitmapIndex.getIndex(value));
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
  public LuceneIndex getLuceneIndex(String dimension)
  {
    LuceneIndex lucene = luceneIndices.get(dimension);
    if (lucene == null) {
      final Column column = index.getColumn(dimension);
      if (column == null || !column.getCapabilities().hasLuceneIndex()) {
        return null;
      }
      luceneIndices.put(dimension, lucene = column.getLuceneIndex());
    }
    return lucene;
  }

  @Override
  public MetricBitmap getMetricBitmap(String dimension)
  {
    MetricBitmap metric = metricBitmaps.get(dimension);
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
  public ColumnCapabilities getCapabilities(String dimension)
  {
    Column column = index.getColumn(dimension);
    return column == null ? null : column.getCapabilities();
  }

  @Override
  public void close() throws IOException
  {
    for (ExternalBitmap bitmap : luceneIndices.values()) {
      CloseQuietly.close(bitmap);
    }
    for (ExternalBitmap bitmap : metricBitmaps.values()) {
      CloseQuietly.close(bitmap);
    }
    luceneIndices.clear();
    metricBitmaps.clear();
  }
}
