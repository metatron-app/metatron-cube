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
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.select.Schema;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;

import java.util.Iterator;
import java.util.Map;

/**
 */
public class ColumnSelectorBitmapIndexSelector implements BitmapIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final QueryableIndex index;
  private final int numRows;
  private final Map<String, LuceneIndex> luceneIndices = Maps.newHashMap();
  private final Map<String, HistogramBitmap> metricBitmaps = Maps.newHashMap();
  private final Map<String, BitSlicedBitmap> bitSlicedBitmapMaps = Maps.newHashMap();

  public ColumnSelectorBitmapIndexSelector(
      final BitmapFactory bitmapFactory,
      final QueryableIndex index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.index = index;
    this.numRows = index.getNumRows();
  }

  @Override
  public Schema getSchema(boolean prependTime)
  {
    return index.asSchema(prependTime);
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
      if (Strings.isNullOrEmpty(value)) {
        return DimFilters.makeTrue(bitmapFactory, getNumRows());
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
  public void close()
  {
    for (SecondaryIndex bitmap : luceneIndices.values()) {
      CloseQuietly.close(bitmap);
    }
    for (SecondaryIndex bitmap : metricBitmaps.values()) {
      CloseQuietly.close(bitmap);
    }
    for (SecondaryIndex bitmap : bitSlicedBitmapMaps.values()) {
      CloseQuietly.close(bitmap);
    }
    luceneIndices.clear();
    metricBitmaps.clear();
    bitSlicedBitmapMaps.clear();
  }
}
