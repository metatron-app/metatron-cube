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

package io.druid.segment.column;

import com.google.common.base.Preconditions;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.ColumnStats;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.Dictionary;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 */
class SimpleColumn implements Column
{
  private final String name;
  private final ColumnCapabilities capabilities;
  private final ColumnPartProvider.DictionarySupport dictionaryEncodedColumn;
  private final ExternalIndexProvider<FSTHolder> fstIndex;
  private final ColumnPartProvider<RunLengthColumn> runLengthColumn;
  private final ColumnPartProvider<GenericColumn> genericColumn;
  private final ColumnPartProvider<ComplexColumn> complexColumn;
  private final ColumnPartProvider<BitmapIndex> bitmapIndex;
  private final ColumnPartProvider<SpatialIndex> spatialIndex;
  private final ColumnPartProvider<HistogramBitmap> metricBitmap;
  private final ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap;
  private final Map<Class, ExternalIndexProvider> secondaryIndices;
  private final ColumnMeta columnMeta;

  SimpleColumn(
      String name,
      ColumnCapabilities capabilities,
      ColumnPartProvider.DictionarySupport dictionaryEncodedColumn,
      ExternalIndexProvider<FSTHolder> fstIndex,
      ColumnPartProvider<RunLengthColumn> runLengthColumn,
      ColumnPartProvider<GenericColumn> genericColumn,
      ColumnPartProvider<ComplexColumn> complexColumn,
      ColumnPartProvider<BitmapIndex> bitmapIndex,
      ColumnPartProvider<SpatialIndex> spatialIndex,
      ColumnPartProvider<HistogramBitmap> metricBitmap,
      ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap,
      Map<Class, ExternalIndexProvider> secondaryIndices,
      Map<String, Object> stats,
      Map<String, String> descs
  )
  {
    this.name = name;
    this.capabilities = Preconditions.checkNotNull(capabilities);
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    this.fstIndex = fstIndex;
    this.runLengthColumn = runLengthColumn;
    this.genericColumn = genericColumn;
    this.complexColumn = complexColumn;
    this.bitmapIndex = bitmapIndex;
    this.spatialIndex = spatialIndex;
    this.metricBitmap = metricBitmap;
    this.bitSlicedBitmap = bitSlicedBitmap;
    this.secondaryIndices = secondaryIndices;
    ImmutableBitmap bitmap = nullBitmap();
    this.columnMeta = new ColumnMeta(
        capabilities.getTypeDesc(),
        capabilities.hasMultipleValues(),
        descs,
        stats != null ? stats : stats(),
        bitmap != null ? !bitmap.isEmpty() : null,
        bitmap
    );
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public ColumnMeta getMetaData()
  {
    return columnMeta;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public int getNumRows()
  {
    if (genericColumn != null) {
      try (GenericColumn column = genericColumn.get()) {
        return column.size();
      }
      catch (Throwable e) {
        // ignore
      }
    } else if (dictionaryEncodedColumn != null) {
      try (DictionaryEncodedColumn column = dictionaryEncodedColumn.get()) {
        return column.length();
      }
      catch (IOException e) {
        // ignore
      }
    }
    return -1;
  }

  @Override
  public boolean hasDictionaryEncodedColumn()
  {
    return dictionaryEncodedColumn != null;
  }

  @Override
  public boolean hasGenericColumn()
  {
    return genericColumn != null;
  }

  @Override
  public Class<? extends GenericColumn> getGenericColumnType()
  {
    return genericColumn == null ? null : genericColumn.provides();
  }

  @Override
  public boolean hasComplexColumn()
  {
    return complexColumn != null;
  }

  @Override
  public CompressionStrategy compressionType()
  {
    if (genericColumn != null) {
      return genericColumn.compressionType();
    } else if (complexColumn != null) {
      return complexColumn.compressionType();
    } else if (dictionaryEncodedColumn != null) {
      return dictionaryEncodedColumn.compressionType();
    }
    return null;
  }

  @Override
  public long getSerializedSize(EncodeType encodeType)
  {
    switch (encodeType) {
      case DICTIONARY_ENCODED:
        return dictionaryEncodedColumn == null ? -1 : dictionaryEncodedColumn.getSerializedSize();
      case RUNLENGTH_ENCODED:
        return runLengthColumn == null ? -1 : runLengthColumn.getSerializedSize();
      case GENERIC:
        return genericColumn == null ? -1 : genericColumn.getSerializedSize();
      case COMPLEX:
        return complexColumn == null ? -1 : complexColumn.getSerializedSize();
      case BITMAP:
        return bitmapIndex == null ? -1 : bitmapIndex.getSerializedSize();
      case SPATIAL:
        return spatialIndex == null ? -1 : spatialIndex.getSerializedSize();
      case METRIC_BITMAP:
        return metricBitmap == null ? -1 : metricBitmap.getSerializedSize();
      case BITSLICED_BITMAP:
        return bitSlicedBitmap == null ? -1 : bitSlicedBitmap.getSerializedSize();
      case FST:
        return fstIndex == null ? -1 : fstIndex.getSerializedSize();
    }
    return -1;
  }

  @Override
  public Dictionary<String> getDictionary()
  {
    return dictionaryEncodedColumn == null ? null : dictionaryEncodedColumn.getDictionary();
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoded()
  {
    return dictionaryEncodedColumn == null ? null : dictionaryEncodedColumn.get();
  }

  @Override
  public RunLengthColumn getRunLengthColumn()
  {
    return runLengthColumn == null ? null : runLengthColumn.get();
  }

  @Override
  public GenericColumn getGenericColumn()
  {
    return genericColumn == null ? null : genericColumn.get();
  }

  @Override
  public ComplexColumn getComplexColumn()
  {
    return complexColumn == null ? null : complexColumn.get();
  }

  @Override
  public BitmapIndex getBitmapIndex()
  {
    return bitmapIndex == null ? null : bitmapIndex.get();
  }

  @Override
  public SpatialIndex getSpatialIndex()
  {
    return spatialIndex == null ? null : spatialIndex.get();
  }

  @Override
  public HistogramBitmap getMetricBitmap()
  {
    return metricBitmap == null ? null : metricBitmap.get();
  }

  @Override
  public BitSlicedBitmap getBitSlicedBitmap()
  {
    return bitSlicedBitmap == null ? null : bitSlicedBitmap.get();
  }

  @Override
  public Set<Class> getExternalIndexKeys()
  {
    return secondaryIndices.keySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ExternalIndexProvider<T> getExternalIndex(Class<T> clazz)
  {
    return secondaryIndices.get(clazz);
  }

  @Override
  public ExternalIndexProvider<FSTHolder> getFST()
  {
    return fstIndex;
  }

  @Override
  public Map<String, Object> getColumnStats()
  {
    return columnMeta.getStats();
  }

  @Override
  public Map<String, String> getColumnDescs()
  {
    return columnMeta.getDescs();
  }

  private ImmutableBitmap nullBitmap()
  {
    if (dictionaryEncodedColumn != null) {
      Dictionary<String> dictionary = dictionaryEncodedColumn.getDictionary();
      Boolean containsNull = dictionary.containsNull();
      if (containsNull == null) {
        return null;
      }
      BitmapIndex bitmaps = bitmapIndex.get();
      if (containsNull) {
        return bitmaps.getBitmap(0);
      }
      return bitmaps.getBitmapFactory().makeEmptyImmutableBitmap();
    }
    if (genericColumn != null) {
      return genericColumn.get().getNulls();
    }
    return null;
  }

  // stats are not included in descriptor of dimensions
  private Map<String, Object> stats()
  {
    if (dictionaryEncodedColumn == null) {
      return null;
    }
    Dictionary<String> dictionary = dictionaryEncodedColumn.getDictionary();
    if (dictionary == null || dictionary.isEmpty() || !dictionary.isSorted()) {
      return null;
    }
    if (!dictionary.containsNull()) {
      return GuavaUtils.mutableMap(
          ColumnStats.MIN, dictionary.get(0),
          ColumnStats.MAX, dictionary.get(dictionary.size() - 1),
          ColumnStats.NUM_NULLS, 0
      );
    }
    if (dictionary.size() > 1) {
      return GuavaUtils.mutableMap(
          ColumnStats.MIN, dictionary.get(1),
          ColumnStats.MAX, dictionary.get(dictionary.size() - 1),
          ColumnStats.NUM_NULLS, bitmapIndex.get().getBitmap(0).size()
      );
    }
    return GuavaUtils.mutableMap(
        ColumnStats.NUM_NULLS, dictionaryEncodedColumn.numRows()
    );
  }
}
