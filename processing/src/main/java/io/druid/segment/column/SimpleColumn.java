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
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.Dictionary;

import java.io.IOException;
import java.util.Map;

/**
 */
class SimpleColumn implements Column
{
  private final String name;
  private final ColumnCapabilities capabilities;
  private final ColumnPartProvider.DictionarySupport dictionaryEncodedColumn;
  private final ColumnPartProvider<RunLengthColumn> runLengthColumn;
  private final ColumnPartProvider<GenericColumn> genericColumn;
  private final ColumnPartProvider<ComplexColumn> complexColumn;
  private final ColumnPartProvider<BitmapIndex> bitmapIndex;
  private final ColumnPartProvider<SpatialIndex> spatialIndex;
  private final ColumnPartProvider<HistogramBitmap> metricBitmap;
  private final ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap;
  private final ColumnPartProvider.ExternalPart<? extends SecondaryIndex> secondaryIndex;
  private final ColumnMeta columnMeta;

  SimpleColumn(
      String name,
      ColumnCapabilities capabilities,
      ColumnPartProvider.DictionarySupport dictionaryEncodedColumn,
      ColumnPartProvider<RunLengthColumn> runLengthColumn,
      ColumnPartProvider<GenericColumn> genericColumn,
      ColumnPartProvider<ComplexColumn> complexColumn,
      ColumnPartProvider<BitmapIndex> bitmapIndex,
      ColumnPartProvider<SpatialIndex> spatialIndex,
      ColumnPartProvider<HistogramBitmap> metricBitmap,
      ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap,
      ColumnPartProvider.ExternalPart<? extends SecondaryIndex> secondaryIndex,
      Map<String, Object> stats,
      Map<String, String> descs
  )
  {
    this.name = name;
    this.capabilities = Preconditions.checkNotNull(capabilities);
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    this.runLengthColumn = runLengthColumn;
    this.genericColumn = genericColumn;
    this.complexColumn = complexColumn;
    this.bitmapIndex = bitmapIndex;
    this.spatialIndex = spatialIndex;
    this.metricBitmap = metricBitmap;
    this.bitSlicedBitmap = bitSlicedBitmap;
    this.secondaryIndex = secondaryIndex;
    this.columnMeta = new ColumnMeta(
        capabilities.getTypeDesc(), capabilities.hasMultipleValues(), descs, stats
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
        return column.getNumRows();
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
      case LUCENE_INDEX:
        return secondaryIndex == null ? -1 : secondaryIndex.getSerializedSize();
    }
    return -1;
  }

  @Override
  public Dictionary<String> getDictionary()
  {
    return dictionaryEncodedColumn == null ? null : dictionaryEncodedColumn.getDictionary();
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoding()
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
  @SuppressWarnings("unchecked")
  public <T extends SecondaryIndex> T getSecondaryIndex()
  {
    return secondaryIndex == null ? null : (T) secondaryIndex.get();
  }

  @Override
  public String sourceOfSecondaryIndex()
  {
    return secondaryIndex == null ? null : secondaryIndex.source();
  }

  @Override
  public Class classOfSecondaryIndex()
  {
    return secondaryIndex == null ? null : secondaryIndex.classOfObject();
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
}
