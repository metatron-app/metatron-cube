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

package io.druid.segment.column;

import io.druid.segment.ColumnPartProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.util.Map;

/**
 */
class SimpleColumn implements Column
{
  private final ColumnCapabilities capabilities;
  private final ColumnPartProvider.DictionarySupport dictionaryEncodedColumn;
  private final ColumnPartProvider<RunLengthColumn> runLengthColumn;
  private final ColumnPartProvider<GenericColumn> genericColumn;
  private final ColumnPartProvider<ComplexColumn> complexColumn;
  private final ColumnPartProvider<BitmapIndex> bitmapIndex;
  private final ColumnPartProvider<SpatialIndex> spatialIndex;
  private final ColumnPartProvider<HistogramBitmap> metricBitmap;
  private final ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap;
  private final ColumnPartProvider<LuceneIndex> luceneIndex;
  private final Map<String, Object> stats;
  private final Map<String, String> descs;

  SimpleColumn(
      ColumnCapabilities capabilities,
      ColumnPartProvider.DictionarySupport dictionaryEncodedColumn,
      ColumnPartProvider<RunLengthColumn> runLengthColumn,
      ColumnPartProvider<GenericColumn> genericColumn,
      ColumnPartProvider<ComplexColumn> complexColumn,
      ColumnPartProvider<BitmapIndex> bitmapIndex,
      ColumnPartProvider<SpatialIndex> spatialIndex,
      ColumnPartProvider<HistogramBitmap> metricBitmap,
      ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap,
      ColumnPartProvider<LuceneIndex> luceneIndex,
      Map<String, Object> stats,
      Map<String, String> descs
  )
  {
    this.capabilities = capabilities;
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    this.runLengthColumn = runLengthColumn;
    this.genericColumn = genericColumn;
    this.complexColumn = complexColumn;
    this.bitmapIndex = bitmapIndex;
    this.spatialIndex = spatialIndex;
    this.metricBitmap = metricBitmap;
    this.bitSlicedBitmap = bitSlicedBitmap;
    this.luceneIndex = luceneIndex;
    this.stats = stats;
    this.descs = descs;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public int getLength()
  {
    if (genericColumn != null) {
      try (GenericColumn column = genericColumn.get()) {
        return column.length();
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
  public long getSerializedSize()
  {
    long serialized = 0L;
    if (dictionaryEncodedColumn != null) {
      serialized += dictionaryEncodedColumn.getSerializedSize();
    }
    if (runLengthColumn != null) {
      serialized += runLengthColumn.getSerializedSize();
    }
    if (genericColumn != null) {
      serialized += genericColumn.getSerializedSize();
    }
    if (complexColumn != null) {
      serialized += complexColumn.getSerializedSize();
    }
    if (bitmapIndex != null) {
      serialized += bitmapIndex.getSerializedSize();
    }
    if (bitSlicedBitmap != null) {
      serialized += bitSlicedBitmap.getSerializedSize();
    }
    if (spatialIndex != null) {
      serialized += spatialIndex.getSerializedSize();
    }
    if (metricBitmap != null) {
      serialized += metricBitmap.getSerializedSize();
    }
    if (luceneIndex != null) {
      serialized += luceneIndex.getSerializedSize();
    }
    return serialized;
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
        return luceneIndex == null ? -1 : luceneIndex.getSerializedSize();
    }
    return -1;
  }

  @Override
  public float getAverageSize()
  {
    if (dictionaryEncodedColumn != null) {
      final GenericIndexed<String> dictionary = dictionaryEncodedColumn.getDictionary();
      return dictionary.totalLengthOfWords() / dictionary.size();
    }
    if (runLengthColumn != null) {
      return runLengthColumn.getSerializedSize() / runLengthColumn.size();
    }
    if (genericColumn != null) {
      return genericColumn.getSerializedSize() / genericColumn.size();
    }
    if (complexColumn != null) {
      return complexColumn.getSerializedSize() / complexColumn.size();
    }
    if (bitmapIndex != null) {
      return bitmapIndex.getSerializedSize() / bitmapIndex.size();
    }
    if (spatialIndex != null) {
      return spatialIndex.getSerializedSize() / spatialIndex.size();
    }
    if (metricBitmap != null) {
      return metricBitmap.getSerializedSize() / metricBitmap.size();
    }
    if (bitSlicedBitmap != null) {
      return bitSlicedBitmap.getSerializedSize() / bitSlicedBitmap.size();
    }
    if (luceneIndex != null) {
      return luceneIndex.getSerializedSize() / luceneIndex.size();
    }
    return 0;
  }

  @Override
  public GenericIndexed<String> getDictionary()
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
  public LuceneIndex getLuceneIndex()
  {
    return luceneIndex == null ? null : luceneIndex.get();
  }

  @Override
  public Map<String, Object> getColumnStats()
  {
    return stats;
  }

  @Override
  public Map<String, String> getColumnDescs()
  {
    return descs;
  }
}
