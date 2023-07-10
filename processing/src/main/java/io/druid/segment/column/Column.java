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

import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.Dictionary;

import java.util.Map;
import java.util.Set;

/**
 */
public interface Column
{
  String TIME_COLUMN_NAME = Row.TIME_COLUMN_NAME;

  enum EncodeType
  {
    DICTIONARY_ENCODED,
    RUNLENGTH_ENCODED,
    GENERIC,
    COMPLEX,
    BITMAP,
    SPATIAL,
    METRIC_BITMAP,
    BITSLICED_BITMAP,
    FST
  }

  String getName();
  ColumnMeta getMetaData();

  ColumnCapabilities getCapabilities();
  int getNumRows();

  boolean hasDictionaryEncodedColumn();
  boolean hasGenericColumn();
  boolean hasComplexColumn();

  CompressionStrategy compressionType();

  Dictionary<String> getDictionary();
  DictionaryEncodedColumn getDictionaryEncoded();
  RunLengthColumn getRunLengthColumn();
  GenericColumn getGenericColumn();
  ComplexColumn getComplexColumn();
  BitmapIndex getBitmapIndex();
  SpatialIndex getSpatialIndex();
  HistogramBitmap getMetricBitmap();
  BitSlicedBitmap getBitSlicedBitmap();

  Class<? extends GenericColumn> getGenericColumnType();

  Set<Class> getExternalIndexKeys();
  <T> ExternalIndexProvider<T> getExternalIndex(Class<T> clazz);

  ExternalIndexProvider<FSTHolder> getFST();

  long getSerializedSize(EncodeType encodeType);

  Map<String, Object> getColumnStats();
  Map<String, String> getColumnDescs();

  default ValueDesc getType()
  {
    ColumnCapabilities capabilities = getCapabilities();
    ValueType valueType = capabilities.getType();
    if (capabilities.isDictionaryEncoded()) {
      return ValueDesc.ofDimension(valueType);
    } else if (!valueType.isPrimitive()) {
      return getComplexColumn().getType();
    }
    return ValueDesc.of(valueType);
  }
}
