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

import io.druid.data.input.Row;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.GenericIndexed;

import java.util.Map;

/**
 */
public interface Column
{
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
    LUCENE_INDEX
  }

  public static final String TIME_COLUMN_NAME = Row.TIME_COLUMN_NAME;
  public ColumnCapabilities getCapabilities();

  public int getNumRows();
  public long getSerializedSize();
  public long getSerializedSize(EncodeType encodeType);
  public float getAverageSize();
  public GenericIndexed<String> getDictionary();
  public DictionaryEncodedColumn getDictionaryEncoding();
  public RunLengthColumn getRunLengthColumn();
  public GenericColumn getGenericColumn();
  public ComplexColumn getComplexColumn();
  public BitmapIndex getBitmapIndex();
  public SpatialIndex getSpatialIndex();
  public HistogramBitmap getMetricBitmap();
  public BitSlicedBitmap getBitSlicedBitmap();
  public LuceneIndex getLuceneIndex();
  public Map<String, Object> getColumnStats();
  public Map<String, String> getColumnDescs();
}
