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

package io.druid.query.filter;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.Indexed;

import java.io.Closeable;

/**
 */
public interface BitmapIndexSelector extends TypeResolver, Closeable
{
  int getNumRows();
  BitmapFactory getBitmapFactory();
  Indexed<String> getDimensionValues(String dimension);
  BitmapIndex getBitmapIndex(String dimension);
  ImmutableBitmap getBitmapIndex(String dimension, String value);
  ImmutableBitmap getBitmapIndex(String dimension, Boolean value);
  ImmutableRTree getSpatialIndex(String dimension);
  LuceneIndex getLuceneIndex(String dimension);
  HistogramBitmap getMetricBitmap(String dimension);
  BitSlicedBitmap getBitSlicedBitmap(String dimension);
  ColumnCapabilities getCapabilities(String dimension);
  Column getColumn(String dimension);

  default void close() {}

  default ValueDesc resolve(String column)
  {
    final ColumnCapabilities capabilities = getCapabilities(column);
    return capabilities == null ? null : ValueDesc.of(capabilities.getType());
  }

  class Abstract implements BitmapIndexSelector
  {
    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return null;
    }

    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      return null;
    }

    @Override
    public BitmapIndex getBitmapIndex(String dimension)
    {
      return null;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, String value)
    {
      return null;
    }

    @Override
    public ImmutableBitmap getBitmapIndex(String dimension, Boolean value)
    {
      return null;
    }

    @Override
    public ImmutableRTree getSpatialIndex(String dimension)
    {
      return null;
    }

    @Override
    public LuceneIndex getLuceneIndex(String dimension)
    {
      return null;
    }

    @Override
    public HistogramBitmap getMetricBitmap(String dimension)
    {
      return null;
    }

    @Override
    public BitSlicedBitmap getBitSlicedBitmap(String dimension)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getCapabilities(String dimension)
    {
      return null;
    }

    @Override
    public Column getColumn(String dimension)
    {
      return null;
    }
  }
}
