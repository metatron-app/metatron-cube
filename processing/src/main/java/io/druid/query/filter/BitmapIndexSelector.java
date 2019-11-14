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
import io.druid.query.select.Schema;
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
public interface BitmapIndexSelector extends Closeable
{
  public Schema getSchema(boolean prependTime);
  public Indexed<String> getDimensionValues(String dimension);
  public int getNumRows();
  public BitmapFactory getBitmapFactory();
  public BitmapIndex getBitmapIndex(String dimension);
  public ImmutableBitmap getBitmapIndex(String dimension, String value);
  public ImmutableBitmap getBitmapIndex(String dimension, Boolean value);
  public ImmutableRTree getSpatialIndex(String dimension);
  public LuceneIndex getLuceneIndex(String dimension);
  public HistogramBitmap getMetricBitmap(String dimension);
  public BitSlicedBitmap getBitSlicedBitmap(String dimension);
  public ColumnCapabilities getCapabilities(String dimension);
  public Column getColumn(String dimension);
  public void close();

  class Abstract implements BitmapIndexSelector {

    @Override
    public Schema getSchema(boolean prependTime)
    {
      return null;
    }

    @Override
    public Indexed<String> getDimensionValues(String dimension)
    {
      return null;
    }

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

    @Override
    public void close()
    {
    }
  }
}
