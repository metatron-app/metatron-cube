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
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.data.BitSlicedBitmap;

import java.io.Closeable;

/**
 */
public interface BitmapIndexSelector extends TypeResolver, Closeable
{
  int getNumRows();
  default BitmapFactory getBitmapFactory() {return null;}
  default BitmapIndex getBitmapIndex(String dimension) {return null;}
  default ImmutableBitmap getBitmapIndex(String dimension, String value) {return null;}
  default ImmutableBitmap getBitmapIndex(String dimension, Boolean value) {return null;}
  default ImmutableRTree getSpatialIndex(String dimension) {return null;}
  default HistogramBitmap getMetricBitmap(String dimension) {return null;}
  default BitSlicedBitmap getBitSlicedBitmap(String dimension) {return null;}

  default <T extends SecondaryIndex> T getExternalIndex(String dimension) {return null;}

  default ColumnCapabilities getCapabilities(String dimension) {return null;}
  default Column getColumn(String dimension) {return null;}

  default ImmutableBitmap createBoolean(boolean value)
  {
    if (value) {
      return DimFilters.makeTrue(getBitmapFactory(), getNumRows());
    } else {
      return DimFilters.makeFalse(getBitmapFactory());
    }
  }

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
  }
}
