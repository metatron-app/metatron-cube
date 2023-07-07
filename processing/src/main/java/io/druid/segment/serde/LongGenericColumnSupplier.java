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

package io.druid.segment.serde;

import com.google.common.base.Supplier;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IndexedLongsGenericColumn;
import io.druid.segment.data.CompressedLongReader;

/**
 */
public class LongGenericColumnSupplier implements ColumnPartProvider<GenericColumn>
{
  private final CompressedLongReader column;
  private final Supplier<ImmutableBitmap> nulls;
  private final boolean timestamp;

  public LongGenericColumnSupplier(
      CompressedLongReader column,
      Supplier<ImmutableBitmap> nulls,
      boolean timestamp
  )
  {
    this.column = column;
    this.nulls = nulls;
    this.timestamp = timestamp;
  }

  @Override
  public int numRows()
  {
    return column.numRows();
  }

  @Override
  public long getSerializedSize()
  {
    return column.getSerializedSize();
  }

  @Override
  public Class<? extends GenericColumn> provides()
  {
    return IndexedLongsGenericColumn.class;
  }

  @Override
  public GenericColumn get()
  {
    if (timestamp) {
      return IndexedLongsGenericColumn.timestamp(column.get(), column.compressionType(), nulls.get());
    }
    return new IndexedLongsGenericColumn(column.get(), column.compressionType(), nulls.get());
  }
}
