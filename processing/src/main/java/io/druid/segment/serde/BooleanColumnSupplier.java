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
import io.druid.segment.column.BooleanGenericColumn;
import io.druid.segment.column.GenericColumn;

/**
 */
public class BooleanColumnSupplier implements ColumnPartProvider<GenericColumn>
{
  private final int serializedLength;
  private final int numRows;

  private final Supplier<ImmutableBitmap> values;
  private final Supplier<ImmutableBitmap> nulls;

  public BooleanColumnSupplier(
      int serializedLength,
      int numRows,
      Supplier<ImmutableBitmap> values,
      Supplier<ImmutableBitmap> nulls
  )
  {
    this.values = values;
    this.nulls = nulls;
    this.numRows = numRows;
    this.serializedLength = serializedLength;
  }

  @Override
  public GenericColumn get()
  {
    return new BooleanGenericColumn(values.get(), nulls.get(), numRows);
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  public long getSerializedSize()
  {
    return serializedLength;
  }
}
