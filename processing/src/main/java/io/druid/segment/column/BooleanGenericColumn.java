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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueType;

/**
 */
public class BooleanGenericColumn extends AbstractGenericColumn
{
  private final int numRows;
  private final ImmutableBitmap values;
  private final ImmutableBitmap nulls;

  public BooleanGenericColumn(ImmutableBitmap values, ImmutableBitmap nulls, int numRows)
  {
    this.values = values;
    this.nulls = nulls;
    this.numRows = numRows;
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public ValueType getType()
  {
    return ValueType.BOOLEAN;
  }

  @Override
  public Double getDouble(int rowNum)
  {
    final Boolean value = getValue(rowNum);
    return value == null ? null : value ? 1D : 0D;
  }

  @Override
  public Float getFloat(int rowNum)
  {
    final Boolean value = getValue(rowNum);
    return value == null ? null : value ? 1F : 0F;
  }

  @Override
  public Long getLong(int rowNum)
  {
    final Boolean value = getValue(rowNum);
    return value == null ? null : value ? 1L : 0L;
  }

  @Override
  public Boolean getBoolean(int rowNum)
  {
    return getValue(rowNum);
  }

  @Override
  public Boolean getValue(final int rowNum)
  {
    return nulls.get(rowNum) ? null : values.get(rowNum);
  }

  @Override
  public ImmutableBitmap getNulls()
  {
    return nulls;
  }
}
