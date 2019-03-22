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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueType;
import io.druid.segment.data.IndexedDoubles;

import java.io.IOException;

/**
*/
public class IndexedDoublesGenericColumn extends AbstractGenericColumn
{
  private final IndexedDoubles column;
  private final ImmutableBitmap nulls;

  public IndexedDoublesGenericColumn(IndexedDoubles column, ImmutableBitmap nulls)
  {
    this.column = column;
    this.nulls = nulls;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public ValueType getType()
  {
    return ValueType.DOUBLE;
  }

  @Override
  public Double getDouble(int rowNum)
  {
    return nulls.get(rowNum) ? null : column.get(rowNum);
  }

  @Override
  public Float getFloat(int rowNum)
  {
    return nulls.get(rowNum) ? null : (float) column.get(rowNum);
  }

  @Override
  public Long getLong(int rowNum)
  {
    return nulls.get(rowNum) ? null : (long) column.get(rowNum);
  }

  @Override
  public ImmutableBitmap getNulls()
  {
    return nulls;
  }

  @Override
  public void close() throws IOException
  {
    column.close();
  }
}
