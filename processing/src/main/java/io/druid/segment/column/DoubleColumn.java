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

import com.google.common.primitives.Doubles;
import io.druid.data.ValueType;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;

/**
 */
@Deprecated
public class DoubleColumn extends AbstractColumn
{
  private static final ColumnCapabilitiesImpl CAPABILITIES = new ColumnCapabilitiesImpl()
      .setType(ValueType.DOUBLE);

  private final CompressedDoublesIndexedSupplier column;

  public DoubleColumn(CompressedDoublesIndexedSupplier column)
  {
    this.column = column;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return CAPABILITIES;
  }

  @Override
  public int getLength()
  {
    return column.size();
  }

  @Override
  public long getSerializedSize()
  {
    return column.getSerializedSize();
  }

  @Override
  public float getAverageSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public GenericColumn getGenericColumn()
  {
    return new IndexedDoublesGenericColumn(column.get());
  }
}
