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

import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.NotCompressedComplexColumn;
import io.druid.segment.data.GenericIndexed;

/**
 */
public class NotCompressedComplexColumnPartSupplier implements ColumnPartProvider<ComplexColumn>
{
  private final ValueDesc type;
  private final GenericIndexed column;

  public NotCompressedComplexColumnPartSupplier(ValueDesc type, GenericIndexed column)
  {
    this.type = type;
    this.column = column;
  }

  @Override
  public int numRows()
  {
    return column.size();
  }

  @Override
  public long getSerializedSize()
  {
    return column.getSerializedSize();
  }

  @Override
  public Class<? extends ComplexColumn> provides()
  {
    return NotCompressedComplexColumn.class;
  }

  @Override
  public ComplexColumn get()
  {
    return new NotCompressedComplexColumn(type, column.dedicated());
  }
}
