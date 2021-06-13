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

import io.druid.common.guava.BufferRef;
import io.druid.data.ValueDesc;
import io.druid.segment.Tools;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;

import java.io.IOException;

/**
*/
public class IndexedComplexColumn implements ComplexColumn, ColumnAccess.WithRawAccess
{
  private final GenericIndexed<?> column;
  private final ValueDesc type;

  public IndexedComplexColumn(ValueDesc type, GenericIndexed<?> column)
  {
    this.column = column;
    this.type = type;
  }

  @Override
  public ValueDesc getType()
  {
    return type;
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return CompressionStrategy.UNCOMPRESSED;
  }

  @Override
  public int getNumRows()
  {
    return column.size();
  }

  @Override
  public Object getValue(int rowNum)
  {
    return column.get(rowNum);
  }

  public byte[] getAsRaw(int index)
  {
    return column.getAsRaw(index);
  }

  public BufferRef getAsRef(int index)
  {
    return column.getAsRef(index);
  }

  public <R> R apply(int index, Tools.Function<R> function)
  {
    return column.apply(index, function);
  }

  @Override
  public void close() throws IOException
  {
  }
}
