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

import io.druid.collections.ResourceHolder;
import io.druid.data.ValueDesc;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

/**
 */
public interface ComplexColumn extends ColumnAccess
{
  ValueDesc getType();

  CompressionStrategy compressionType();

  int getNumRows();

  class Compressed extends ColumnAccess.Compressed implements ComplexColumn
  {
    private final ValueDesc type;
    private final CompressionStrategy compression;

    public Compressed(
        ComplexMetricSerde serde,
        int[] mapping,
        ShortBuffer offsets,
        GenericIndexed<ResourceHolder<ByteBuffer>> indexed,
        CompressionStrategy compression
    )
    {
      super(serde.getObjectStrategy(), mapping, offsets, indexed);
      this.type = ValueDesc.of(serde.getTypeName());
      this.compression = compression;
    }

    @Override
    public ValueDesc getType()
    {
      return type;
    }

    @Override
    public CompressionStrategy compressionType()
    {
      return compression;
    }

    @Override
    public int getNumRows()
    {
      return numRows();
    }
  }
}
