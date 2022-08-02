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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class BooleanColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static BooleanColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new BooleanColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  private final Serializer serializer;

  public BooleanColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(
          final ByteBuffer buffer,
          final ColumnBuilder builder,
          final BitmapSerdeFactory serdeFactory
      )
      {
        final ByteBuffer serialized = ByteBufferSerializer.prepareForRead(buffer);
        final int size = serialized.remaining();
        final int numRows = serialized.getInt();
        final Supplier<ImmutableBitmap> values = ComplexMetrics.readBitmap(serialized, serdeFactory);
        final Supplier<ImmutableBitmap> nulls = ComplexMetrics.readBitmap(serialized, serdeFactory);
        builder.setType(ValueDesc.BOOLEAN)
               .setHasMultipleValues(false)
               .setGenericColumn(new BooleanColumnSupplier(size, numRows, values, nulls));
      }
    };
  }
}
