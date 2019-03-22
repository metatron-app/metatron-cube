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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.DoubleColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 */
public class DoubleGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static DoubleGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new DoubleGenericColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  private Serializer serializer;

  public DoubleGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
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
        final CompressedDoublesIndexedSupplier column = CompressedDoublesIndexedSupplier.fromByteBuffer(buffer, byteOrder);
        final Supplier<ImmutableBitmap> nulls;
        if (buffer.remaining() > Ints.BYTES) {
          final int size = buffer.getInt();
          final ByteBuffer serialized = ByteBufferSerializer.prepareForRead(buffer, size);
          nulls = new Supplier<ImmutableBitmap>()
          {
            @Override
            public ImmutableBitmap get()
            {
              return serdeFactory.getObjectStrategy().fromByteBuffer(serialized, size);
            }
          };
        } else {
          nulls = Suppliers.<ImmutableBitmap>ofInstance(serdeFactory.getBitmapFactory().makeEmptyImmutableBitmap());
        }
        builder.setType(ValueDesc.DOUBLE)
               .setHasMultipleValues(false)
               .setGenericColumn(new DoubleGenericColumnSupplier(column, nulls));
      }
    };
  }
}
