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

package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.IOPeon;
import io.druid.segment.serde.BooleanColumnPartSerde;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class BooleanColumnSerializer implements GenericColumnSerializer
{
  public static BooleanColumnSerializer create(BitmapSerdeFactory serdeFactory)
  {
    return new BooleanColumnSerializer(serdeFactory);
  }

  final BooleanWriter writer;

  private BooleanColumnSerializer(final BitmapSerdeFactory serdeFactory)
  {
    writer = new BooleanWriter(serdeFactory.getBitmapFactory());
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    writer.open();
  }

  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    writer.add(Rows.parseBoolean(obj));
  }

  @Override
  public Builder buildDescriptor(IOPeon ioPeon, Builder builder)
  {
    builder.setValueType(ValueDesc.BOOLEAN);
    builder.addSerde(new BooleanColumnPartSerde(IndexIO.BYTE_ORDER, this));
    return builder;
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    return writer.writeToChannel(channel);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (writer.index == 0) {
      return null;
    }
    return ImmutableMap.<String, Object>of(
        ColumnStats.NUM_NULLS, writer.nulls.size()
    );
  }

  private static class BooleanWriter implements ColumnPartWriter<Boolean>
  {
    private final BitmapFactory factory;

    int index = 0;
    MutableBitmap values;
    MutableBitmap nulls;

    public BooleanWriter(BitmapFactory factory)
    {
      this.factory = factory;
    }

    @Override
    public void open() throws IOException
    {
      index = 0;
      values = factory.makeEmptyMutableBitmap();
      nulls = factory.makeEmptyMutableBitmap();
    }

    @Override
    public void add(Boolean value) throws IOException
    {
      if (value == null) {
        nulls.add(index);
      } else if (value) {
        values.add(index);
      }
      index++;
    }

    @Override
    public long getSerializedSize()
    {
      long serialized = 0;
      serialized += Integer.BYTES;      // length of block
      serialized += Integer.BYTES;      // size
      serialized += Integer.BYTES;      // values.length
      serialized += values.isEmpty() ? 0 : values.toBytes().length;
      serialized += Integer.BYTES;      // nulls.length
      serialized += nulls.isEmpty() ? 0 : nulls.toBytes().length;
      return serialized;
    }

    @Override
    public long writeToChannel(WritableByteChannel channel) throws IOException
    {
      final byte[] valuesBytes = values.isEmpty() ? new byte[0] : values.toBytes();
      final byte[] nullsBytes = nulls.isEmpty() ? new byte[0] : nulls.toBytes();

      int serialized = Integer.BYTES;     // except length of block
      serialized += Integer.BYTES + valuesBytes.length;
      serialized += Integer.BYTES + nullsBytes.length;

      final DataOutputStream output = new DataOutputStream(Channels.newOutputStream(channel));
      output.writeInt(serialized);
      output.writeInt(index);   // size
      output.writeInt(valuesBytes.length);
      output.write(valuesBytes);
      output.writeInt(nullsBytes.length);
      output.write(nullsBytes);
      output.flush();

      return Integer.BYTES + serialized;
    }
  }
}
