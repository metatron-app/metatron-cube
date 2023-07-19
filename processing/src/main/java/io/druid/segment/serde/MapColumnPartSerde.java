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
import io.druid.common.utils.IOUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 *
 */
public class MapColumnPartSerde implements ColumnPartSerde
{
  private final ColumnDescriptor keyDescriptor;
  private final ColumnDescriptor valueDescriptor;

  @JsonCreator
  public MapColumnPartSerde(
      @JsonProperty("keyDescriptor") ColumnDescriptor keyDescriptor,
      @JsonProperty("valueDescriptor") ColumnDescriptor valueDescriptor
  )
  {
    this.keyDescriptor = keyDescriptor;
    this.valueDescriptor = valueDescriptor;
  }

  @JsonProperty
  public ColumnDescriptor getKeyDescriptor()
  {
    return keyDescriptor;
  }

  @JsonProperty
  public ColumnDescriptor getValueDescriptor()
  {
    return valueDescriptor;
  }

  @Override
  public Serializer getSerializer()
  {
    return new Serializer()
    {
      @Override
      public long getSerializedSize()
      {
        return Integer.BYTES + keyDescriptor.numBytes() + Integer.BYTES + valueDescriptor.numBytes();
      }

      @Override
      public long writeToChannel(WritableByteChannel channel) throws IOException
      {
        byte[] scratch = new byte[Integer.BYTES];
        long written = 0;
        written += channel.write(ByteBuffer.wrap(IOUtils.intTo(keyDescriptor.numBytes(), scratch)));
        written += keyDescriptor.write(channel);
        written += channel.write(ByteBuffer.wrap(IOUtils.intTo(valueDescriptor.numBytes(), scratch)));
        written += valueDescriptor.write(channel);
        return written;
      }
    };
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
      {
        String prefix = builder.getName() + ".";
        ByteBuffer prepared = ByteBufferSerializer.prepareForRead(buffer);
        Column keys = keyDescriptor.read(prefix + "key", prepared, serdeFactory);

        prepared = ByteBufferSerializer.prepareForRead(buffer);
        Column values = valueDescriptor.read(prefix + "value", prepared, serdeFactory);

        ValueDesc type = ValueDesc.ofMap(keys.getType().unwrapDimension(), values.getType());
        builder.setComplexColumn(new ColumnPartProvider<ComplexColumn>()
        {
          @Override
          public int numRows()
          {
            return keys.getNumRows();
          }

          @Override
          public long getSerializedSize()
          {
            return Integer.BYTES + keyDescriptor.numBytes() + Integer.BYTES + valueDescriptor.numBytes();
          }

          @Override
          public Class<? extends ComplexColumn> provides()
          {
            return MapColumn.class;
          }

          @Override
          public ComplexColumn get()
          {
            return new MapColumn(type, keys, values);
          }
        });
      }
    };
  }

  private static class MapColumn implements ComplexColumn.MapColumn
  {
    private final ValueDesc type;
    private final Column key;
    private final Column value;

    public MapColumn(ValueDesc type, Column key, Column value)
    {
      this.type = type;
      this.key = key;
      this.value = value;
    }

    @Override
    public ValueDesc getType()
    {
      return type;
    }

    @Override
    public int numRows()
    {
      return key.getNumRows();
    }

    @Override
    public Column getKey()
    {
      return key;
    }

    @Override
    public Column getValue()
    {
      return value;
    }

    @Override
    public CompressionStrategy compressionType()
    {
      return null;
    }

    @Override
    public CompressionStrategy keyCompressionType()
    {
      return key.compressionType();
    }

    @Override
    public CompressionStrategy valueCompressionType()
    {
      return value.compressionType();
    }

    @Override
    public Object getValue(int rowNum)
    {
      throw new UnsupportedOperationException();    // todo
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
