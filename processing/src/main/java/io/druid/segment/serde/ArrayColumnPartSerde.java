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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.data.ValueDesc;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class ArrayColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static ArrayColumnPartSerde createDeserializer(
      @JsonProperty("descriptor") ColumnDescriptor descriptor
  )
  {
    return new ArrayColumnPartSerde(Arrays.asList(descriptor));
  }

  private final List<ColumnDescriptor> descriptors;

  public ArrayColumnPartSerde(List<ColumnDescriptor> descriptors)
  {
    this.descriptors = descriptors;
  }

  @JsonProperty
  public ColumnDescriptor getDescriptor()
  {
    return descriptors.get(0);
  }

  @Override
  public Serializer getSerializer()
  {
    return new Serializer()
    {
      @Override
      public long getSerializedSize()
      {
        return Integer.BYTES + ColumnDescriptor.getPrefixedSize(descriptors);
      }

      @Override
      public long writeToChannel(WritableByteChannel channel) throws IOException
      {
        long written = channel.write(ByteBuffer.wrap(Ints.toByteArray(descriptors.size())));
        for (ColumnDescriptor descriptor : descriptors) {
          written += channel.write(ByteBuffer.wrap(Ints.toByteArray(Ints.checkedCast(descriptor.numBytes()))));
          written += descriptor.write(channel);
        }
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
        int position = buffer.position();
        ColumnDescriptor descriptor = descriptors.get(0);
        List<Column> elements = Lists.newArrayList();
        int length = buffer.getInt();
        for (int i = 0; i < length; i++) {
          ByteBuffer prepared = ByteBufferSerializer.prepareForRead(buffer);
          elements.add(descriptor.read(prefix + i, prepared, serdeFactory));
        }
        long serializedSize = buffer.position() - position;
        ValueDesc type = ValueDesc.ofArray(descriptor.getValueType());
        builder.setComplexColumn(new ColumnPartProvider<ComplexColumn>()
        {
          @Override
          public int numRows()
          {
            return Iterables.getFirst(elements, null).getNumRows();
          }

          @Override
          public long getSerializedSize()
          {
            return serializedSize;
          }

          @Override
          public Class<? extends ComplexColumn> provides()
          {
            return ArrayColumn.class;
          }

          @Override
          public ComplexColumn get()
          {
            return new ArrayColumn(type, elements);
          }
        });
      }
    };
  }

  private static class ArrayColumn implements ComplexColumn.ArrayColumn
  {
    private final ValueDesc type;
    private final List<Column> elements;

    public ArrayColumn(ValueDesc type, List<Column> elements)
    {
      this.type = type;
      this.elements = elements;
    }

    @Override
    public ValueDesc getType()
    {
      return type;
    }

    @Override
    public Column resolve(String expression)
    {
      int ix = expression.indexOf('.');
      Integer access = Ints.tryParse(ix < 0 ? expression : expression.substring(0, ix));
      if (access == null) {
        return null;
      }
      Column column = getElement(access);
      if (ix < 0 || column == null) {
        return column;
      }
      return column.resolve(expression.substring(ix + 1));
    }

    @Override
    public int numRows()
    {
      return Iterables.getFirst(elements, null).getNumRows();
    }

    @Override
    public int numElements()
    {
      return elements.size();
    }

    @Override
    public ValueDesc getType(int ix)
    {
      Column column = getElement(ix);
      return column == null ? null : column.getType();
    }

    @Override
    public Column getElement(int ix)
    {
      return ix < 0 || ix >= elements.size() ? null : elements.get(ix);
    }

    @Override
    public Map<String, Object> getStats(int ix)
    {
      Column column = getElement(ix);
      return column == null ? null : column.getColumnStats();
    }

    @Override
    public Object getValue(int rowNum)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
