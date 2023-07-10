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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
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
import java.util.List;
import java.util.Map;

/**
 */
public class StructColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static StructColumnPartSerde createDeserializer(
      @JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("fieldDescriptors") List<ColumnDescriptor> fieldDescriptors
  )
  {
    return new StructColumnPartSerde(fieldNames, fieldDescriptors);
  }

  private final List<String> fieldNames;
  private final List<ColumnDescriptor> fieldDescriptors;

  public StructColumnPartSerde(List<String> fieldNames, List<ColumnDescriptor> fieldDescriptors)
  {
    Preconditions.checkArgument(fieldNames.size() == fieldDescriptors.size());
    this.fieldNames = fieldNames;
    this.fieldDescriptors = fieldDescriptors;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public List<ColumnDescriptor> getFieldDescriptors()
  {
    return fieldDescriptors;
  }

  @Override
  public Serializer getSerializer()
  {
    return new Serializer()
    {
      @Override
      public long getSerializedSize()
      {
        return getPrefixedSize(fieldDescriptors);
      }

      @Override
      public long writeToChannel(WritableByteChannel channel) throws IOException
      {
        long written = 0;
        for (ColumnDescriptor descriptor : fieldDescriptors) {
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
        Map<String, Column> fields = Maps.newLinkedHashMap();
        for (int i = 0; i < fieldNames.size(); i++) {
          String fieldName = fieldNames.get(i);
          ByteBuffer prepared = ByteBufferSerializer.prepareForRead(buffer);
          fields.put(fieldName, fieldDescriptors.get(i).read(fieldName, prepared, serdeFactory));
        }
        ValueDesc[] fieldTypes = fields.values().stream().map(c -> c.getType()).toArray(x -> new ValueDesc[x]);
        ValueDesc type = ValueDesc.ofStruct(fieldNames.toArray(new String[0]), fieldTypes);
        builder.setComplexColumn(new ColumnPartProvider<ComplexColumn>()
        {
          @Override
          public int numRows()
          {
            return Iterables.getFirst(fields.values(), null).getNumRows();
          }

          @Override
          public long getSerializedSize()
          {
            return getPrefixedSize(fieldDescriptors);
          }

          @Override
          public Class<? extends ComplexColumn> provides()
          {
            return StructColumn.class;
          }

          @Override
          public ComplexColumn get()
          {
            return new StructColumn(type, fieldNames, fields);
          }
        });
      }
    };
  }

  private static long getPrefixedSize(List<ColumnDescriptor> fieldDescriptors)
  {
    long sum = 0;
    for (ColumnDescriptor descriptor : fieldDescriptors) {
      sum += Integer.BYTES;
      sum += descriptor.numBytes();
    }
    return sum;
  }

  private static class StructColumn implements ComplexColumn.StructColumn
  {
    private final ValueDesc type;
    private final List<String> fieldNames;
    private final Map<String, Column> fields;

    public StructColumn(ValueDesc type, List<String> fieldNames, Map<String, Column> fields) {
      this.type = type;
      this.fieldNames = fieldNames;
      this.fields = fields;
    }

    @Override
    public ValueDesc getType()
    {
      return type;
    }

    @Override
    public CompressionStrategy compressionType()
    {
      return null;
    }

    @Override
    public int size()
    {
      return Iterables.getFirst(fields.values(), null).getNumRows();
    }

    @Override
    public List<String> getFieldNames()
    {
      return fieldNames;
    }

    @Override
    public ValueDesc getType(String field)
    {
      Column column = fields.get(field);
      return column == null ? null : column.getType();
    }

    @Override
    public CompressionStrategy compressionType(String field)
    {
      Column column = fields.get(field);
      return column == null ? null : column.compressionType();
    }

    @Override
    public Column getField(String field)
    {
      return fields.get(field);
    }

    @Override
    public Map<String, Object> getStats(String field)
    {
      Column column = fields.get(field);
      return column == null ? null : column.getColumnStats();
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
