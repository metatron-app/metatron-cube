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

package io.druid.query.aggregation;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.AbstractInputRow;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class ArrayMetricSerde implements ComplexMetricSerde
{
  private final ValueType elementType;
  private final ValueDesc arrayType;

  private final ComplexMetricExtractor extractor;
  private final ObjectStrategy strategy;

  public ArrayMetricSerde(ValueType element)
  {
    Preconditions.checkArgument(element.isPrimitive(), "not for complex type");
    this.elementType = element;
    this.arrayType = ValueDesc.ofArray(element.name());
    this.extractor = null;
    this.strategy = null;
  }

  public ArrayMetricSerde(ComplexMetricSerde serde)
  {
    Preconditions.checkNotNull(serde, "complex serde is null");
    this.elementType = ValueType.COMPLEX;
    this.arrayType = ValueDesc.ofArray(serde.getTypeName());
    this.extractor = serde.getExtractor(null);
    this.strategy = serde.getObjectStrategy();
  }

  @Override
  public String getTypeName()
  {
    return arrayType.typeName();
  }

  @Override
  public ComplexMetricExtractor getExtractor(List<String> typeHint)
  {
    return new ComplexMetricExtractor()
    {
      private final AbstractInputRow.Dummy dummy = new AbstractInputRow.Dummy();

      @Override
      public Object extractValue(Row inputRow, String metricName)
      {
        Object raw = inputRow.getRaw(metricName);
        return raw == null ? null : extractElement(toList(raw));
      }

      private List toList(Object value)
      {
        if (value instanceof List) {
          if (extractor != null) {  // the content of the list would be probably modified
            return Lists.newArrayList((List) value);
          }
          return (List) value;
        }
        if (value != null && value.getClass().isArray()) {
          final int length = Array.getLength(value);
          final List list = Lists.newArrayListWithCapacity(length);
          for (int i = 0; i < length; i++) {
            list.add(Array.get(value, i));
          }
          return list;
        }
        return Arrays.asList(value);
      }

      private List extractElement(List list)
      {
        if (extractor != null) {
          for (int i = 0; i < list.size(); i++) {
            dummy.setObject(list.get(i));
            list.set(i, extractor.extractValue(dummy, "dummy"));
          }
        } else {
          for (int i = 0; i < list.size(); i++) {
            list.set(i, elementType.cast(list.get(i)));
          }
        }
        return list;
      }
    };
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy()
    {
      @Override
      public Class getClazz()
      {
        return List.class;
      }

      @Override
      public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        if (numBytes == 0) {
          return null;
        }
        int size = buffer.getShort();
        List<Object> value = Lists.newArrayListWithCapacity(size);
        switch (elementType) {
          case FLOAT:
            for (int i = 0; i < size; i++) {
              value.add(buffer.getFloat());
            }
            break;
          case DOUBLE:
            for (int i = 0; i < size; i++) {
              value.add(buffer.getDouble());
            }
            break;
          case LONG:
            for (int i = 0; i < size; i++) {
              value.add(buffer.getLong());
            }
            break;
          case STRING:
            for (int i = 0; i < size; i++) {
              value.add(readString(buffer));
            }
            break;
          case COMPLEX:
            for (int i = 0; i < size; i++) {
              int length = buffer.getInt();
              int position = buffer.position();
              value.add(strategy.fromByteBuffer(buffer, length));
              buffer.position(position + length);
            }
        }
        return value;
      }

      @Override
      public byte[] toBytes(Object val)
      {
        if (val == null) {
          return new byte[0];
        }
        List<Object> value = (List<Object>) val;
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeShort(value.size());
        switch (elementType) {
          case FLOAT:
            for (Object v : value) {
              out.writeFloat(((Number) v).floatValue());
            }
            break;
          case DOUBLE:
            for (Object v : value) {
              out.writeDouble(((Number) v).doubleValue());
            }
            break;
          case LONG:
            for (Object v : value) {
              out.writeLong(((Number) v).longValue());
            }
            break;
          case STRING:
            for (Object v : value) {
              writeString(Objects.toString(v, ""), out);
            }
            break;
          case COMPLEX:
            for (Object v : value) {
              writeBytes(strategy.toBytes(v), out);
            }
        }
        return out.toByteArray();
      }
    };
  }

  private static void writeString(String value, ByteArrayDataOutput out)
  {
    writeBytes(value.getBytes(Charsets.UTF_8), out);
  }

  private static String readString(ByteBuffer in)
  {
    byte[] result = readBytes(in);
    return new String(result, Charsets.UTF_8);
  }

  private static void writeBytes(byte[] bytes, ByteArrayDataOutput out)
  {
    out.writeInt(bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static byte[] readBytes(ByteBuffer in)
  {
    byte[] result = new byte[in.getInt()];
    in.get(result);
    return result;
  }

  @Override
  public String toString()
  {
    return "ArrayMetricSerde{" +
           "typeName='" + getTypeName() + '\'' +
           "}";
  }
}
