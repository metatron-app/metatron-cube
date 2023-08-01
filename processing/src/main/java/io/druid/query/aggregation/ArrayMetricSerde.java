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
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.MetricExtractor;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 */
public class ArrayMetricSerde implements ComplexMetricSerde
{
  private final ValueDesc elementType;
  private final ObjectStrategy strategy;

  public ArrayMetricSerde(ValueType element)
  {
    Preconditions.checkArgument(element.isPrimitive(), "not for complex type");
    this.elementType = ValueDesc.of(element);
    this.strategy = null;
  }

  public ArrayMetricSerde(ComplexMetricSerde serde)
  {
    Preconditions.checkNotNull(serde, "complex serde is null");
    this.elementType = serde.getType();
    this.strategy = serde.getObjectStrategy();
  }

  @Override
  public ValueDesc getType()
  {
    return ValueDesc.ofArray(elementType);
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return extractor(elementType);
  }

  public static MetricExtractor extractor(ValueDesc elementType)
  {
    MetricExtractor extractor = MetricExtractor.of(elementType);
    return v -> {
      if (v == null) {
        return null;
      } else if (v instanceof List) {
        List list = (List) v;
        Object[] array = new Object[list.size()];
        for (int i = 0; i < array.length; i++) {
          array[i] = extractor.extract(list.get(i));
        }
        return Arrays.asList(array);
      } else if (v.getClass().isArray()) {
        Object[] array = new Object[Array.getLength(v)];
        for (int i = 0; i < array.length; i++) {
          array[i] = extractor.extract(Array.get(v, i));
        }
        return Arrays.asList(array);
      } else {
        return Arrays.asList(extractor.extract(v));
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
        switch (elementType.type()) {
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
      @SuppressWarnings("unchecked")
      public byte[] toBytes(Object val)
      {
        if (val == null) {
          return new byte[0];
        }
        List<Object> value = (List<Object>) val;
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeShort(value.size());
        switch (elementType.type()) {
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
    return "ArrayMetricSerde{" + ValueDesc.ofArray(elementType) + "}";
  }
}
