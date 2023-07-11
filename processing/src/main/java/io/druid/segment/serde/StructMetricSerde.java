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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.segment.data.ObjectStrategy;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class StructMetricSerde implements ComplexMetricSerde, Iterable<Pair<String, ValueDesc>>
{
  private final String elementType;

  private final String[] fieldNames;
  private final ValueDesc[] fieldTypes;

  public StructMetricSerde(String[] elements)
  {
    Preconditions.checkArgument(elements[0].equals(ValueDesc.STRUCT_TYPE));
    Preconditions.checkArgument(elements.length <= 255, "cannot exceed 255 elements");
    fieldNames = new String[elements.length - 1];
    fieldTypes = new ValueDesc[elements.length - 1];

    for (int i = 0; i < fieldNames.length; i++) {
      String element = elements[i + 1].trim();
      int index = element.indexOf(":");
      Preconditions.checkArgument(index > 0, "'fieldName:fieldType' for field declaration");
      fieldNames[i] = element.substring(0, index).trim();
      fieldTypes[i] = ValueDesc.of(element.substring(index + 1).trim());
    }
    this.elementType = StringUtils.join(Arrays.copyOfRange(elements, 1, elements.length), ',');
  }

  @Override
  public ValueDesc getType()
  {
    return ValueDesc.ofStruct(fieldNames, fieldTypes);
  }

  public ValueDesc getTypeOf(String fieldName)
  {
    return type(indexOf(fieldName));
  }

  public int indexOf(String fieldName)
  {
    return Arrays.asList(fieldNames).indexOf(fieldName);
  }

  public ValueDesc type(int index)
  {
    return index < 0 ? null : fieldTypes[index];
  }

  public String[] getFieldNames()
  {
    return fieldNames;
  }

  public ValueDesc[] getFieldTypes()
  {
    return fieldTypes;
  }

  @Override
  public Iterator<Pair<String, ValueDesc>> iterator()
  {
    return new Iterator<Pair<String, ValueDesc>>()
    {
      private int index;

      @Override
      public boolean hasNext()
      {
        return index < fieldNames.length;
      }

      @Override
      public Pair<String, ValueDesc> next()
      {
        return Pair.of(fieldNames[index], fieldTypes[index++]);
      }
    };
  }

  @Override
  public ComplexMetricExtractor getExtractor(List<String> typeHint)
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Object extractValue(Row inputRow, String metricName)
      {
        return extractElement(inputRow.getRaw(metricName));
      }

      private Object extractElement(Object value)
      {
        if (value == null || value instanceof Map) {
          return value;
        }
        final Object[] struct = new Object[fieldNames.length];
        if (value instanceof List) {
          List list = (List) value;
          int length = Math.min(list.size(), fieldTypes.length);
          for (int i = 0; i < length; i++) {
            switch (fieldTypes[i].type()) {
              case BOOLEAN:
                struct[i] = Rows.parseBoolean(list.get(i));
                break;
              case LONG:
                struct[i] = Rows.parseLong(list.get(i));
                break;
              case DOUBLE:
                struct[i] = Rows.parseDouble(list.get(i));
                break;
              case STRING:
                struct[i] = Objects.toString(list.get(i), null);
                break;
              default:
                throw new UnsupportedOperationException("only primitives are allowed in struct");
            }
          }
        } else if (value.getClass().isArray()) {
          int length = Math.min(Array.getLength(value), fieldTypes.length);
          for (int i = 0; i < length; i++) {
            switch (fieldTypes[i].type()) {
              case BOOLEAN:
                struct[i] = Rows.parseBoolean(Array.get(value, i));
                break;
              case FLOAT:
                struct[i] = Rows.parseFloat(Array.get(value, i));
                break;
              case LONG:
                struct[i] = Rows.parseLong(Array.get(value, i));
                break;
              case DOUBLE:
                struct[i] = Rows.parseDouble(Array.get(value, i));
                break;
              case STRING:
                struct[i] = Objects.toString(Array.get(value, i), null);
                break;
              default:
                throw new UnsupportedOperationException("only primitives are allowed in struct");
            }
          }
        } else {
          throw new IAE("Cannot extract struct type from %s", value.getClass());
        }
        return struct;
      }
    };
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<Object[]>()
    {
      @Override
      public Class<Object[]> getClazz()
      {
        return Object[].class;
      }

      @Override
      public Object[] fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        Object[] struct = new Object[fieldNames.length];
        for (int i = 0; i < struct.length; i++) {
          switch (fieldTypes[i].type()) {
            case FLOAT:
              struct[i] = buffer.getFloat();
              break;
            case DOUBLE:
              struct[i] = buffer.getDouble();
              break;
            case LONG:
              struct[i] = buffer.getLong();
              break;
            case STRING:
              struct[i] = readString(buffer);
              break;
            default:
              throw new UnsupportedOperationException("only primitives are allowed in struct");
          }
        }
        return struct;
      }

      @Override
      public byte[] toBytes(Object[] struct)
      {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        for (int i = 0; i < struct.length; i++) {
          switch (fieldTypes[i].type()) {
            case FLOAT:
              out.writeFloat((Float) struct[i]);
              break;
            case DOUBLE:
              out.writeDouble((Double) struct[i]);
              break;
            case LONG:
              out.writeLong((Long) struct[i]);
              break;
            case STRING:
              writeString(Objects.toString(struct[i], ""), out);
              break;
            default:
              throw new UnsupportedOperationException("only primitives are allowed in struct");
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
    return "StructMetricSerde{elementType='" + elementType + '\'' + "}";
  }

  public static class Factory implements ComplexMetricSerde.Factory
  {
    @Override
    public ComplexMetricSerde create(String[] elements)
    {
      return new StructMetricSerde(elements);
    }
  }
}
