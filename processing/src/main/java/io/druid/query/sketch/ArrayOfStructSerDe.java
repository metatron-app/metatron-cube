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

package io.druid.query.sketch;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.StringUtils;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import io.druid.data.ValueType;

import java.util.List;
import java.util.Objects;

/**
 */
public class ArrayOfStructSerDe extends ArrayOfItemsSerDe<Object[]>
{
  private final ValueType[] fields;

  public ArrayOfStructSerDe(List<ValueType> fields)
  {
    this.fields = fields.toArray(new ValueType[fields.size()]);
  }

  @Override
  public byte[] serializeToByteArray(final Object[][] items)
  {
    ByteArrayDataOutput output = ByteStreams.newDataOutput();
    for (Object[] item : items) {
      for (int j = 0; j < fields.length; j++) {
        switch (fields[j]) {
          case FLOAT:
            output.writeFloat(((Number) item[j]).floatValue());
            break;
          case LONG:
            output.writeLong(((Number) item[j]).longValue());
            break;
          case DOUBLE:
            output.writeDouble(((Number) item[j]).doubleValue());
            break;
          case STRING:
            byte[] value = StringUtils.toUtf8(Objects.toString(item[j], ""));
            Preconditions.checkArgument(value.length < 0xFF);
            output.writeByte((byte) value.length);
            output.write(value);
            break;
          default:
            throw new UnsupportedOperationException("not supported type " + fields[j]);
        }
      }
    }
    return output.toByteArray();
  }

  @Override
  public Object[][] deserializeFromMemory(final Memory mem, final int numItems)
  {
    final Object[][] items = new Object[numItems][fields.length];
    long offsetBytes = 0;
    for (Object[] item : items) {
      for (int j = 0; j < fields.length; j++) {
        switch (fields[j]) {
          case FLOAT:
            item[j] = mem.getFloat(offsetBytes);
            offsetBytes += Float.BYTES;
            break;
          case LONG:
            item[j] = mem.getLong(offsetBytes);
            offsetBytes += Long.BYTES;
            break;
          case DOUBLE:
            item[j] = mem.getDouble(offsetBytes);
            offsetBytes += Double.BYTES;
            break;
          case STRING:
            byte[] bytes = new byte[UnsignedBytes.toInt(mem.getByte(offsetBytes++))];
            mem.getByteArray(offsetBytes, bytes, 0, bytes.length);
            item[j] = StringUtils.fromUtf8(bytes);
            offsetBytes += bytes.length;
            break;
          default:
            throw new UnsupportedOperationException("not supported type " + fields[j]);
        }
      }
    }
    return items;
  }
}
