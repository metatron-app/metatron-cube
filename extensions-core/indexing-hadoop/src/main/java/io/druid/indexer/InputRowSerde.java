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

package io.druid.indexer;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.druid.java.util.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class InputRowSerde
{
  private static final Logger log = new Logger(InputRowSerde.class);

  private final List<String> forwardingDimensions;
  private final List<String> deserializedDimensions;
  private final List<String> auxColumns;

  public InputRowSerde(AggregatorFactory[] aggregatorFactories, List<String> exportDimensions)
  {
    this(aggregatorFactories, exportDimensions, Lists.newArrayList(exportDimensions));
  }

  public InputRowSerde(
      AggregatorFactory[] aggregatorFactories,
      List<String> forwardingDimensions,
      List<String> deserializedDimensions
  )
  {
    this.forwardingDimensions = forwardingDimensions;
    this.auxColumns = Lists.newArrayList();
    for (AggregatorFactory factory : aggregatorFactories) {
      for (String required : factory.requiredFields()) {
        if (!forwardingDimensions.contains(required) && !auxColumns.contains(required)) {
          auxColumns.add(required);
        }
      }
    }
    this.deserializedDimensions = deserializedDimensions;
    log.info("serde with forwarding dimensions " + forwardingDimensions + " and aux columns " + auxColumns);
    if (!deserializedDimensions.equals(forwardingDimensions)) {
      log.info("deserialized rows will use " + deserializedDimensions + " as dimension names");
    }
    log.info("forwardingDimensions: " + forwardingDimensions + ", auxColumns: " + auxColumns);
  }

  public byte[] serialize(final InputRow row)
  {
    final ByteArrayDataOutput out = ByteStreams.newDataOutput(256);

    try {
      //write timestamp
      out.writeLong(row.getTimestampFromEpoch());

      //writing all dimensions
      for (String dim : forwardingDimensions) {
        Object dimValue = row.getRaw(dim);
        if (dimValue == null) {
          out.writeByte(0);
        } else if (dimValue instanceof List) {
          writeStringArray((List) dimValue, out);
        } else {
          out.writeByte(1);
          writeString(String.valueOf(dimValue), out);
        }
      }
      for (String aux : auxColumns) {
        Object dimValue = row.getRaw(aux);
        if (dimValue instanceof List) {
          List list = (List)dimValue;
          out.writeByte(8);
          WritableUtils.writeVInt(out, list.size());
          for (Object o : list) {
            writeObject(out, o);
          }
        } else {
          writeObject(out, dimValue);
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return out.toByteArray();
  }

  private void writeObject(ByteArrayDataOutput out, Object dimValue) throws IOException
  {
    if (dimValue == null) {
      out.writeByte(0);
    } else if (dimValue instanceof Number) {
      if (dimValue instanceof Float) {
        out.writeByte(1);
        out.writeFloat((Float) dimValue);
      } else if (dimValue instanceof Double) {
        out.writeByte(2);
        out.writeDouble((Double) dimValue);
      } else if (dimValue instanceof Byte) {
        out.writeByte(3);
        out.writeByte((Byte) dimValue);
      } else if (dimValue instanceof Short) {
        out.writeByte(4);
        out.writeShort((Short) dimValue);
      } else if (dimValue instanceof Integer) {
        out.writeByte(5);
        out.writeInt((Integer) dimValue);
      } else if (dimValue instanceof Long) {
        out.writeByte(6);
        out.writeLong((Long) dimValue);
      } else {
        out.writeByte(7);
        writeString(String.valueOf(dimValue), out);
      }
    } else {
      out.writeByte(7);
      writeString(String.valueOf(dimValue), out);
    }
  }

  public MapBasedInputRow deserialize(final byte[] input)
  {
    return deserialize(input, false);
  }

  public MapBasedInputRow deserialize(final byte[] input, final boolean mutable)
  {
    final ByteArrayDataInput in = ByteStreams.newDataInput(input);

    try {
      //Read timestamp
      long timestamp = in.readLong();
      //Read dimensions
      final Map<String, Object> event = Maps.newHashMapWithExpectedSize(forwardingDimensions.size() + auxColumns.size());
      for (String dimension : forwardingDimensions) {
        int count = WritableUtils.readVInt(in);
        if (count == 1) {
          event.put(dimension, readString(in));
        } else if (count > 1) {
          List<String> values = Lists.newArrayListWithCapacity(count);
          for (int j = 0; j < count; j++) {
            values.add(readString(in));
          }
          event.put(dimension, values);
        }
      }

      //Read metrics
      for (String aux : auxColumns) {
        int code = in.readByte();
        if (code == 8) {
          int size = WritableUtils.readVInt(in);
          List list = Lists.newArrayListWithCapacity(size);
          for (int i = 0; i < size; i++) {
            list.add(readObject(in));
          }
          event.put(aux, list);
        } else {
          event.put(aux, readObject(in, code));
        }
      }

      final List<String> rowDimensions = mutable ? Lists.newArrayList(deserializedDimensions) : deserializedDimensions;
      return new MapBasedInputRow(timestamp, rowDimensions, event);
    }
    catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  private Object readObject(ByteArrayDataInput in) throws IOException
  {
    return readObject(in, in.readByte());
  }

  private Object readObject(ByteArrayDataInput in, int code) throws IOException
  {
    switch (code) {
      case 0:
        return null;
      case 1:
        return in.readFloat();
      case 2:
        return in.readDouble();
      case 3:
        return in.readByte();
      case 4:
        return in.readShort();
      case 5:
        return in.readInt();
      case 6:
        return in.readLong();
      case 7:
        return readString(in);
      default:
        throw new IllegalStateException("invalid code " + code);
    }
  }

  private static void writeString(String value, ByteArrayDataOutput out) throws IOException
  {
    byte[] bytes = value.getBytes(Charsets.UTF_8);
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static void writeStringArray(List<String> values, ByteArrayDataOutput out) throws IOException
  {
    if (values == null || values.size() == 0) {
      WritableUtils.writeVInt(out, 0);
      return;
    }
    WritableUtils.writeVInt(out, values.size());
    for (String value : values) {
      writeString(value, out);
    }
  }

  private static String readString(DataInput in) throws IOException
  {
    byte[] result = new byte[WritableUtils.readVInt(in)];
    in.readFully(result, 0, result.length);
    return new String(result, Charsets.UTF_8);
  }
}
