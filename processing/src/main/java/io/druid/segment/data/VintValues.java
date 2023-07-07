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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.segment.data.IndexedInts.Shared;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class VintValues implements IntValues, Shared, WritableSupplier<IntValues>
{
  public static final byte VERSION = 0x0;

  public static VintValues fromArray(int[] array)
  {
    return fromArray(array, Ints.max(array));
  }

  public static VintValues fromArray(int[] array, int maxValue)
  {
    return fromList(Ints.asList(array), maxValue);
  }

  /**
   * provide for performance reason.
   */
  public static byte[] getBytesNoPaddingfromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return buffer.array();
  }

  public static VintValues fromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return new VintValues(buffer.asReadOnlyBuffer(), numBytes);
  }

  private static void writeToBuffer(ByteBuffer buffer, List<Integer> list, int numBytes, int maxValue)
  {
    int i = 0;
    for (Integer val : list) {
      if (val < 0) {
        throw new IAE("integer values must be positive, got[%d], i[%d]", val, i);
      }
      if (val > maxValue) {
        throw new IAE("val[%d] > maxValue[%d], please don't lie about maxValue.  i[%d]", val, maxValue, i);
      }

      byte[] intAsBytes = Ints.toByteArray(val);
      buffer.put(intAsBytes, intAsBytes.length - numBytes, numBytes);
      ++i;
    }
    buffer.position(0);
  }

  public static byte getNumBytesForMax(int maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }

    byte numBytes = 4;
    if (maxValue <= 0xFF) {
      numBytes = 1;
    }
    else if (maxValue <= 0xFFFF) {
      numBytes = 2;
    }
    else if (maxValue <= 0xFFFFFF) {
      numBytes = 3;
    }
    return numBytes;
  }

  private final ByteBuffer buffer;
  private final int position;
  private final int numBytes;
  private final RowReader reader;

  private final int size;

  public VintValues(ByteBuffer buffer, int numBytes)
  {
    this.buffer = buffer;
    this.position = buffer.position();
    this.numBytes = numBytes;
    this.reader = makeReader(buffer, numBytes);
    this.size = (buffer.remaining() - (4 - numBytes)) / numBytes;
  }

  private static interface RowReader
  {
    int read(int index);
  }

  private static RowReader makeReader(ByteBuffer buffer, int numBytes)
  {
    switch (numBytes) {
      case 1:
        return x -> buffer.get(x) & 0xff;
      case 2:
        return buffer.order() == ByteOrder.BIG_ENDIAN ?
               x -> be(buffer.get(x), buffer.get(x + 1)) :
               x -> le(buffer.get(x), buffer.get(x + 1));
      case 3:
        return buffer.order() == ByteOrder.BIG_ENDIAN ?
               x -> be(buffer.get(x), buffer.get(x + 1), buffer.get(x + 2)) :
               x -> le(buffer.get(x), buffer.get(x + 1), buffer.get(x + 2));
      case 4:
        return x -> buffer.getInt(x);
      default:
        throw new ISE("Invalid vint length " + numBytes);
    }
  }

  @Override
  public IndexedInts asSingleThreaded(int cache)
  {
    if (numBytes == Integer.BYTES) {
      return this;
    }
    final ByteBuffer dedicated = buffer.asReadOnlyBuffer();
    return new VintValues(dedicated, numBytes)
    {
      private final byte[] temp = new byte[cache * numBytes];
      private final RowReader reader = fromBuffer(dedicated.order(), temp, numBytes);

      @Override
      public int get(int index, int[] convey)
      {
        final int limit = Math.min(size() - index, convey.length);
        dedicated.position(position + index * numBytes);
        dedicated.get(temp, 0, limit * numBytes).position(position);
        for (int i = 0; i < limit; i++) {
          convey[i] = reader.read(i * numBytes);
        }
        return limit;
      }
    };
  }

  private static RowReader fromBuffer(ByteOrder order, byte[] buffer, int numBytes)
  {
    switch (numBytes) {
      case 1:
        return x -> buffer[x] & 0xff;
      case 2:
        return order == ByteOrder.BIG_ENDIAN ?
               x -> be(buffer[x], buffer[x + 1]) :
               x -> le(buffer[x], buffer[x + 1]);
      case 3:
        return order == ByteOrder.BIG_ENDIAN ?
               x -> be(buffer[x], buffer[x + 1], buffer[x + 2]) :
               x -> le(buffer[x], buffer[x + 1], buffer[x + 2]);
      default:
        throw new ISE("Invalid vint length " + numBytes);
    }
  }

  private static int be(byte b0, byte b1)
  {
    return ((b0 & 0xff) << 8) + (b1 & 0xff);
  }

  private static int be(byte b0, byte b1, byte b2)
  {
    return ((b0 & 0xff) << 16) + ((b1 & 0xff) << 8) + (b2 & 0xff);
  }

  private static int le(byte b0, byte b1)
  {
    return (b0 & 0xff) + ((b1 & 0xff) << 8);
  }

  private static int le(byte b0, byte b1, byte b2)
  {
    return (b0 & 0xff) + ((b1 & 0xff) << 8) + ((b2 & 0xff) << 16);
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    return reader.read(position + (index * numBytes));
  }

  public byte[] getBytesNoPadding()
  {
    final int bytesToTake = buffer.remaining() - (4 - numBytes);
    final byte[] bytes = new byte[bytesToTake];
    buffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }

  public int getNumBytes()
  {
    return numBytes;
  }

  public long getSerializedSize()
  {
    // version, numBytes, size, remaining
    return 1 + 1 + 4 + buffer.remaining();
  }

  @Override
  public int numRows()
  {
    return size;
  }

  @Override
  public Class<? extends IntValues> provides()
  {
    return VintValues.class;
  }

  @Override
  public IntValues get()
  {
    return this;
  }

  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = channel.write(ByteBuffer.wrap(new byte[]{VERSION, (byte) numBytes}));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(buffer.remaining())));
    written += channel.write(buffer.asReadOnlyBuffer());
    return written;
  }

  public static VintValues readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VintValues(bufferToUse, numBytes);
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
