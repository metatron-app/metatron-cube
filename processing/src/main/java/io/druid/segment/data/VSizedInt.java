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

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class VSizedInt extends IndexedInts.Abstract implements Comparable<VSizedInt>
{
  public static final byte VERSION = 0x0;

  public static VSizedInt fromArray(int[] array)
  {
    return fromArray(array, Ints.max(array));
  }

  public static VSizedInt fromArray(int[] array, int maxValue)
  {
    return fromList(Ints.asList(array), maxValue);
  }

  public static VSizedInt empty()
  {
    return fromList(Lists.<Integer>newArrayList(), 0);
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

  public static VSizedInt fromList(List<Integer> list, int maxValue)
  {
    int numBytes = getNumBytesForMax(maxValue);

    final ByteBuffer buffer = ByteBuffer.allocate((list.size() * numBytes) + (4 - numBytes));
    writeToBuffer(buffer, list, numBytes, maxValue);

    return new VSizedInt(buffer.asReadOnlyBuffer(), numBytes);
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
  private final int numBytes;

  private final int bitsToShift;
  private final int size;

  public VSizedInt(ByteBuffer buffer, int numBytes)
  {
    this.buffer = buffer;
    this.numBytes = numBytes;

    bitsToShift = 32 - (numBytes << 3); // numBytes * 8

    int numBufferBytes = 4 - numBytes;
    size = (buffer.remaining() - numBufferBytes) / numBytes;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    return buffer.getInt(buffer.position() + (index * numBytes)) >>> bitsToShift;
  }

  public byte[] getBytesNoPadding()
  {
    int bytesToTake = buffer.remaining() - (4 - numBytes);
    byte[] bytes = new byte[bytesToTake];
    buffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }

  public byte[] getBytes()
  {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.asReadOnlyBuffer().get(bytes);
    return bytes;
  }

  @Override
  public int compareTo(VSizedInt o)
  {
    int retVal = Ints.compare(numBytes, o.numBytes);

    if (retVal == 0) {
      retVal = buffer.compareTo(o.buffer);
    }

    return retVal;
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

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(buffer.remaining())));
    channel.write(buffer.asReadOnlyBuffer());
  }

  public static VSizedInt readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VSizedInt(
          bufferToUse,
          numBytes
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public WritableSupplier<IndexedInts> asWritableSupplier() {
    return new VSizeIndexedIntsSupplier(this);
  }

  public static class VSizeIndexedIntsSupplier implements WritableSupplier<IndexedInts> {
    final VSizedInt delegate;

    public VSizeIndexedIntsSupplier(VSizedInt delegate) {
      this.delegate = delegate;
    }

    @Override
    public int numRows()
    {
      return delegate.size();
    }

    @Override
    public long getSerializedSize()
    {
      return delegate.getSerializedSize();
    }

    @Override
    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      delegate.writeToChannel(channel);
    }

    @Override
    public IndexedInts get()
    {
      return delegate;
    }
  }
}