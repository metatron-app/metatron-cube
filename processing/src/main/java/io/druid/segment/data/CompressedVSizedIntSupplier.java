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

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.CompressedPools;
import io.druid.segment.bitmap.IntIterators;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

public class CompressedVSizedIntSupplier implements WritableSupplier<IndexedInts>
{
  public static final byte VERSION = 0x2;

  private final int totalSize;
  private final int sizePer;
  private final int numBytes;
  private final int bigEndianShift;
  private final int littleEndianMask;
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedVSizedIntSupplier(
      int totalSize,
      int sizePer,
      int numBytes,
      GenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        sizePer == (1 << Integer.numberOfTrailingZeros(sizePer)),
        "Number of entries per chunk must be a power of 2"
    );

    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseBuffers = baseBuffers;
    this.compression = compression;
    this.numBytes = numBytes;
    this.bigEndianShift = Integer.SIZE - (numBytes << 3); // numBytes * 8
    this.littleEndianMask = (int) ((1L << (numBytes << 3)) - 1); // set numBytes * 8 lower bits to 1
  }

  public static int maxIntsInBufferForBytes(int numBytes)
  {
    int maxSizePer = (CompressedPools.BUFFER_SIZE - bufferPadding(numBytes)) / numBytes;
    // round down to the nearest power of 2
    return 1 << (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(maxSizePer));
  }

  public static int bufferPadding(int numBytes)
  {
    // when numBytes == 3 we need to pad the buffer to allow reading an extra byte
    // beyond the end of the last value, since we use buffer.getInt() to read values.
    // for numBytes 1, 2 we remove the need for padding by reading bytes or shorts directly.
    switch (numBytes) {
      case Short.BYTES:
      case 1:
        return 0;
      default:
        return Integer.BYTES - numBytes;
    }
  }

  public static int maxIntsInBufferForValue(int maxValue)
  {
    return maxIntsInBufferForBytes(VSizedInt.getNumBytesForMax(maxValue));
  }

  public int numRows()
  {
    return totalSize;
  }

  @Override
  public IndexedInts get()
  {
    // optimized versions for int, short, and byte columns
    if (numBytes == Integer.BYTES) {
      return new CompressedFullSizeIndexedInts();
    } else if (numBytes == Short.BYTES) {
      return new CompressedShortSizeIndexedInts();
    } else if (numBytes == 1) {
      return new CompressedByteSizeIndexedInts();
    } else {
      // default version of everything else, i.e. 3-bytes per value
      return new CompressedVSizeIndexedInts();
    }
  }


  @Override
  public long getSerializedSize()
  {
    return 1 +             // version
           1 +             // numBytes
           Integer.BYTES + // totalSize
           Integer.BYTES + // sizePer
           1 +             // compression id
           baseBuffers.getSerializedSize(); // data
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION, (byte) numBytes}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseBuffers.writeToChannel(channel);
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<ByteBuffer>> getBaseBuffers()
  {
    return baseBuffers;
  }

  public static CompressedVSizedIntSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final int numBytes = buffer.get();
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final int chunkBytes = sizePer * numBytes + bufferPadding(numBytes);

      final CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.forId(buffer.get());

      return new CompressedVSizedIntSupplier(
          totalSize,
          sizePer,
          numBytes,
          GenericIndexed.read(
              buffer,
              CompressedByteBufferObjectStrategy.getBufferForOrder(order, compression, chunkBytes)
          ),
          compression
      );

    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static CompressedVSizedIntSupplier fromList(
      final List<Integer> list,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    final int numBytes = VSizedInt.getNumBytesForMax(maxValue);
    final int chunkBytes = chunkFactor * numBytes + bufferPadding(numBytes);

    Preconditions.checkArgument(
        chunkFactor <= maxIntsInBufferForBytes(numBytes),
        "Chunks must be <= 64k bytes. chunkFactor was[%s]",
        chunkFactor
    );

    return new CompressedVSizedIntSupplier(
        list.size(),
        chunkFactor,
        numBytes,
        GenericIndexed.v2(
            new Iterable<ResourceHolder<ByteBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<ByteBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<ByteBuffer>>()
                {
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ResourceHolder<ByteBuffer> next()
                  {
                    ByteBuffer retVal = ByteBuffer
                        .allocate(chunkBytes)
                        .order(byteOrder);

                    if (chunkFactor > list.size() - position) {
                      retVal.limit((list.size() - position) * numBytes);
                    } else {
                      retVal.limit(chunkFactor * numBytes);
                    }

                    final List<Integer> ints = list.subList(position, position + retVal.remaining() / numBytes);
                    final ByteBuffer buf = ByteBuffer
                        .allocate(Integer.BYTES)
                        .order(byteOrder);
                    final boolean bigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
                    for (int value : ints) {
                      buf.putInt(0, value);
                      if (bigEndian) {
                        retVal.put(buf.array(), Integer.BYTES - numBytes, numBytes);
                      } else {
                        retVal.put(buf.array(), 0, numBytes);
                      }
                    }
                    retVal.rewind();
                    position += retVal.remaining() / numBytes;

                    return StupidResourceHolder.create(retVal);
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            CompressedByteBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkBytes)
        ),
        compression
    );
  }

  private class CompressedFullSizeIndexedInts extends CompressedVSizeIndexedInts
  {
    IntBuffer intBuffer;

    @Override
    protected void loadBuffer(int bufferNum)
    {
      super.loadBuffer(bufferNum);
      intBuffer = buffer.asIntBuffer();
    }

    @Override
    protected int _get(int index)
    {
      return intBuffer.get(intBuffer.position() + index);
    }
  }

  private class CompressedShortSizeIndexedInts extends CompressedVSizeIndexedInts
  {
    ShortBuffer shortBuffer;

    @Override
    protected void loadBuffer(int bufferNum)
    {
      super.loadBuffer(bufferNum);
      shortBuffer = buffer.asShortBuffer();
    }

    @Override
    protected int _get(int index)
    {
      // removes the need for padding
      return shortBuffer.get(shortBuffer.position() + index) & 0xFFFF;
    }
  }

  private class CompressedByteSizeIndexedInts extends CompressedVSizeIndexedInts
  {
    @Override
    protected int _get(int index)
    {
      // removes the need for padding
      return buffer.get(buffer.position() + index) & 0xFF;
    }
  }

  private class CompressedVSizeIndexedInts implements IndexedInts
  {
    final Indexed.Closeable<ResourceHolder<ByteBuffer>> singleThreaded = baseBuffers.asSingleThreaded();

    private final int div = Integer.numberOfTrailingZeros(sizePer);
    private final int rem = sizePer - 1;

    private int pindex = -1;
    private int pvalue = -1;

    private int currIndex = -1;
    private ResourceHolder<ByteBuffer> holder;

    ByteBuffer buffer;
    boolean bigEndian;

    @Override
    public int size()
    {
      return totalSize;
    }

    /**
     * Returns the value at the given index into the column.
     * <p/>
     * Assumes the number of entries in each decompression buffers is a power of two.
     *
     * @param index index of the value in the column
     *
     * @return the value at the given index
     */
    @Override
    public int get(int index)
    {
      if (index == pindex) {
        return pvalue;
      }
      // assumes the number of entries in each buffer is a power of 2
      final int bufferNum = index >> div;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return pvalue = _get((pindex = index) & rem);
    }

    @Override
    public int get(int index, int[] convey)
    {
      final int bufferNum = index >> div;
      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }
      final int offset = index & rem;
      final int n = Math.min(totalSize - index, Math.min(sizePer - offset, convey.length));
      for (int i = 0; i < n; i++) {
        convey[i] = _get(offset + i);
      }
      return n;
    }

    @Override
    public IntIterator iterator()
    {
      return new IntIterators.Abstract()
      {
        private final int[] bulk = new int[Math.min(totalSize, 4096)];

        private IntIterator iterator = IntIterators.EMPTY;
        private int index;

        @Override
        public int next()
        {
          if (!iterator.hasNext()) {
            final int n = get(index, bulk);
            iterator = IntIterators.from(bulk, n);
            index += n;
          }
          return iterator.next();
        }

        @Override
        public boolean hasNext()
        {
          return index < totalSize || iterator.hasNext();
        }
      };
    }

    /**
     * Returns the value at the given index in the current decompression buffer
     *
     * @param index index of the value in the curent buffer
     *
     * @return the value at the given index
     */
    protected int _get(final int index)
    {
      final int pos = buffer.position() + index * numBytes;
      // example for numBytes = 3
      // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
      // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
      return bigEndian ?
             buffer.getInt(pos) >>> bigEndianShift :
             buffer.getInt(pos) & littleEndianMask;
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreaded.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
      bigEndian = buffer.order().equals(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public String toString()
    {
      return "CompressedVSizedIntsIndexedSupplier{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreaded.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
      Closeables.close(singleThreaded, false);
    }
  }

  public static void main(String[] args)
  {
    System.out.println(maxIntsInBufferForBytes(1));
    System.out.println(maxIntsInBufferForBytes(2));
    System.out.println(maxIntsInBufferForBytes(3));
    System.out.println(maxIntsInBufferForBytes(4));
  }
}
