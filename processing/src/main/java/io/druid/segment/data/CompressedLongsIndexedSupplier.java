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
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class CompressedLongsIndexedSupplier implements Supplier<IndexedLongs>, ColumnPartSerde.Serializer
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;
  public static final int MAX_LONGS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Long.BYTES;


  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers;
  private final CompressionStrategy compression;

  public CompressedLongsIndexedSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers,
      CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseLongBuffers = baseLongBuffers;
    this.compression = compression;
  }

  public int size()
  {
    return totalSize;
  }

  public CompressionStrategy compressionType()
  {
    return compression;
  }

  @Override
  public IndexedLongs get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedIndexedLongs() {
        @Override
        public long get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currIndex) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return buffer.get(buffer.position() + bufferIndex);
        }
      };
    } else {
      return new CompressedIndexedLongs();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return baseLongBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseLongBuffers.writeToChannel(channel);
  }

  public CompressedLongsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedLongsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.fromIterable(baseLongBuffers, CompressedLongBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<LongBuffer>> getBaseLongBuffers()
  {
    return baseLongBuffers;
  }

  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final byte versionFromBuffer = buffer.get();
    final int totalSize = buffer.getInt();
    final int sizePer = buffer.getInt();

    final CompressionStrategy compression;
    if (versionFromBuffer == ColumnPartSerde.WITH_COMPRESSION_ID) {
      compression = CompressedObjectStrategy.forId(buffer.get());
    } else if (versionFromBuffer == ColumnPartSerde.LZF_FIXED) {
      compression = CompressionStrategy.LZF;
    } else {
      throw new IAE("Unknown version[%s]", versionFromBuffer);
    }

    final CompressedLongBufferObjectStrategy strategy =
        CompressedLongBufferObjectStrategy.getBufferForOrder(order, compression, sizePer);

    return new CompressedLongsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.read(buffer, strategy),
        compression
    );
  }

  public static CompressedLongsIndexedSupplier fromLongBuffer(LongBuffer buffer, final ByteOrder byteOrder, CompressionStrategy compression)
  {
    return fromLongBuffer(buffer, MAX_LONGS_IN_BUFFER, byteOrder, compression);
  }

  public static CompressedLongsIndexedSupplier fromLongBuffer(
      final LongBuffer buffer, final int chunkFactor, final ByteOrder byteOrder, CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_LONGS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedLongsIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<LongBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<LongBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<LongBuffer>>()
                {
                  LongBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourceHolder<LongBuffer> next()
                  {
                    LongBuffer retVal = myBuffer.asReadOnlyBuffer();

                    if (chunkFactor < myBuffer.remaining()) {
                      retVal.limit(retVal.position() + chunkFactor);
                    }
                    myBuffer.position(myBuffer.position() + retVal.remaining());

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
            CompressedLongBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }

  public static CompressedLongsIndexedSupplier fromList(
      final List<Long> list , final int chunkFactor, final ByteOrder byteOrder, CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_LONGS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedLongsIndexedSupplier(
        list.size(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<LongBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<LongBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<LongBuffer>>()
                {
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ResourceHolder<LongBuffer> next()
                  {
                    LongBuffer retVal = LongBuffer.allocate(chunkFactor);

                    if (chunkFactor > list.size() - position) {
                      retVal.limit(list.size() - position);
                    }
                    final List<Long> longs = list.subList(position, position + retVal.remaining());
                    for (long value : longs) {
                      retVal.put(value);
                    }
                    retVal.rewind();
                    position += retVal.remaining();

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
            CompressedLongBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }

  private class CompressedIndexedLongs implements IndexedLongs
  {
    final Indexed<ResourceHolder<LongBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<LongBuffer> holder;
    LongBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return buffer.get(buffer.position() + bufferIndex);
    }

    @Override
    public int fill(int index, long[] toFill)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      buffer.mark();
      buffer.position(buffer.position() + bufferIndex);

      final int numToGet = Math.min(buffer.remaining(), toFill.length);
      buffer.get(toFill, 0, numToGet);
      buffer.reset();

      return numToGet;
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedLongBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public int binarySearch(long key)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int binarySearch(long key, int from, int to)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
      return "CompressedLongsIndexedSupplier_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedLongBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
    }
  }
}
