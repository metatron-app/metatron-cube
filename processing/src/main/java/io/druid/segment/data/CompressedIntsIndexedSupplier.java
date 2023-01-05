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
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

public class CompressedIntsIndexedSupplier implements WritableSupplier<IndexedInts>
{
  public static final int MAX_INTS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Integer.BYTES;

  public static CompressedIntsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == ColumnPartSerde.WITH_COMPRESSION_ID) {
      final int numRows = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.forId(buffer.get());
      return new CompressedIntsIndexedSupplier(
          numRows,
          sizePer,
          GenericIndexed.read(buffer, CompressedIntBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final int numRows;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<IntBuffer>> baseIntBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedIntsIndexedSupplier(
      int numRows,
      int sizePer,
      GenericIndexed<ResourceHolder<IntBuffer>> baseIntBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.numRows = numRows;
    this.sizePer = sizePer;
    this.baseIntBuffers = baseIntBuffers;
    this.compression = compression;
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  public long getSerializedSize()
  {
    return 1 + // version
           4 + // numRows
           4 + // sizePer
           1 + // compressionId
           baseIntBuffers.getSerializedSize(); // data
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numRows)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseIntBuffers.writeToChannel(channel);
  }

  @Override
  public IndexedInts get()
  {
    return new CompressedIndexedInts();
  }

  @Override
  public Class<? extends IndexedInts> provides()
  {
    return CompressedIndexedInts.class;
  }

  private class CompressedIndexedInts implements IndexedInts
  {
    private final GenericIndexed<ResourceHolder<IntBuffer>> singleThreaded = baseIntBuffers.asSingleThreaded();

    private int currIndex = -1;
    private ResourceHolder<IntBuffer> holder;
    private IntBuffer buffer;
    private int bufferPos = -1;

    @Override
    public int size()
    {
      return numRows;
    }

    @Override
    public int get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return buffer.get(bufferPos + bufferIndex);
    }

    private void loadBuffer(int bufferNum)
    {
      if (singleThreaded.isRecyclable()) {
        holder = singleThreaded.get(bufferNum, holder);
      } else {
        CloseQuietly.close(holder);
        holder = singleThreaded.get(bufferNum);
      }
      buffer = holder.get();
      bufferPos = buffer.position();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedIndexedInts{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreaded.size() +
             ", numRows=" + numRows +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
      Closeables.close(singleThreaded, false);
    }
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<IntBuffer>> getBaseIntBuffers()
  {
    return baseIntBuffers;
  }

  public static CompressedIntsIndexedSupplier fromList(
      final List<Integer> list , final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedIntsIndexedSupplier(
        list.size(),
        chunkFactor,
        GenericIndexed.v2(
            new Iterable<ResourceHolder<IntBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<IntBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<IntBuffer>>()
                {
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ResourceHolder<IntBuffer> next()
                  {
                    IntBuffer retVal = IntBuffer.allocate(chunkFactor);

                    if (chunkFactor > list.size() - position) {
                      retVal.limit(list.size() - position);
                    }
                    final List<Integer> ints = list.subList(position, position + retVal.remaining());
                    for(int value : ints) {
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
            CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }
}
