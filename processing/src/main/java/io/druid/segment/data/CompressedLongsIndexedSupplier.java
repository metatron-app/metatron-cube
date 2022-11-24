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

import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedLongsIndexedSupplier implements ColumnPartProvider<IndexedLongs>, ColumnPartSerde.Serializer
{
  public static final int MAX_LONGS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Long.BYTES;

  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    final byte versionFromBuffer = buffer.get();
    final int numRows = buffer.getInt();
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
        numRows,
        sizePer,
        GenericIndexed.read(buffer, strategy),
        compression
    );
  }

  private final int numRows;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers;
  private final CompressionStrategy compression;

  public CompressedLongsIndexedSupplier(
      int numRows,
      int sizePer,
      GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers,
      CompressionStrategy compression
  )
  {
    this.numRows = numRows;
    this.sizePer = sizePer;
    this.baseLongBuffers = baseLongBuffers;
    this.compression = compression;
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return compression;
  }

  @Override
  public IndexedLongs get()
  {
    return new CompressedIndexedLongs();
  }

  @Override
  public long getSerializedSize()
  {
    return baseLongBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numRows)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseLongBuffers.writeToChannel(channel);
  }

  private class CompressedIndexedLongs implements IndexedLongs
  {
    private final GenericIndexed<ResourceHolder<LongBuffer>> singleThreaded = baseLongBuffers.asSingleThreaded();

    private int currIndex = -1;
    private ResourceHolder<LongBuffer> holder;
    private LongBuffer buffer;
    private int bufferPos = -1;

    @Override
    public int size()
    {
      return numRows;
    }

    @Override
    public long get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return buffer.get(bufferPos + bufferIndex);
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
      buffer.position(bufferPos + bufferIndex);

      final int numToGet = Math.min(buffer.remaining(), toFill.length);
      buffer.get(toFill, 0, numToGet);
      buffer.reset();

      return numToGet;
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
      return "CompressedIndexedLongs{" +
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
}
