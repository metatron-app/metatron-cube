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
import io.druid.segment.column.DoubleScanner;
import io.druid.segment.column.IntDoubleConsumer;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;
import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedDoublesIndexedSupplier implements ColumnPartProvider<IndexedDoubles>, ColumnPartSerde.Serializer
{
  public static final int MAX_DOUBLES_IN_BUFFER = CompressedPools.BUFFER_SIZE / Double.BYTES;

  public static CompressedDoublesIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
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

    final CompressedDoubleBufferObjectStrategy strategy =
        CompressedDoubleBufferObjectStrategy.getBufferForOrder(order, compression, sizePer);

    return new CompressedDoublesIndexedSupplier(
        numRows,
        sizePer,
        GenericIndexed.read(buffer, strategy),
        compression
    );
  }

  private final int numRows;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<DoubleBuffer>> baseDoubleBuffers;
  private final CompressionStrategy compression;

  public CompressedDoublesIndexedSupplier(
      int numRows,
      int sizePer,
      GenericIndexed<ResourceHolder<DoubleBuffer>> baseDoubleBuffers,
      CompressionStrategy compression
  )
  {
    this.numRows = numRows;
    this.sizePer = sizePer;
    this.baseDoubleBuffers = baseDoubleBuffers;
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
  public long getSerializedSize()
  {
    return baseDoubleBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numRows)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseDoubleBuffers.writeToChannel(channel);
  }

  @Override
  public Class<? extends IndexedDoubles> provides()
  {
    return CompressedIndexedDoubles.class;
  }

  @Override
  public IndexedDoubles get()
  {
    return new CompressedIndexedDoubles();
  }

  private class CompressedIndexedDoubles implements IndexedDoubles
  {
    private final GenericIndexed<ResourceHolder<DoubleBuffer>> singleThreaded = baseDoubleBuffers.dedicated();

    private int currIndex = -1;
    private ResourceHolder<DoubleBuffer> holder;
    private DoubleBuffer buffer;
    private int bufferPos = -1;

    @Override
    public int size()
    {
      return numRows;
    }

    @Override
    public double get(final int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }
      return buffer.get(bufferPos + bufferIndex);
    }

    @Override
    public int fill(final int index, final double[] toFill)
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

    @Override
    public void scan(final IntIterator iterator, final DoubleScanner scanner)
    {
      Int2DoubleFunction f = x -> buffer.get(bufferPos + x % sizePer);
      if (iterator == null) {
        final int size = size();
        for (int x = 0; x < size; x++) {
          if (x / sizePer != currIndex) {
            loadBuffer(x / sizePer);
          }
          scanner.apply(x, f);
        }
      } else {
        while (iterator.hasNext()) {
          final int x = iterator.next();
          if (x / sizePer != currIndex) {
            loadBuffer(x / sizePer);
          }
          scanner.apply(x, f);
        }
      }
    }

    @Override
    public void consume(final IntIterator iterator, final IntDoubleConsumer consumer)
    {
      if (iterator == null) {
        final int size = size();
        for (int x = 0; x < size; x++) {
          if (x / sizePer != currIndex) {
            loadBuffer(x / sizePer);
          }
          consumer.apply(x, buffer.get(bufferPos + x % sizePer));
        }
      } else {
        while (iterator.hasNext()) {
          final int x = iterator.next();
          if (x / sizePer != currIndex) {
            loadBuffer(x / sizePer);
          }
          consumer.apply(x, buffer.get(bufferPos + x % sizePer));
        }
      }
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
      return "CompressedIndexedDoubles{" +
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
