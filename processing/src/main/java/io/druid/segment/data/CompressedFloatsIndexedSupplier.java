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
import java.nio.FloatBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class CompressedFloatsIndexedSupplier implements Supplier<IndexedFloats>, ColumnPartSerde.Serializer
{
  public static final int MAX_FLOATS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Float.BYTES;

  private final int numRows;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<FloatBuffer>> baseFloatBuffers;
  private final CompressionStrategy compression;

  public CompressedFloatsIndexedSupplier(
      int numRows,
      int sizePer,
      GenericIndexed<ResourceHolder<FloatBuffer>> baseFloatBuffers,
      CompressionStrategy compression
  )
  {
    this.numRows = numRows;
    this.sizePer = sizePer;
    this.baseFloatBuffers = baseFloatBuffers;
    this.compression = compression;
  }

  public int size()
  {
    return numRows;
  }

  public CompressionStrategy compressionType()
  {
    return compression;
  }

  @Override
  public IndexedFloats get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedIndexedFloats() {
        @Override
        public float get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currIndex) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return buffer.get(bufferPos + bufferIndex);
        }
      };
    } else {
      return new CompressedIndexedFloats();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return baseFloatBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numRows)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseFloatBuffers.writeToChannel(channel);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }

  public CompressedFloatsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedFloatsIndexedSupplier(
        numRows,
        sizePer,
        GenericIndexed.fromIterable(baseFloatBuffers, CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing. Do not depend on unless you like things breaking.
   */
  GenericIndexed<ResourceHolder<FloatBuffer>> getBaseFloatBuffers()
  {
    return baseFloatBuffers;
  }

  public static CompressedFloatsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
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

    final CompressedFloatBufferObjectStrategy strategy =
        CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, sizePer);

    return new CompressedFloatsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.read(buffer, strategy),
        compression
    );
  }

  public static CompressedFloatsIndexedSupplier fromFloatBuffer(FloatBuffer buffer, final ByteOrder order, CompressionStrategy compression)
  {
    return fromFloatBuffer(buffer, MAX_FLOATS_IN_BUFFER, order, compression);
  }

  public static CompressedFloatsIndexedSupplier fromFloatBuffer(
      final FloatBuffer buffer, final int chunkFactor, final ByteOrder order, final CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_FLOATS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedFloatsIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<FloatBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<FloatBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<FloatBuffer>>()
                {
                  FloatBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourceHolder<FloatBuffer> next()
                  {
                    final FloatBuffer retVal = myBuffer.asReadOnlyBuffer();

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
            CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, chunkFactor)
        ),
        compression
    );
  }

  private class CompressedIndexedFloats implements IndexedFloats
  {
    final Indexed<ResourceHolder<FloatBuffer>> singleThreadedFloatBuffers = baseFloatBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<FloatBuffer> holder;
    FloatBuffer buffer;
    int bufferPos = -1;

    @Override
    public int size()
    {
      return numRows;
    }

    @Override
    public float get(final int index)
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
    public int fill(int index, float[] toFill)
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

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedFloatBuffers.get(bufferNum);
      buffer = holder.get();
      bufferPos = buffer.position();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedFloatsIndexedSupplier{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedFloatBuffers.size() +
             ", numRows=" + numRows +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
    }
  }
}
