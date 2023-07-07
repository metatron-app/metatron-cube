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
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.IndexIO;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by CompressedVSizeIntsIndexedSupplier
 */
public class CompressedVSizeIntWriter extends SingleValueIndexedIntsWriter implements ColumnPartWriter.Compressed
{
  public static CompressedVSizeIntWriter create(
      final IOPeon ioPeon,
      final String filenameBase,
      final int maxValue,
      final CompressionStrategy compression
  )
  {
    return new CompressedVSizeIntWriter(
        ioPeon, filenameBase, maxValue,
        CompressedVSizedIntSupplier.maxIntsInBufferForValue(maxValue),
        IndexIO.BYTE_ORDER, compression
    );
  }

  private final int numBytes;
  private final int chunkFactor;
  private final int chunkBytes;
  private final ByteOrder byteOrder;
  private final CompressionStrategy compression;
  private final ColumnPartWriter<ResourceHolder<ByteBuffer>> flattener;
  private final ByteBuffer intBuffer;
  private ByteBuffer endBuffer;
  private int numInserted;

  public CompressedVSizeIntWriter(
      final IOPeon ioPeon,
      final String filenameBase,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression
  )
  {
    this.numBytes = VSizedInt.getNumBytesForMax(maxValue);
    this.chunkFactor = chunkFactor;
    this.chunkBytes = chunkFactor * numBytes + CompressedVSizedIntSupplier.bufferPadding(numBytes);
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.flattener = GenericIndexedWriter.v2(
        ioPeon, filenameBase, CompressedByteBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkBytes)
    );
    this.intBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
    this.endBuffer = ByteBuffer.allocate(chunkBytes).order(byteOrder);
    this.endBuffer.limit(numBytes * chunkFactor);
    this.numInserted = 0;
  }

  @Override
  public CompressionStrategy appliedCompression()
  {
    return compression;
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void add(int val) throws IOException
  {
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.add(StupidResourceHolder.create(endBuffer));
      endBuffer = ByteBuffer.allocate(chunkBytes).order(byteOrder);
      endBuffer.limit(numBytes * chunkFactor);
    }
    intBuffer.putInt(0, val);
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
      endBuffer.put(intBuffer.array(), Integer.BYTES - numBytes, numBytes);
    } else {
      endBuffer.put(intBuffer.array(), 0, numBytes);
    }
    numInserted++;
  }

  @Override
  public void close() throws IOException
  {
    try {
      if (numInserted > 0) {
        endBuffer.limit(endBuffer.position());
        endBuffer.rewind();
        flattener.add(StupidResourceHolder.create(endBuffer));
      }
      endBuffer = null;
    }
    finally {
      flattener.close();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +             // version
           1 +             // numBytes
           Integer.BYTES + // numInserted
           Integer.BYTES + // chunkFactor
           1 +             // compression id
           flattener.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID, (byte) numBytes}));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(chunkFactor)));
    written += channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    written += flattener.writeToChannel(channel);
    return written;
  }
}
