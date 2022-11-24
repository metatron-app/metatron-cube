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
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by CompressedIntsIndexedSupplier
 */
public class CompressedIntsIndexedWriter extends SingleValueIndexedIntsWriter
{
  public static CompressedIntsIndexedWriter create(
      final IOPeon ioPeon,
      final String filenameBase,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    return new CompressedIntsIndexedWriter(
        ioPeon,
        filenameBase,
        CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER,
        IndexIO.BYTE_ORDER,
        compression
    );
  }

  private final int chunkFactor;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final ColumnPartWriter<ResourceHolder<IntBuffer>> flattener;
  private IntBuffer endBuffer;
  private int numInserted;

  public CompressedIntsIndexedWriter(
      final IOPeon ioPeon,
      final String filenameBase,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.chunkFactor = chunkFactor;
    this.compression = compression;
    this.flattener = GenericIndexedWriter.v2(
        ioPeon, filenameBase, CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
    );
    this.endBuffer = IntBuffer.allocate(chunkFactor);
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
      endBuffer = IntBuffer.allocate(chunkFactor);
    }
    endBuffer.put(val);
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
  public long getSerializedSize() throws IOException
  {
    return 1 +             // version
           Integer.BYTES + // numInserted
           Integer.BYTES + // chunkFactor
           1 +             // compression id
           flattener.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(chunkFactor)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    flattener.writeToChannel(channel);
  }
}
