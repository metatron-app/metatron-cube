/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedLongsSupplierSerializer implements ColumnPartWriter<Long>
{
  public static CompressedLongsSupplierSerializer create(
      final IOPeon ioPeon,
      final String filenameBase,
      final ByteOrder order,
      final CompressedObjectStrategy.CompressionStrategy compression
  ) throws IOException
  {
    return new CompressedLongsSupplierSerializer(
        CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER,
        new GenericIndexedWriter<ResourceHolder<LongBuffer>>(
            ioPeon,
            filenameBase,
            CompressedLongBufferObjectStrategy.getBufferForOrder(
                order,
                compression,
                CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER
            )
        ),
        compression
    );
  }

  private final int sizePer;
  private final ColumnPartWriter<ResourceHolder<LongBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private int numInserted = 0;

  private LongBuffer endBuffer;

  public CompressedLongsSupplierSerializer(
      int sizePer,
      ColumnPartWriter<ResourceHolder<LongBuffer>> flattener,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.sizePer = sizePer;
    this.flattener = flattener;
    this.compression = compression;

    endBuffer = LongBuffer.allocate(sizePer);
    endBuffer.mark();
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(Long value) throws IOException
  {
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.add(StupidResourceHolder.create(endBuffer));
      endBuffer = LongBuffer.allocate(sizePer);
      endBuffer.mark();
    }

    endBuffer.put(value);
    ++numInserted;
  }

  @Override
  public void close() throws IOException
  {
    endBuffer.limit(endBuffer.position());
    endBuffer.rewind();
    flattener.add(StupidResourceHolder.create(endBuffer));
    endBuffer = null;
    flattener.close();
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +              // version
           Ints.BYTES +     // elements num
           Ints.BYTES +     // sizePer
           1 +              // compression id
           flattener.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{CompressedFloatsIndexedSupplier.version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    flattener.writeToChannel(channel);
  }
}
