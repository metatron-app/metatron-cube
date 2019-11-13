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
import com.google.common.primitives.Shorts;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class CompressedComplexColumnSerializer implements ColumnPartWriter
{
  public static CompressedComplexColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      CompressedObjectStrategy.CompressionStrategy compression,
      ObjectStrategy strategy
  ) throws IOException
  {
    return new CompressedComplexColumnSerializer(
        new GenericIndexedWriter<ResourceHolder<ByteBuffer>>(
            ioPeon,
            filenameBase,
            new SizePrefixedCompressedObjectStrategy(compression)
        ),
        compression,
        strategy
    );
  }

  private final ColumnPartWriter<ResourceHolder<ByteBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final ObjectStrategy strategy;

  private int rowNum;
  private int offsetInBlock;

  private final ByteBuffer endBuffer;
  private final List<Integer> mappings;   // thresholds for blocks
  private final List<Integer> offsets;    // offsets in each block

  public CompressedComplexColumnSerializer(
      ColumnPartWriter<ResourceHolder<ByteBuffer>> flattener,
      CompressedObjectStrategy.CompressionStrategy compression,
      ObjectStrategy strategy
  )
  {
    this.flattener = flattener;
    this.compression = compression;
    this.strategy = strategy;
    this.endBuffer = ByteBuffer.allocate(CompressedPools.BUFFER_SIZE);  // offset : unsigned short
    this.mappings = Lists.newArrayList();
    this.offsets = Lists.newArrayList();
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(Object value) throws IOException
  {
    final byte[] bytes = strategy.toBytes(value);

    if (endBuffer.remaining() < bytes.length) {
      endBuffer.flip();
      flattener.add(StupidResourceHolder.create(endBuffer));
      mappings.add(rowNum);
      endBuffer.clear();
      offsetInBlock = 0;
    }

    endBuffer.put(bytes);
    offsetInBlock += bytes.length;
    offsets.add(offsetInBlock);
    rowNum++;
  }

  @Override
  public void close() throws IOException
  {
    endBuffer.flip();
    if (endBuffer.hasRemaining()) {
      flattener.add(StupidResourceHolder.create(endBuffer));
    }
    flattener.close();
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +              // version
           1 +              // compression id
           Ints.BYTES +     // meta header length
           Ints.BYTES * (mappings.size() + 1) +     // mappings
           Short.BYTES * offsets.size() +           // offsets (unsigned short)
           flattener.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    // check
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    // compression meta block
    int length = Ints.BYTES * (mappings.size() + 1) + Short.BYTES * offsets.size();
    channel.write(ByteBuffer.wrap(Ints.toByteArray(length)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(mappings.size())));
    for (int i = 0; i < mappings.size(); i++) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(mappings.get(i))));
    }
    for (int i = 0; i < offsets.size(); i++) {
      channel.write(ByteBuffer.wrap(Shorts.toByteArray(offsets.get(i).shortValue())));
    }
    // data block
    flattener.writeToChannel(channel);
  }
}