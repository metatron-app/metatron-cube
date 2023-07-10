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
import com.google.common.primitives.Shorts;
import io.druid.collections.IntList;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetricSerde.CompressionSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class CompressedComplexColumnSerializer implements ColumnPartWriter
{
  private final ColumnPartWriter<ResourceHolder<ByteBuffer>> flattener;
  private final CompressionStrategy compression;
  private final ObjectStrategy strategy;

  private int rowNum;
  private int offsetInBlock;

  private final ByteBuffer endBuffer;
  private final IntList mappings;   // thresholds for blocks
  private final IntList offsets;    // offsets in each block

  public CompressedComplexColumnSerializer(
      ColumnPartWriter<ResourceHolder<ByteBuffer>> flattener,
      CompressionStrategy compression,
      CompressionSupport serde
  )
  {
    this.flattener = flattener;
    this.compression = compression;
    this.strategy = serde.getObjectStrategy();
    this.endBuffer = ByteBuffer.allocate(CompressedPools.BUFFER_SIZE);  // offset : unsigned short
    this.mappings = new IntList();
    this.offsets = new IntList();
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
    final boolean deficit = endBuffer.remaining() - CompressedPools.RESERVE < bytes.length;
    if (deficit && offsetInBlock == 0) {
      flattener.add(StupidResourceHolder.create(ByteBuffer.wrap(bytes)));
      offsets.add(CompressedPools.BUFFER_EXCEEDED);   // marker
      mappings.add(++rowNum);
      return;
    }
    if (deficit) {
      endBuffer.flip();
      flattener.add(StupidResourceHolder.create(endBuffer));
      mappings.add(rowNum);
      endBuffer.clear();
      offsetInBlock = 0;
    }
    rowNum++;
    endBuffer.put(bytes);
    offsetInBlock += bytes.length;
    offsets.add(offsetInBlock);
  }

  @Override
  public void close() throws IOException
  {
    endBuffer.flip();
    if (endBuffer.hasRemaining()) {
      flattener.add(StupidResourceHolder.create(endBuffer));
      mappings.add(rowNum);
    }
    flattener.close();
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +              // version
           1 +              // compression id
           Integer.BYTES +  // meta header length
           Integer.BYTES + Integer.BYTES * mappings.size() +    // length + mappings
           Short.BYTES * offsets.size() +                       // offsets (unsigned short)
           flattener.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    // check
    long written = channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    written += channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    // compression meta block
    int length = Integer.BYTES + Integer.BYTES * mappings.size() + Short.BYTES * offsets.size();
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(length)));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(mappings.size())));
    for (int i = 0; i < mappings.size(); i++) {
      written += channel.write(ByteBuffer.wrap(Ints.toByteArray(mappings.get(i))));
    }
    for (int i = 0; i < offsets.size(); i++) {
      written += channel.write(ByteBuffer.wrap(Shorts.toByteArray((short) offsets.get(i))));
    }
    // data block
    written += flattener.writeToChannel(channel);
    return written;
  }
}
