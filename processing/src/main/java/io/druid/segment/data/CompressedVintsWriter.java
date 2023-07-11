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

/**
 * Streams array of integers out in the binary format described by CompressedVSizeIndexedV3Supplier
 */
package io.druid.segment.data;

import io.druid.segment.CompressedVIntsSupplierV3;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;

public class CompressedVintsWriter implements IntsWriter, ColumnPartWriter.Compressed
{
  private static final byte VERSION = CompressedVIntsSupplierV3.VERSION;

  private final CompressionStrategy compression;
  private final IntWriter offsetWriter;
  private final IntWriter valueWriter;
  private int offset;

  public CompressedVintsWriter(
      CompressionStrategy compression,
      IntWriter offsetWriter,
      IntWriter valueWriter
  )
  {
    this.compression = compression;
    this.offsetWriter = offsetWriter;
    this.valueWriter = valueWriter;
    this.offset = 0;
  }

  @Override
  public CompressionStrategy appliedCompression()
  {
    return compression;
  }

  @Override
  public void open() throws IOException
  {
    offsetWriter.open();
    valueWriter.open();
  }

  @Override
  public void add(List<Integer> vals) throws IOException
  {
    if (vals == null) {
      vals = Collections.emptyList();
    }
    offsetWriter.add(offset);
    for (Integer val : vals) {
      valueWriter.add(val);
    }
    offset += vals.size();
  }

  @Override
  public void add(int[] vals) throws IOException
  {
    if (vals == null) {
      vals = EMPTY_ROW;
    }
    offsetWriter.add(offset);
    for (int i = 0; i < vals.length; i++) {
      valueWriter.add(vals[i]);
    }
    offset += vals.length;
  }

  @Override
  public void close() throws IOException
  {
    try {
      offsetWriter.add(offset);
    }
    finally {
      offsetWriter.close();
      valueWriter.close();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +   // version
           offsetWriter.getSerializedSize() +
           valueWriter.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    written += offsetWriter.writeToChannel(channel);
    written += valueWriter.writeToChannel(channel);
    return written;
  }
}
