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

import io.druid.segment.CompressedVSizedIndexedIntV3Supplier;
import io.druid.segment.IndexIO;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class CompressedVSizeIntsV3Writer extends MultiValueIndexedIntsWriter implements ColumnPartWriter.Compressed
{
  private static final byte VERSION = CompressedVSizedIndexedIntV3Supplier.VERSION;

  private static final List<Integer> EMPTY_LIST = new ArrayList<>();

  public static CompressedVSizeIntsV3Writer create(
      final IOPeon ioPeon,
      final String filenameBase,
      final int maxValue,
      final CompressionStrategy compression
  )
  {
    return new CompressedVSizeIntsV3Writer(
        compression,
        new CompressedIntsIndexedWriter(
            ioPeon,
            String.format("%s.offsets", filenameBase),
            CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER,
            IndexIO.BYTE_ORDER,
            compression
        ),
        new CompressedVSizeIntWriter(
            ioPeon,
            String.format("%s.values", filenameBase),
            maxValue,
            CompressedVSizedIntSupplier.maxIntsInBufferForValue(maxValue),
            IndexIO.BYTE_ORDER,
            compression
        )
    );
  }

  private final CompressionStrategy compression;
  private final CompressedIntsIndexedWriter offsetWriter;
  private final CompressedVSizeIntWriter valueWriter;
  private int offset;

  public CompressedVSizeIntsV3Writer(
      CompressionStrategy compression,
      CompressedIntsIndexedWriter offsetWriter,
      CompressedVSizeIntWriter valueWriter
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
  protected void addValues(List<Integer> vals) throws IOException
  {
    if (vals == null) {
      vals = EMPTY_LIST;
    }
    offsetWriter.add(offset);
    for (Integer val : vals) {
      valueWriter.add(val);
    }
    offset += vals.size();
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
  public long getSerializedSize() throws IOException
  {
    return 1 +   // version
           offsetWriter.getSerializedSize() +
           valueWriter.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    offsetWriter.writeToChannel(channel);
    valueWriter.writeToChannel(channel);
  }
}
