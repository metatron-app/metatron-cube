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

package io.druid.segment;

import io.druid.java.util.common.IAE;
import io.druid.segment.data.CompressedIntReader;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVintsReader;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IntsValues;
import io.druid.segment.data.WritableSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The format is mostly the same with CompressedVSizeIndexedSupplier(which has version 0x2, so we call it V2),
 * the only difference is V3's offsets is not VSize encoded, it's just compressed.
 * The reason we provide this is we can streams the data out in the binary format with CompressedVSizeIndexedV3Writer.
 * If we want to streams VSizeInts, we must know the max value in the value sets. It's easy to know the max id of
 * values(like dimension cardinality while encoding dimension), but difficult to known the max id of offsets.
 */
public class CompressedVIntsSupplierV3 implements WritableSupplier<IntsValues>
{
  public static final byte VERSION = 0x3;

  public static CompressedVIntsSupplierV3 from(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      CompressedIntReader offsets = CompressedIntReader.from(buffer, order);
      CompressedVintsReader values = CompressedVintsReader.from(buffer, order);
      return new CompressedVIntsSupplierV3(offsets, values);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final CompressedIntReader offsets;
  private final CompressedVintsReader values;

  private CompressedVIntsSupplierV3(CompressedIntReader offsets, CompressedVintsReader values)
  {
    this.offsets = offsets;
    this.values = values;
  }

  // for test
  public static CompressedVIntsSupplierV3 fromIterable(
      Iterable<IndexedInts> objectsIterable,
      int offsetChunkFactor,
      int maxValue,
      final ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Iterator<IndexedInts> objects = objectsIterable.iterator();
    List<Integer> offsetList = new ArrayList<>();
    List<Integer> values = new ArrayList<>();

    int offset = 0;
    while (objects.hasNext()) {
      IndexedInts next = objects.next();
      offsetList.add(offset);
      for (int i = 0; i < next.size(); i++) {
        values.add(next.get(i));
      }
      offset += next.size();
    }
    offsetList.add(offset);
    CompressedIntReader headerSupplier = CompressedIntReader.fromList(
        offsetList,
        offsetChunkFactor,
        byteOrder,
        compression
    );
    CompressedVintsReader valuesSupplier = CompressedVintsReader.fromList(
        values,
        maxValue,
        CompressedVintsReader.maxIntsInBufferForValue(maxValue),
        byteOrder,
        compression
    );
    return new CompressedVIntsSupplierV3(headerSupplier, valuesSupplier);
  }

  @Override
  public int numRows()
  {
    return offsets.numRows();
  }

  @Override
  public long getSerializedSize()
  {
    return 1 + offsets.getSerializedSize() + values.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    written += offsets.writeToChannel(channel);
    written += values.writeToChannel(channel);
    return written;
  }

  @Override
  public Class<? extends IntsValues> provides()
  {
    return CompressedVintsSupplier.CompressedIntsValues.class;
  }

  @Override
  public IntsValues get()
  {
    return new CompressedVintsSupplier.CompressedIntsValues(offsets.get(), values.get());
  }
}
