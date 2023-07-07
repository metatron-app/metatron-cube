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
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVintsReader;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
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
 * Format -
 * byte 1 - version
 * offsets - indexed integers of length num of rows + 1 representing offsets of starting index of first element of each row in values index and last element equal to length of values column,
 * the last element in the offsets represents the total length of values column.
 * values - indexed integer representing values in each row
 */

public class CompressedVintsSupplier implements WritableSupplier<IntsValues>
{
  private static final byte VERSION = 0x2;

  public static CompressedVintsSupplier from(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      CompressedVintsReader offsets = CompressedVintsReader.from(buffer, order);
      CompressedVintsReader values = CompressedVintsReader.from(buffer, order);
      return new CompressedVintsSupplier(offsets, values);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final CompressedVintsReader offsets;
  private final CompressedVintsReader values;

  private CompressedVintsSupplier(CompressedVintsReader offsets, CompressedVintsReader values)
  {
    this.offsets = offsets;
    this.values = values;
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
    return CompressedIntsValues.class;
  }

  @Override
  public IntsValues get()
  {
    return new CompressedIntsValues(offsets.get(), values.get());
  }

  public static class CompressedIntsValues implements IntsValues
  {
    private final IndexedInts offsets;
    private final IndexedInts values;

    CompressedIntsValues(IndexedInts offsets, IndexedInts values)
    {
      this.offsets = offsets;
      this.values = values;
    }

    @Override
    public void close() throws IOException
    {
      offsets.close();
      values.close();
    }

    @Override
    public int size()
    {
      return offsets.size() - 1;
    }

    @Override
    public IndexedInts get(int index)
    {
      final int offset = offsets.get(index);
      final int size = offsets.get(index + 1) - offset;

      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return size;
        }

        @Override
        public int get(int index)
        {
          if (index >= size) {
            throw new IllegalArgumentException(String.format("Index[%s] >= size[%s]", index, size));
          }
          return values.get(index + offset);
        }
      };
    }

    @Override
    public Iterator<IndexedInts> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
  }

  public static CompressedVintsSupplier fromIterable(
      Iterable<IndexedInts> objectsIterable,
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
    int offsetMax = offset;
    CompressedVintsReader headerSupplier = CompressedVintsReader.fromList(
        offsetList,
        offsetMax,
        CompressedVintsReader.maxIntsInBufferForValue(offsetMax),
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
    return new CompressedVintsSupplier(headerSupplier, valuesSupplier);
  }
}
