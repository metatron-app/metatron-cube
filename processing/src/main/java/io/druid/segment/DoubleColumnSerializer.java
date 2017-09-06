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

package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.druid.data.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedDoublesSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.DoubleHistogram;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.MetricBitmaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class DoubleColumnSerializer implements GenericColumnSerializer
{
  public static DoubleColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      CompressedObjectStrategy.CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory
  )
  {
    return new DoubleColumnSerializer(ioPeon, filenameBase, IndexIO.BYTE_ORDER, compression, serdeFactory);
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private CompressedDoublesSupplierSerializer writer;

  private final BitmapSerdeFactory serdeFactory;
  private final DoubleHistogram histogram;
  private ByteBuffer bitmapPayload;

  private DoubleColumnSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.serdeFactory = serdeFactory;
    this.histogram = new DoubleHistogram(
        serdeFactory.getBitmapFactory(),
        DEFAULT_NUM_SAMPLE,
        DEFAULT_NUM_GROUP,
        DEFAULT_COMPACT_INTERVAL
    );
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressedDoublesSupplierSerializer.create(
        ioPeon,
        String.format("%s.double_column", filenameBase),
        byteOrder,
        compression
    );
    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    double val = (obj == null) ? 0 : ((Number) obj).doubleValue();
    histogram.offer(val);
    writer.add(val);
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (writer.size() == 0) {
      return null;
    }
    return ImmutableMap.<String, Object>of(
        "min", histogram.getMin(),
        "max", histogram.getMax()
    );
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public long getSerializedSize()
  {
    long size = writer.getSerializedSize();
    MetricBitmaps bitmaps = histogram.snapshot();
    if (bitmaps != null) {
      byte[] payload = MetricBitmaps.getStrategy(serdeFactory, ValueType.DOUBLE).toBytes(bitmaps);
      bitmapPayload = (ByteBuffer) ByteBuffer.allocate(Ints.BYTES + payload.length)
                                             .putInt(payload.length)
                                             .put(payload)
                                             .flip();
      size += bitmapPayload.remaining();
    }
    return size;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
    if (bitmapPayload != null) {
      channel.write(bitmapPayload);
    }
  }
}
