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
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.BitSlicedBitmaps;
import io.druid.segment.data.BitSlicer;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedLongsSupplierSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.LongHistogram;
import io.druid.segment.data.HistogramBitmaps;
import io.druid.segment.data.MetricHistogram;
import io.druid.segment.serde.LongGenericColumnPartSerde;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class LongColumnSerializer implements GenericColumnSerializer
{
  public static LongColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      CompressedObjectStrategy.CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory,
      SecondaryIndexingSpec indexing
  )
  {
    return new LongColumnSerializer(ioPeon, filenameBase, IndexIO.BYTE_ORDER, compression, serdeFactory, indexing);
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private CompressedLongsSupplierSerializer writer;

  private final BitmapSerdeFactory serdeFactory;
  private final MetricHistogram.LongType histogram;
  private final BitSlicer.LongType slicer;

  private LongColumnSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory,
      SecondaryIndexingSpec indexing
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.serdeFactory = serdeFactory;
    if (indexing instanceof HistogramIndexingSpec) {
      this.histogram = new LongHistogram(
          serdeFactory.getBitmapFactory(),
          DEFAULT_NUM_SAMPLE,
          DEFAULT_NUM_GROUP,
          DEFAULT_COMPACT_INTERVAL
      );
      this.slicer = null;
    } else if (indexing instanceof BitSlicedBitmapSpec) {
      this.histogram = new LongMinMax();
      this.slicer = new BitSlicer.LongType(serdeFactory.getBitmapFactory());
    } else {
      this.histogram = new LongMinMax();
      this.slicer = null;
    }
  }

  @Override
  public void open() throws IOException
  {
    writer = CompressedLongsSupplierSerializer.create(
        ioPeon,
        String.format("%s.long_column", filenameBase),
        byteOrder,
        compression
    );
    writer.open();
  }

  @Override
  public void serialize(Object obj) throws IOException
  {
    long val = (obj == null) ? 0 : ((Number) obj).longValue();
    histogram.offer(val);
    if (slicer != null) {
      slicer.add(val);
    }
    writer.add(val);
  }

  @Override
  public Builder buildDescriptor(ValueDesc desc, Builder builder)
  {
    builder.setValueType(ValueDesc.LONG);
    builder.addSerde(
        LongGenericColumnPartSerde.serializerBuilder()
                                  .withByteOrder(IndexIO.BYTE_ORDER)
                                  .withDelegate(this)
                                  .build()
    );
    HistogramBitmaps bitmaps = histogram.snapshot();
    if (bitmaps != null) {
      builder.addSerde(new HistogramBitmaps.SerDe(ValueType.LONG, serdeFactory, bitmaps));
    }
    if (slicer != null) {
      builder.addSerde(new BitSlicedBitmaps.SerDe(ValueType.LONG, serdeFactory, slicer.build()));
    }
    return builder;
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public long getSerializedSize()
  {
    return writer.getSerializedSize();
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    if (writer.size() == 0) {
      return null;
    }
    return ImmutableMap.<String, Object>of(
        "min", histogram.getMin(),
        "max", histogram.getMax(),
        "numZeros", histogram.getNumZeros()
    );
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
  }
}
