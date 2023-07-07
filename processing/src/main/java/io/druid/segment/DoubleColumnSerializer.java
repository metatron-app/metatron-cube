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

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.BitSlicedBitmaps;
import io.druid.segment.data.BitSlicer;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ColumnPartWriter.DoubleType;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.DoubleHistogram;
import io.druid.segment.data.HistogramBitmaps;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.MetricHistogram;
import io.druid.segment.serde.DoubleGenericColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class DoubleColumnSerializer implements GenericColumnSerializer
{
  public static DoubleColumnSerializer create(
      String filenameBase,
      CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory,
      SecondaryIndexingSpec indexing,
      boolean allowNullForNumbers
  )
  {
    final ByteOrder ordering = IndexIO.BYTE_ORDER;
    if (allowNullForNumbers) {
      return new DoubleColumnSerializer(filenameBase, ordering, compression, serdeFactory, indexing)
      {
        private final MutableBitmap nulls = serdeFactory.getBitmapFactory().makeEmptyMutableBitmap();

        @Override
        public void serialize(int rowNum, Object obj) throws IOException
        {
          if (obj != null) {
            super.serialize(rowNum, obj);
          } else {
            writer.add(0D);
            nulls.add(rowNum);
            if (slicer != null) {
              slicer.add((Double) obj);
            }
          }
        }

        @Override
        public long getSerializedSize()
        {
          long serialized = super.getSerializedSize();
          serialized += Integer.BYTES;
          serialized += serdeFactory.getObjectStrategy().toBytes(nulls).length;
          return serialized;
        }

        @Override
        public long writeToChannel(WritableByteChannel channel) throws IOException
        {
          long written = super.writeToChannel(channel);
          byte[] serialized = serdeFactory.getObjectStrategy().toBytes(nulls);
          written += channel.write(ByteBuffer.wrap(Ints.toByteArray(serialized.length)));
          written += channel.write(ByteBuffer.wrap(serialized));
          return written;
        }

        @Override
        public Map<String, Object> getSerializeStats()
        {
          if (histogram.getMin() > histogram.getMax()) {
            return null;
          }
          return ImmutableMap.<String, Object>of(
              ColumnStats.MIN, histogram.getMin(),
              ColumnStats.MAX, histogram.getMax(),
              ColumnStats.NUM_ZEROS, histogram.getNumZeros(),
              ColumnStats.NUM_NULLS, nulls.size()
          );
        }
      };
    } else {
      return new DoubleColumnSerializer(filenameBase, ordering, compression, serdeFactory, indexing);
    }
  }

  private final String filenameBase;
  private final ByteOrder byteOrder;
  private final CompressionStrategy compression;

  DoubleType writer;

  final BitmapSerdeFactory serdeFactory;
  final MetricHistogram.DoubleType histogram;
  final BitSlicer.DoubleType slicer;

  private DoubleColumnSerializer(
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression,
      BitmapSerdeFactory serdeFactory,
      SecondaryIndexingSpec indexing
  )
  {
    this.filenameBase = filenameBase;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.serdeFactory = serdeFactory;
    if (indexing instanceof HistogramIndexingSpec) {
      this.histogram = new DoubleHistogram(
          serdeFactory.getBitmapFactory(),
          DEFAULT_NUM_SAMPLE,
          DEFAULT_NUM_GROUP,
          DEFAULT_COMPACT_INTERVAL
      );
      this.slicer = null;
    } else if (indexing instanceof BitSlicedBitmapSpec) {
      this.histogram = new DoubleMinMax();
      this.slicer = new BitSlicer.DoubleType(serdeFactory.getBitmapFactory());
    } else {
      this.histogram = new DoubleMinMax();
      this.slicer = null;
    }
  }

  @Override
  public void open(IOPeon ioPeon) throws IOException
  {
    writer = DoubleType.create(ioPeon, String.format("%s.double_column", filenameBase), byteOrder, compression);
    writer.open();
  }

  @Override
  public void serialize(int rowNum, Object obj) throws IOException
  {
    double val = (obj == null) ? 0 : ((Number) obj).doubleValue();
    histogram.offer(val);
    if (slicer != null) {
      slicer.add(val);
    }
    writer.add(val);
  }

  @Override
  public Builder buildDescriptor(Builder builder)
  {
    builder.setValueType(ValueDesc.DOUBLE);
    builder.addSerde(new DoubleGenericColumnPartSerde(IndexIO.BYTE_ORDER, this));
    HistogramBitmaps bitmaps = histogram.snapshot();
    if (bitmaps != null) {
      builder.addSerde(new HistogramBitmaps.SerDe(ValueType.DOUBLE, serdeFactory, bitmaps));
    }
    if (slicer != null) {
      builder.addSerde(new BitSlicedBitmaps.SerDe(ValueType.DOUBLE, serdeFactory, slicer.build()));
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
    if (histogram.getMin() > histogram.getMax()) {
      return null;
    }
    return ImmutableMap.<String, Object>of(
        ColumnStats.MIN, histogram.getMin(),
        ColumnStats.MAX, histogram.getMax(),
        ColumnStats.NUM_ZEROS, histogram.getNumZeros()
    );
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    return writer.writeToChannel(channel);
  }
}
