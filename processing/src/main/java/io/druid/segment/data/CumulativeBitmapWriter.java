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
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.collections.IntList;
import io.druid.data.VLongUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.DimFilters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;

public class CumulativeBitmapWriter implements ColumnPartWriter<ImmutableBitmap>
{
  private static final Logger LOG = new Logger(CumulativeBitmapWriter.class);
  private static final int BITMAP_MERGE_INTERVAL = 1024;

  private final int threshold;
  private final IntList thresholds;
  private final BitmapFactory bitmapFactory;
  private final ColumnPartWriter<ImmutableBitmap> bitmapWriter;
  private final GenericIndexedWriter<ImmutableBitmap> cumulativeWriter;

  private int numWritten;
  private final List<ImmutableBitmap> bitmaps = Lists.newArrayList();

  public CumulativeBitmapWriter(
      IOPeon ioPeon,
      String filenameBase,
      ColumnPartWriter<ImmutableBitmap> bitmapWriter,
      BitmapSerdeFactory serdeFactory,
      int threshold
  )
  {
    this.thresholds = new IntList();
    this.bitmapWriter = bitmapWriter;
    this.bitmapFactory = serdeFactory.getBitmapFactory();
    this.cumulativeWriter = GenericIndexedWriter.v2(ioPeon, filenameBase, serdeFactory.getObjectStrategy());
    this.threshold = threshold;
  }

  @Override
  public void open() throws IOException
  {
    bitmapWriter.open();
    cumulativeWriter.open();
  }

  @Override
  public void add(ImmutableBitmap obj) throws IOException
  {
    bitmapWriter.add(obj);
    bitmaps.add(obj);
    if (++numWritten % threshold == 0) {
      cumulativeWriter.add(next());
      thresholds.add(numWritten);
    } else if (bitmaps.size() == BITMAP_MERGE_INTERVAL) {
      next();
    }
  }

  private ImmutableBitmap next()
  {
    final ImmutableBitmap union = DimFilters.union(bitmapFactory, bitmaps);
    bitmaps.clear();
    bitmaps.add(union);
    return union;
  }

  @Override
  public void close() throws IOException
  {
    bitmaps.clear();  // skip last one
    bitmapWriter.close();
    cumulativeWriter.close();
  }

  @Override
  public long getSerializedSize()
  {
    int length = VLongUtils.getVIntSize(thresholds.size());
    for (int i = 0; i < thresholds.size(); i++) {
      length += VLongUtils.getVIntSize(thresholds.get(i));
    }
    return bitmapWriter.getSerializedSize() + length + cumulativeWriter.getSerializedSize();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = bitmapWriter.writeToChannel(channel);
    BytesOutputStream output = new BytesOutputStream();
    output.writeVarInt(thresholds.size());
    for (int i = 0; i < thresholds.size(); i++) {
      output.writeVarInt(thresholds.get(i));
    }
    written += channel.write(ByteBuffer.wrap(output.toByteArray()));
    written += cumulativeWriter.writeToChannel(channel);
    return written;
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return bitmapWriter.getSerializeStats();
  }
}
