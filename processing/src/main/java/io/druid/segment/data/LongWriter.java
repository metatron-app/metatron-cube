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

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class LongWriter implements ColumnPartWriter.LongType
{
  private final IOPeon ioPeon;
  private final String valueFileName;

  private int count;
  private DataOutputStream valuesOut;

  public LongWriter(IOPeon ioPeon, String filenameBase)
  {
    this.ioPeon = ioPeon;
    this.valueFileName = String.format("%s.values", filenameBase);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = new DataOutputStream(ioPeon.makeOutputStream(valueFileName));
  }

  @Override
  public void add(Long obj) throws IOException
  {
    throw new ISE("not expects nulls");
  }

  @Override
  public void add(long obj) throws IOException
  {
    valuesOut.writeLong(obj);
    count++;
  }

  @Override
  public void close() throws IOException
  {
    valuesOut.close();
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return 1 +              // version
           Integer.BYTES +  // elements num
           Integer.BYTES +  // sizePer
           1 +              // compression id
           Long.BYTES * count;
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{ColumnPartSerde.WITH_COMPRESSION_ID}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(count)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(Long.BYTES)));
    channel.write(ByteBuffer.wrap(new byte[]{CompressionStrategy.NONE.getId()}));
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(valueFileName))) {
      if (channel instanceof SmooshedWriter && input instanceof FileChannel) {
        ((SmooshedWriter) channel).transferFrom((FileChannel) input);
      } else {
        ByteStreams.copy(input, channel);
      }
    }
  }
}
