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

package io.druid.segment.lucene;

import com.google.common.primitives.Ints;
import io.druid.data.input.BytesOutputStream;
import io.druid.segment.serde.ColumnPartSerde;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class SimpleRelaySerializer implements ColumnPartSerde.Serializer
{
  public static ColumnPartSerde.Serializer forFST(FST fst) throws IOException
  {
    BytesOutputStream out = new BytesOutputStream();
    out.writeInt(0);
    fst.save(new OutputStreamDataOutput(out));
    byte[] contents = out.toByteArray();
    System.arraycopy(Ints.toByteArray(contents.length - Integer.BYTES), 0, contents, 0, Integer.BYTES);
    return new SimpleRelaySerializer(contents);
  }

  private final byte[] contents;

  private SimpleRelaySerializer(byte[] contents) {this.contents = contents;}

  @Override
  public long getSerializedSize() throws IOException
  {
    return contents.length;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(contents));
  }
}
