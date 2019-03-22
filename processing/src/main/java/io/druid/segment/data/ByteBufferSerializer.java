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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.metamx.common.IAE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ByteBufferSerializer<T>
{
  public static <T> T read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    int size = buffer.getInt();
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return strategy.fromByteBuffer(bufferToUse, size);
  }

  public static ByteBuffer prepareForRead(ByteBuffer buffer, byte version)
  {
    final byte versionFromBuffer = buffer.get();
    if (versionFromBuffer != version) {
      throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
    return prepareForRead(buffer);
  }

  // make buffer for read and move forward position of original buffer
  public static ByteBuffer prepareForRead(ByteBuffer buffer)
  {
    return prepareForRead(buffer, buffer.getInt());
  }

  public static ByteBuffer prepareForRead(ByteBuffer buffer, int size)
  {
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return bufferToUse;
  }

  public static <T> void writeToChannel(T obj, ObjectStrategy<T> strategy, WritableByteChannel channel)
      throws IOException
  {
    byte[] toWrite = strategy.toBytes(obj);
    channel.write(ByteBuffer.allocate(Ints.BYTES).putInt(0, toWrite.length));
    channel.write(ByteBuffer.wrap(toWrite));
  }
}
