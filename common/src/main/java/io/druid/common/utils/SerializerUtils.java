/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.common.utils;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;

public class SerializerUtils
{
  private static final Charset UTF8 = Charset.forName("UTF-8");

  private SerializerUtils() {}

  public static <T extends OutputStream> void writeString(T out, String name) throws IOException
  {
    byte[] nameBytes = name.getBytes(UTF8);
    writeInt(out, nameBytes.length);
    out.write(nameBytes);
  }

  public static void writeString(WritableByteChannel out, String name) throws IOException
  {
    byte[] nameBytes = name.getBytes(UTF8);
    writeInt(out, nameBytes.length);
    out.write(ByteBuffer.wrap(nameBytes));
  }

  public static String readString(InputStream in) throws IOException
  {
    final int length = readInt(in);
    byte[] stringBytes = new byte[length];
    ByteStreams.readFully(in, stringBytes);
    return StringUtils.fromUtf8(stringBytes);
  }

  public static String readString(ByteBuffer in)
  {
    final int length = in.getInt();
    return StringUtils.fromUtf8(readBytes(in, length));
  }

  public static byte[] readBytes(ByteBuffer in, int length)
  {
    byte[] bytes = new byte[length];
    in.get(bytes);
    return bytes;
  }

  public static void writeInt(OutputStream out, int intValue) throws IOException
  {
    byte[] outBytes = new byte[4];

    ByteBuffer.wrap(outBytes).putInt(intValue);

    out.write(outBytes);
  }

  public static void writeInt(WritableByteChannel out, int intValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(intValue);
    buffer.flip();
    out.write(buffer);
  }

  public static int readInt(InputStream in) throws IOException
  {
    byte[] intBytes = new byte[4];

    ByteStreams.readFully(in, intBytes);

    return ByteBuffer.wrap(intBytes).getInt();
  }

  public static void writeLong(OutputStream out, long longValue) throws IOException
  {
    byte[] outBytes = new byte[8];

    ByteBuffer.wrap(outBytes).putLong(longValue);

    out.write(outBytes);
  }

  public static void writeLong(WritableByteChannel out, long longValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(longValue);
    buffer.flip();
    out.write(buffer);
  }

  public static long readLong(InputStream in) throws IOException
  {
    byte[] longBytes = new byte[8];

    ByteStreams.readFully(in, longBytes);

    return ByteBuffer.wrap(longBytes).getLong();
  }

  public static int getSerializedStringByteSize(String str)
  {
    return Integer.BYTES + str.getBytes(UTF8).length;
  }
}
