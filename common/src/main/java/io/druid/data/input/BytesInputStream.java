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

package io.druid.data.input;

import com.google.common.io.ByteArrayDataInput;
import io.druid.data.UTF8Bytes;
import io.druid.data.VLongUtils;
import io.druid.java.util.common.StringUtils;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public final class BytesInputStream extends InputStream implements ByteArrayDataInput
{
  private final byte[] bytes;
  private int pos;

  public BytesInputStream(byte[] bytes)
  {
    this.bytes = bytes;
  }

  @Override
  public int read()
  {
    return pos < bytes.length ? bytes[pos++] & 0xff : -1;
  }

  @Override
  public int read(byte[] b, int off, int len)
  {
    if (len == 0) {
      return 0;
    } else if (pos >= bytes.length) {
      return -1;
    }
    final int read = Math.min(len, bytes.length - pos);
    System.arraycopy(bytes, pos, b, off, read);
    pos += read;
    return read;
  }

  @Override
  public long skip(long n)
  {
    final long skip = Math.min(n, bytes.length - pos);
    pos += skip;
    return skip;
  }

  @Override
  public int available()
  {
    return bytes.length - pos;
  }

  @Override
  public void readFully(byte[] b)
  {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len)
  {
    System.arraycopy(bytes, pos, b, off, len);
    pos += len;
  }

  @Override
  public int skipBytes(int n)
  {
    final int skip = Math.min(n, bytes.length - pos);
    pos += skip;
    return skip;
  }

  @Override
  public boolean readBoolean()
  {
    return bytes[pos++] != 0;
  }

  @Override
  public byte readByte()
  {
    return (byte) (bytes[pos++] & 0xff);
  }

  @Override
  public int readUnsignedByte()
  {
    return bytes[pos++] & 0xff;
  }

  @Override
  public short readShort()
  {
    final int x = pos;
    final byte[] b = bytes;
    final int ch1 = b[x] & 0xff;
    final int ch2 = b[x + 1] & 0xff;
    pos += Short.BYTES;
    return (short) ((ch1 << 8) + ch2);
  }

  @Override
  public int readUnsignedShort()
  {
    final int x = pos;
    final byte[] b = bytes;
    final int ch1 = b[x] & 0xff;
    final int ch2 = b[x + 1] & 0xff;
    pos += Short.BYTES;
    return (ch1 << 8) + ch2;
  }

  @Override
  public char readChar()
  {
    final int x = pos;
    final byte[] b = bytes;
    final int ch1 = b[x] & 0xff;
    final int ch2 = b[x + 1] & 0xff;
    pos += Character.BYTES;
    return (char) ((ch1 << 8) + ch2);
  }

  @Override
  public int readInt()
  {
    final int x = pos;
    final byte[] b = bytes;
    final int ch1 = b[x] & 0xff;
    final int ch2 = b[x + 1] & 0xff;
    final int ch3 = b[x + 2] & 0xff;
    final int ch4 = b[x + 3] & 0xff;
    pos += Integer.BYTES;
    return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
  }

  @Override
  public long readLong()
  {
    final int x = pos;
    final byte[] b = bytes;
    final long ch1 = b[x] & 0xff;
    final long ch2 = b[x + 1] & 0xff;
    final long ch3 = b[x + 2] & 0xff;
    final long ch4 = b[x + 3] & 0xff;
    final long ch5 = b[x + 4] & 0xff;
    final int ch6 = b[x + 5] & 0xff;
    final int ch7 = b[x + 6] & 0xff;
    final int ch8 = b[x + 7] & 0xff;
    pos += Long.BYTES;
    return (ch1 << 56) + (ch2 << 48) + (ch3 << 40) + (ch4 << 32) + (ch5 << 24) + (ch6 << 16) + (ch7 << 8) + ch8;
  }

  @Override
  public float readFloat()
  {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble()
  {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine()
  {
    throw new UnsupportedOperationException("readLine");
  }

  @Override
  public String readUTF()
  {
    throw new UnsupportedOperationException("readUTF");
  }

  public String readVarSizeUTF()
  {
    final int length = readUnsignedVarInt();
    final String value = StringUtils.toUTF8String(bytes, pos, length);
    pos += length;
    return value;
  }

  public List<String> readVarSizeUTFs(int valueLength)
  {
    final String[] values = new String[valueLength];
    for (int i = 0; i < valueLength; i++) {
      final int length = readUnsignedVarInt();
      values[i] = StringUtils.fromUtf8(bytes, pos, length);
      pos += length;
    }
    return Arrays.asList(values);
  }

  public byte[] readVarSizeBytes()
  {
    final byte[] bytes = new byte[readUnsignedVarInt()];
    readFully(bytes);
    return bytes;
  }

  public UTF8Bytes viewVarSizeUTF()
  {
    final int length = readUnsignedVarInt();
    UTF8Bytes view = UTF8Bytes.of(bytes, pos, length);
    pos += length;
    return view;
  }

  // copied from org.apache.parquet.bytes.BytesUtils
  public int readUnsignedVarInt()
  {
    int value = 0;
    int i;
    int b;
    for (i = 0; ((b = readUnsignedByte()) & 0x80) != 0; i += 7) {
      value |= (b & 0x7f) << i;
    }
    return value | b << i;
  }

  public int readVarInt()
  {
    return VLongUtils.readVInt(this);
  }

  public long readVarLong()
  {
    return VLongUtils.readVLong(this);
  }
}
