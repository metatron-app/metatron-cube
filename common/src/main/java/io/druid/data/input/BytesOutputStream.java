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

import com.google.common.io.ByteArrayDataOutput;
import io.druid.common.guava.BytesRef;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

public final class BytesOutputStream extends OutputStream implements ByteArrayDataOutput
{
  private static final int MAX_VARINT_SIZE = Integer.BYTES + Byte.BYTES;
  private static final int MAX_VARLONG_SIZE = Long.BYTES + Short.BYTES;

  private byte buf[];
  private int count;

  public BytesOutputStream()
  {
    this(32);
  }

  public BytesOutputStream(int size)
  {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: " + size);
    }
    buf = new byte[size];
  }

  public void ensureCapacity(int minCapacity)
  {
    if (minCapacity > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(minCapacity, buf.length << 1));
    }
  }

  public int size()
  {
    return count;
  }

  @Override
  public byte[] toByteArray()
  {
    return Arrays.copyOf(buf, count);
  }

  @Override
  public void write(final int b)
  {
    final int x = count;
    ensureCapacity(x + 1);
    buf[x] = (byte) b;
    count += Byte.BYTES;
  }

  @Override
  public void write(final byte[] b)
  {
    write(b, 0, b.length);
  }

  @Override
  public void write(final byte[] b, final int off, final int len)
  {
    ensureCapacity(count + len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public void writeBoolean(final boolean v)
  {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(final int v)
  {
    write(v);
  }

  @Override
  public void writeBytes(final String s)
  {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      write((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChar(final int v)
  {
    final int x = count;
    ensureCapacity(x + Character.BYTES);
    final byte[] b = buf;
    b[x] = (byte) (v >>> 8);
    b[x + 1] = (byte) v;
    count += Character.BYTES;
  }

  @Override
  public void writeChars(final String s)
  {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      writeChar(s.charAt(i));
    }
  }

  @Override
  public void writeDouble(final double v)
  {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeFloat(final float v)
  {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeInt(final int v)
  {
    final int x = count;
    ensureCapacity(x + Integer.BYTES);
    final byte[] b = buf;
    b[x] = (byte) (v >>> 24);
    b[x + 1] = (byte) (v >>> 16);
    b[x + 2] = (byte) (v >>> 8);
    b[x + 3] = (byte) v;
    count += Integer.BYTES;
  }

  @Override
  public void writeLong(final long v)
  {
    final int x = count;
    ensureCapacity(x + Long.BYTES);
    final byte[] b = buf;
    b[x] = (byte) (v >>> 56);
    b[x + 1] = (byte) (v >>> 48);
    b[x + 2] = (byte) (v >>> 40);
    b[x + 3] = (byte) (v >>> 32);
    b[x + 4] = (byte) (v >>> 24);
    b[x + 5] = (byte) (v >>> 16);
    b[x + 6] = (byte) (v >>> 8);
    b[x + 7] = (byte) v;
    count += Long.BYTES;
  }

  @Override
  public void writeShort(int v)
  {
    final int x = count;
    ensureCapacity(x + Short.BYTES);
    final byte[] b = buf;
    b[x] = (byte) (v >>> 8);
    b[x + 1] = (byte) v;
    count += Short.BYTES;
  }

  @Override
  public void writeUTF(String s)
  {
    try {
      new DataOutputStream(this).writeUTF(s);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  public void position(int position)
  {
    count = position;
  }

  public void clear()
  {
    count = 0;
  }

  public byte[] unwrap()
  {
    return buf;
  }

  public void writeVarSizeBytes(byte[] value)
  {
    writeUnsignedVarInt(value.length);
    write(value);
  }

  // from org.apache.parquet.bytes.BytesUtils
  public void writeUnsignedVarInt(int v)
  {
    ensureCapacity(count + MAX_VARINT_SIZE);
    int i = 0;
    while ((long) (v & -128) != 0L) {
      buf[count++] = (byte) (v & 127 | 128);
      v >>>= 7;
    }
    buf[count++] = (byte) (v & 127);
  }

  public void write(BytesRef ref)
  {
    write(ref.bytes, 0, ref.length);
  }

  public void writeVarSizeBytes(BytesRef ref)
  {
    writeUnsignedVarInt(ref.length);
    write(ref.bytes, 0, ref.length);
  }

  public void writeVarInt(int v)
  {
    writeVarLong(v);
  }

  // from org.apache.hadoop.io.WritableUtils
  public void writeVarLong(long v)
  {
    ensureCapacity(count + MAX_VARLONG_SIZE);

    if (v >= -112 && v <= 127) {
      buf[count++] = (byte) v;
      return;
    }

    int i = 0;
    int len = -112;
    if (v < 0) {
      v ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = v;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    buf[count++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      final int shiftbits = (idx - 1) * 8;
      final long mask = 0xFFL << shiftbits;
      buf[count++] = ((byte) ((v & mask) >> shiftbits));
    }
  }

  public BytesRef asRef()
  {
    return new BytesRef(buf, count);
  }
}
