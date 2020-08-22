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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BytesOutputStream extends ByteArrayOutputStream implements ByteArrayDataOutput
{
  private final byte[] scratch = new byte[Long.BYTES + 2];

  public BytesOutputStream() {}

  public BytesOutputStream(int size)
  {
    super(size);
  }

  @Override
  public void write(final byte[] b)
  {
    super.write(b, 0, b.length);
  }

  @Override
  public void write(final byte[] b, final int off, final int len)
  {
    super.write(b, off, len);
  }

  @Override
  public void writeBoolean(final boolean v)
  {
    super.write(v ? 1 : 0);
  }

  @Override
  public void writeByte(final int v)
  {
    super.write(v);
  }

  @Override
  public void writeBytes(final String s)
  {
    int len = s.length();
    for (int i = 0; i < len; i++) {
      super.write((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChar(final int v)
  {
    scratch[0] = (byte)(v >>> 8 & 0xFF);
    scratch[1] = (byte)(v & 0xFF);
    write(scratch, 0, 2);
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
    scratch[0] = (byte) (v >>> 24);
    scratch[1] = (byte) (v >>> 16);
    scratch[2] = (byte) (v >>> 8);
    scratch[3] = (byte) v;
    write(scratch, 0, Integer.BYTES);
  }

  @Override
  public void writeLong(final long v)
  {
    scratch[0] = (byte) (v >>> 56);
    scratch[1] = (byte) (v >>> 48);
    scratch[2] = (byte) (v >>> 40);
    scratch[3] = (byte) (v >>> 32);
    scratch[4] = (byte) (v >>> 24);
    scratch[5] = (byte) (v >>> 16);
    scratch[6] = (byte) (v >>> 8);
    scratch[7] = (byte) v;
    write(scratch, 0, Long.BYTES);
  }

  @Override
  public void writeShort(int v)
  {
    scratch[0] = (byte) (v >>> 8);
    scratch[1] = (byte) v;
    write(scratch, 0, Short.BYTES);
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

  public byte[] toByteArray(int from)
  {
    return Arrays.copyOfRange(buf, from, count);
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
    int i = 0;
    while ((long) (v & -128) != 0L) {
      scratch[i++] = (byte) (v & 127 | 128);
      v >>>= 7;
    }
    scratch[i++] = (byte) (v & 127);
    write(scratch, 0, i);
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
    if (v >= -112 && v <= 127) {
      write((byte) v);
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

    scratch[i++] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      scratch[i++] = ((byte) ((v & mask) >> shiftbits));
    }
    write(scratch, 0, i);
  }

  public BytesRef asRef()
  {
    return new BytesRef(buf, count);
  }
}
