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

package io.druid.data;

import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * from org.apache.hadoop.io.WritableUtils. modified for using ByteBuffer
 */
public class VLongUtils
{
  /**
   * Serializes an integer to a binary stream with zero-compressed encoding.
   * For -120 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * integer is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -121 and -124, the following integer
   * is positive, with number of bytes that follow are -(v+120).
   * If the first byte value v is between -125 and -128, the following integer
   * is negative, with number of bytes that follow are -(v+124). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param buf ByteBuffer output buffer
   * @param i Integer to be serialized
   * @throws java.io.IOException
   */
  public static void writeVInt(ByteBuffer buf, int i)
  {
    writeVLong(buf, i);
  }

  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param buf ByteBuffer output buffer
   * @param i Long to be serialized
   */
  public static void writeVLong(ByteBuffer buf, long i)
  {
    if (i >= -112 && i <= 127) {
      buf.put((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    buf.put((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      buf.put((byte) ((i & mask) >> shiftbits));
    }
  }

  public static void writeVInt(BytesOutputStream stream, int i)
  {
    writeVLong(stream, i);
  }

  public static void writeVLong(BytesOutputStream stream, long i)
  {
    if (i >= -112 && i <= 127) {
      stream.writeByte((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    stream.writeByte((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.writeByte((byte) ((i & mask) >> shiftbits));
    }
  }

  public static void writeVInt(OutputStream stream, long i) throws IOException
  {
    writeVLong(stream, i);
  }

  public static void writeVLong(OutputStream stream, long i) throws IOException
  {
    if (i >= -112L && i <= 127L) {
      stream.write((byte) ((int) i));
      return;
    }

    int len = -112;
    if (i < 0L) {
      i = ~i;
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    stream.write((byte) len);

    len = len < -120 ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; --idx) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.write((byte) ((int) ((i & mask) >> shiftbits)));
    }
  }

  /**
   * Reads a zero-compressed encoded long from input buffer and returns it.
   *
   * @param buf ByteBuffer input buffer
   * @return deserialized long from buffer.
   */
  public static long readVLong(ByteBuffer buf)
  {
    byte firstByte = buf.get();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buf.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return isNegativeVInt(firstByte) ? ~i : i;
  }

  public static int sizeOfUnsignedVarInt(int v)
  {
    if (v < 0x80) {
      return 1;
    } else if (v < 0x40_00) {
      return 2;
    } else if (v < 0x20_0000) {
      return 3;
    } else if (v < 0x10_000000) {
      return 4;
    } else {
      return 5;
    }
  }

  public static void writeUnsignedVarInt(ByteBuffer buffer, int v)
  {
    int i = 0;
    while ((long) (v & -128) != 0L) {
      buffer.put((byte) (v & 127 | 128));
      v >>>= 7;
    }
    buffer.put((byte) (v & 127));
  }

  public static void writeUnsignedVarInt(OutputStream out, int v) throws IOException
  {
    int i = 0;
    while ((long) (v & -128) != 0L) {
      out.write((byte) (v & 127 | 128));
      v >>>= 7;
    }
    out.write((byte) (v & 127));
  }

  private static final int N_MASK = 0x80;
  private static final int V_MASK = 0x7f;

  public static int readUnsignedVarInt(ByteBuffer buffer)
  {
    int b1 = buffer.get() & 0xff;
    if ((b1 & N_MASK) == 0) {
      return b1;
    }
    int b2 = buffer.get() & 0xff;
    if ((b2 & N_MASK) == 0) {
      return b2 << 7 | b1 & V_MASK;
    }
    int value = (b2 & V_MASK) << 7 | b1 & V_MASK;
    int i;
    int b;
    for (i = 14; ((b = buffer.get() & 0xff) & N_MASK) != 0; i += 7) {
      value |= (b & V_MASK) << i;
    }
    return value | b << i;
  }

  public static int __readUnsignedVarInt(ByteBuffer buffer, int offset)
  {
    final int b1 = buffer.get(offset++) & 0xff;
    if ((b1 & N_MASK) == 0) {
      return b1;
    }
    final int b2 = buffer.get(offset++) & 0xff;
    if ((b2 & N_MASK) == 0) {
      return b2 << 7 | b1 & V_MASK;
    }
    final int b3 = buffer.get(offset++) & 0xff;
    if ((b3 & N_MASK) == 0) {
      return b3 << 14 | (b2 & V_MASK) << 7 | b1 & V_MASK;
    }
    final int b4 = buffer.get(offset++) & 0xff;
    if ((b4 & N_MASK) == 0) {
      return b4 << 21 | (b3 & V_MASK) << 14 | (b2 & V_MASK) << 7 | b1 & V_MASK;
    }
    int b5 = buffer.get(offset) & 0xff;
    return b5 << 28 | (b4 & V_MASK) << 21 | (b3 & V_MASK) << 14 | (b2 & V_MASK) << 7 | b1 & V_MASK;
  }

  public static int readUnsignedVarInt(ByteBuffer buffer, int offset)
  {
    return _readUnsignedVarInt(buffer, offset, 0);
  }

  private static int _readUnsignedVarInt(ByteBuffer buffer, int offset, int shift)
  {
    final int b = buffer.get(offset) & 0xff;
    if ((b & N_MASK) != 0) {
      return _readUnsignedVarInt(buffer, offset + 1, shift + 7) | (b & V_MASK) << shift;
    } else {
      return b << shift;
    }
  }

  public static int readInt(ByteBuffer buf, int offset, ByteOrder order)
  {
    return buf.order() == order ? buf.getInt(offset) : Integer.reverseBytes(buf.getInt(offset));
  }

  /**
   * Reads a zero-compressed encoded integer from input buffer and returns it.
   *
   * @param buf ByteBuffer input buffer
   * @throws IllegalStateException the value is not fit for int
   * @return deserialized integer from buffer.
   */
  public static int readVInt(ByteBuffer buf)
  {
    long n = readVLong(buf);
    if (n > Integer.MAX_VALUE || n < Integer.MIN_VALUE) {
      throw new IllegalStateException("value too long to fit in integer");
    }
    return (int)n;
  }

  public static long readVLong(BytesInputStream input)
  {
    byte firstByte = input.readByte();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      byte b = input.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return isNegativeVInt(firstByte) ? ~i : i;
  }

  public static int readVInt(BytesInputStream input)
  {
    long n = readVLong(input);
    if (n > Integer.MAX_VALUE || n < Integer.MIN_VALUE) {
      throw new IllegalStateException("value too long to fit in integer");
    }
    return (int) n;
  }

  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  public static int getVIntSize(long i)
  {
    if (i >= -112 && i <= 127) {
      return 1;
    }

    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }
}
