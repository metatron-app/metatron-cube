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
import io.druid.data.VLongUtils;
import io.druid.java.util.common.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

// copied from ByteStreams.ByteArrayDataInputStream
public class BytesInputStream extends ByteArrayInputStream implements ByteArrayDataInput
{
  private final DataInput input;

  public BytesInputStream(byte[] bytes)
  {
    super(bytes);
    this.input = new DataInputStream(this);
  }

  @Override
  public void readFully(byte b[])
  {
    try {
      input.readFully(b);
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void readFully(byte b[], int off, int len)
  {
    try {
      input.readFully(b, off, len);
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int skipBytes(int n)
  {
    try {
      return input.skipBytes(n);
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean readBoolean()
  {
    try {
      return input.readBoolean();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public byte readByte()
  {
    try {
      return input.readByte();
    }
    catch (EOFException e) {
      throw new IllegalStateException(e);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public int readUnsignedByte()
  {
    try {
      return input.readUnsignedByte();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public short readShort()
  {
    try {
      return input.readShort();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int readUnsignedShort()
  {
    try {
      return input.readUnsignedShort();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public char readChar()
  {
    try {
      return input.readChar();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int readInt()
  {
    try {
      return input.readInt();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public long readLong()
  {
    try {
      return input.readLong();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public float readFloat()
  {
    try {
      return input.readFloat();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public double readDouble()
  {
    try {
      return input.readDouble();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String readLine()
  {
    try {
      return input.readLine();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String readUTF()
  {
    try {
      return input.readUTF();
    }
    catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public String readVarSizeUTF()
  {
    final int length = readUnsignedVarInt();
    final String value = StringUtils.toUTF8String(buf, pos, length);
    pos += length;
    return value;
  }

  public byte[] readVarSizeBytes()
  {
    final byte[] bytes = new byte[readUnsignedVarInt()];
    readFully(bytes);
    return bytes;
  }

  // copied from org.apache.parquet.bytes.BytesUtils
  public int readUnsignedVarInt()
  {
    int value = 0;
    int i;
    int b;
    for (i = 0; ((b = readUnsignedByte()) & 128) != 0; i += 7) {
      value |= (b & 127) << i;
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

  public byte[] unwrap()
  {
    return buf;
  }

  public int position()
  {
    return pos;
  }
}
