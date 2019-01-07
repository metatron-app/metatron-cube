package io.druid.data.input;

import com.google.common.base.Charsets;
import com.google.common.io.ByteArrayDataInput;

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
    final int size = readUnsignedVarInt();
    final String value = new String(buf, pos, size, Charsets.UTF_8);
    pos += size;
    return value;
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
}
