package io.druid.data.input;

import com.google.common.io.ByteArrayDataOutput;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

// copied from ByteStreams.ByteArrayDataOutputStream
public class BytesOutputStream extends ByteArrayOutputStream implements ByteArrayDataOutput
{
  private final DataOutput output;

  public BytesOutputStream()
  {
    this.output = new DataOutputStream(this);
  }

  public BytesOutputStream(int size)
  {
    super(size);
    this.output = new DataOutputStream(this);
  }

  @Override
  public void write(int b)
  {
    super.write(b);
  }

  @Override
  public void write(byte[] b)
  {
    super.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len)
  {
    super.write(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v)
  {
    try {
      output.writeBoolean(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeByte(int v)
  {
    try {
      output.writeByte(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeBytes(String s)
  {
    try {
      output.writeBytes(s);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeChar(int v)
  {
    try {
      output.writeChar(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeChars(String s)
  {
    try {
      output.writeChars(s);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeDouble(double v)
  {
    try {
      output.writeDouble(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeFloat(float v)
  {
    try {
      output.writeFloat(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeInt(int v)
  {
    try {
      output.writeInt(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeLong(long v)
  {
    try {
      output.writeLong(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeShort(int v)
  {
    try {
      output.writeShort(v);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  @Override
  public void writeUTF(String s)
  {
    try {
      output.writeUTF(s);
    }
    catch (IOException impossible) {
      throw new AssertionError(impossible);
    }
  }

  public void writeVarSizeBytes(byte[] value)
  {
    writeUnsignedVarInt(value.length);
    write(value);
  }

  // copied from org.apache.parquet.bytes.BytesUtils
  public void writeUnsignedVarInt(int value)
  {
    while ((long) (value & -128) != 0L) {
      write(value & 127 | 128);
      value >>>= 7;
    }
    write(value & 127);
  }
}