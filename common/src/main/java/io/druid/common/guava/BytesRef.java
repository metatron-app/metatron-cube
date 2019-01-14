package io.druid.common.guava;

import java.util.Arrays;

public class BytesRef
{
  public final byte[] bytes;
  public final int from;
  public final int len;

  public BytesRef(byte[] bytes, int len)
  {
    this(bytes, 0, len);
  }

  public BytesRef(byte[] bytes, int from, int len)
  {
    this.bytes = bytes;
    this.from = from;
    this.len = len;
  }

  public byte[] asArray()
  {
    return Arrays.copyOfRange(bytes, from, from + len);
  }
}
