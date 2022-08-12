/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.segment.data;

import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.druid.data.VLongUtils;
import io.druid.java.util.common.ByteBufferUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import static io.druid.segment.data.GenericIndexed.Feature.SORTED;
import static io.druid.segment.data.GenericIndexed.Feature.VSIZED_VALUE;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public abstract class GenericIndexedWriter<T> extends ColumnPartWriter.Abstract<T>
{
  public static GenericIndexedWriter<String> forDictionaryV1(IOPeon ioPeon, String filenameBase)
  {
    return new V1<>(ioPeon, filenameBase, ObjectStrategy.STRING_STRATEGY, true);
  }

  public static GenericIndexedWriter<String> forDictionaryV2(IOPeon ioPeon, String filenameBase)
  {
    return new V2<>(ioPeon, filenameBase, ObjectStrategy.STRING_STRATEGY, true);
  }

  public static <T> GenericIndexedWriter<T> v1(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy)
  {
    return new V1<>(ioPeon, filenameBase, strategy, false);
  }

  public static <T> GenericIndexedWriter<T> v2(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy)
  {
    return new V2<>(ioPeon, filenameBase, strategy, false);
  }

  final IOPeon ioPeon;
  final String filenameBase;
  final ObjectStrategy<T> strategy;
  final boolean sorted;

  CountingOutputStream headerOut;
  CountingOutputStream valuesOut;
  int numRow;

  private GenericIndexedWriter(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy, boolean sorted)
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.sorted = sorted;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("values")));
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  private final byte[] scratch = new byte[Integer.BYTES];

  @Override
  public void add(T object) throws IOException
  {
    final byte[] bytes = strategy.toBytes(object);
    writeValueLength(valuesOut, bytes.length);
    valuesOut.write(bytes);

    final int offset = Ints.checkedCast(valuesOut.getCount());
    headerOut.write(toInt(offset));
    numRow++;
  }

  final byte[] toInt(int offset)
  {
    scratch[0] = (byte) (offset >> 24);
    scratch[1] = (byte) (offset >> 16);
    scratch[2] = (byte) (offset >> 8);
    scratch[3] = (byte) offset;
    return scratch;
  }

  protected abstract int flag();

  protected abstract void writeValueLength(OutputStream valuesOut, int length) throws IOException;

  protected abstract int readValueLength(ByteBuffer values);

  @Override
  public void close() throws IOException
  {
    // called after all add()
    valuesOut.close();
    headerOut.close();
    final int payload = Ints.checkedCast(valuesOut.getCount() + headerOut.getCount());

    try (OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"))) {
      metaOut.write(GenericIndexed.version);
      metaOut.write(flag());
      metaOut.write(Ints.toByteArray(payload + Integer.BYTES));
      metaOut.write(Ints.toByteArray(numRow));
    }
  }

  @Override
  public long getSerializedSize()
  {
    // called after close()
    return Byte.BYTES +           // version
           Byte.BYTES +           // flag
           Integer.BYTES +        // numBytesWritten
           Integer.BYTES +        // numElements
           headerOut.getCount() + // header length
           valuesOut.getCount();  // values length
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    // called after getSerializedSize()
    ioPeon.copyTo(channel, makeFilename("meta"));
    ioPeon.copyTo(channel, makeFilename("header"));
    ioPeon.copyTo(channel, makeFilename("values"));
  }

  // this is just for index merger.. which only uses size() and get()
  public Indexed.Closeable<T> asIndexed() throws IOException
  {
    final MappedByteBuffer header = Files.map(ioPeon.getFile(makeFilename("header")));
    final MappedByteBuffer values = Files.map(ioPeon.getFile(makeFilename("values")));

    return new Indexed.Closeable<T>()
    {
      private final ObjectStrategy<T> dedicated = ObjectStrategies.singleThreaded(strategy);

      @Override
      public void close() throws IOException
      {
        ByteBufferUtils.unmap(header);
        ByteBufferUtils.unmap(values);
      }

      @Override
      public T get(int index)
      {
        final int position = index == 0 ? 0 : header.getInt((index - 1) << 2);
        values.position(position);

        final int length = readValueLength(values);
        // fromByteBuffer must not modify the buffer limit
        return length == 0 ? null : dedicated.fromByteBuffer(values, length);
      }

      @Override
      public int size()
      {
        return numRow;
      }

      @Override
      public int indexOf(T value)
      {
        throw new UnsupportedOperationException("indexOf");
      }

      @Override
      public Iterator<T> iterator()
      {
        throw new UnsupportedOperationException("iterator");
      }
    };
  }

  private static class V1<T> extends GenericIndexedWriter<T>
  {
    public V1(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy, boolean sorted)
    {
      super(ioPeon, filenameBase, strategy, sorted);
    }

    @Override
    protected int flag()
    {
      return sorted ? 1 : 0;
    }

    @Override
    protected void writeValueLength(OutputStream valuesOut, int length) throws IOException
    {
      valuesOut.write(toInt(length));
    }

    @Override
    protected int readValueLength(ByteBuffer values)
    {
      return values.getInt();
    }
  }

  private static class V2<T> extends GenericIndexedWriter<T>
  {
    public V2(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy, boolean sorted)
    {
      super(ioPeon, filenameBase, strategy, true);
    }

    @Override
    protected int flag()
    {
      return SORTED.set(VSIZED_VALUE.getMask(), sorted);
    }

    @Override
    protected void writeValueLength(OutputStream valuesOut, int length) throws IOException
    {
      VLongUtils.writeUnsignedVarInt(valuesOut, length);
    }

    @Override
    protected int readValueLength(ByteBuffer values)
    {
      return VLongUtils.readUnsignedVarInt(values);
    }
  }
}
