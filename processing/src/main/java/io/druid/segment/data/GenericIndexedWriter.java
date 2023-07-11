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

import com.google.common.base.Preconditions;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BufferRef;
import io.druid.common.utils.IOUtils;
import io.druid.data.VLongUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.ByteBufferUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import static io.druid.segment.data.GenericIndexed.Feature.NO_OFFSET;
import static io.druid.segment.data.GenericIndexed.Feature.SORTED;
import static io.druid.segment.data.GenericIndexed.Feature.VSIZED_VALUE;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public abstract class GenericIndexedWriter<T> implements ColumnPartWriter<T>
{
  public static GenericIndexedWriter<String> forDictionaryV1(IOPeon ioPeon, String filenameBase)
  {
    return new V1<>(ioPeon, filenameBase, ObjectStrategy.STRING_STRATEGY, true);
  }

  public static GenericIndexedWriter<String> forDictionaryV2(IOPeon ioPeon, String filenameBase)
  {
    return new DictionaryWriter(ioPeon, filenameBase);
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

  protected final String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  final byte[] scratch = new byte[Integer.BYTES];

  @Override
  public void add(T object) throws IOException
  {
    final byte[] bytes = strategy.toBytes(object);
    writeValueLength(valuesOut, bytes.length);
    valuesOut.write(bytes);
    headerOut.write(IOUtils.intTo(valuesOut.getCount(), scratch));
    numRow++;
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
      metaOut.write(IOUtils.intTo(payload + Integer.BYTES, scratch));
      metaOut.write(IOUtils.intTo(numRow, scratch));
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
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    // called after getSerializedSize()
    long written = ioPeon.copyTo(channel, makeFilename("meta"));
    written += ioPeon.copyTo(channel, makeFilename("header"));
    written += ioPeon.copyTo(channel, makeFilename("values"));
    return written;
  }

  // this is just for index merger.. which only uses size() and get()
  public Indexed.BufferBacked<T> asIndexed() throws IOException
  {
    final MappedByteBuffer header = Files.map(ioPeon.getFile(makeFilename("header")));
    final MappedByteBuffer values = Files.map(ioPeon.getFile(makeFilename("values")));

    return new Indexed.BufferBacked<T>()
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
      public BufferRef getAsRef(int index)
      {
        final int position = index == 0 ? 0 : header.getInt((index - 1) << 2);
        values.position(position);

        final int length = readValueLength(values);
        return BufferRef.of(values, values.position(), length);
      }

      @Override
      public int size()
      {
        return numRow;
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
      valuesOut.write(IOUtils.intTo(length, scratch));
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
      super(ioPeon, filenameBase, strategy, sorted);
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

  private static final int NO_OFFSET_THRESHOLD = 7;
  private static final byte[][] PACK = new byte[NO_OFFSET_THRESHOLD][];

  static {
    for (int i = 0; i < PACK.length; i++) {
      PACK[i] = new byte[i];
    }
  }

  private static class DictionaryWriter extends V2<String>
  {
    private final String metaFile = makeFilename("meta.rewrite");
    private final String valuesFile = makeFilename("values.rewrite");

    private boolean hasNull;
    private int minLen = -1;
    private int maxLen = -1;

    public DictionaryWriter(IOPeon ioPeon, String filenameBase)
    {
      super(ioPeon, filenameBase, ObjectStrategy.STRING_STRATEGY, true);
    }

    private boolean noHeader()
    {
      return maxLen > 0 && maxLen - minLen < NO_OFFSET_THRESHOLD;
    }

    @Override
    protected void writeValueLength(OutputStream valuesOut, int length) throws IOException
    {
      super.writeValueLength(valuesOut, length);
      if (length == 0) {
        hasNull = true;
      } else {
        minLen = minLen < 0 ? length : Math.min(length, minLen);
        maxLen = maxLen < 0 ? length : Math.max(length, maxLen);
      }
    }

    @Override
    public void close() throws IOException
    {
      super.close();
      if (noHeader()) {
        final byte[] buf = new byte[1];
        final ByteBuffer wrap = ByteBuffer.wrap(buf);
        final boolean fixedLen = minLen == maxLen;
        try (Indexed.BufferBacked<String> dictionary = asIndexed();
             WritableByteChannel valuesOut = ioPeon.makeOutputChannel(valuesFile)) {
          int i = hasNull ? 1 : 0;
          for (; i < numRow; i++) {
            BufferRef ref = dictionary.getAsRef(i);
            Preconditions.checkArgument(ref.length() >= minLen && ref.length() <= maxLen);
            if (!fixedLen) {
              buf[0] = (byte) (ref.length() - minLen);
              valuesOut.write((ByteBuffer) wrap.position(0));
            }
            valuesOut.write(ref.toBuffer());
            if (!fixedLen && ref.length() < maxLen) {
              valuesOut.write(ByteBuffer.wrap(PACK[maxLen - ref.length()]));
            }
          }
        }
        int length1 = VLongUtils.sizeOfUnsignedVarInt(minLen);
        int length2 = VLongUtils.sizeOfUnsignedVarInt(maxLen);
        int payload = Ints.checkedCast(ioPeon.getFile(valuesFile).length());

        final BytesOutputStream scratch = new BytesOutputStream();
        try (OutputStream metaOut = ioPeon.makeOutputStream(metaFile)) {
          scratch.write(GenericIndexed.version);
          scratch.write(NO_OFFSET.set(flag(), true));
          scratch.write(Ints.toByteArray(payload + Integer.BYTES + Byte.BYTES + length1 + length2));
          scratch.write(Ints.toByteArray(numRow));
          scratch.writeBoolean(hasNull);
          scratch.writeUnsignedVarInt(minLen);
          scratch.writeUnsignedVarInt(maxLen);
          metaOut.write(scratch.unwrap(), 0, scratch.size());
        }
      }
    }

    @Override
    public long getSerializedSize()
    {
      if (noHeader()) {
        return ioPeon.getFile(metaFile).length() +
               ioPeon.getFile(valuesFile).length();
      } else {
        return super.getSerializedSize();
      }
    }

    @Override
    public long writeToChannel(WritableByteChannel channel) throws IOException
    {
      if (noHeader()) {
        // called after getSerializedSize()
        long written = ioPeon.copyTo(channel, metaFile);
        written += ioPeon.copyTo(channel, valuesFile);
        return written;
      } else {
        return super.writeToChannel(channel);
      }
    }
  }
}
