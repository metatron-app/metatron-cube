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

import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 *
 */
public class VintsValues implements IntsValues, WritableSupplier<IntsValues>
{
  private static final byte version = 0x1;

  public static VintsValues fromIterable(Iterable<VintValues> objectsIterable)
  {
    Iterator<VintValues> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new VintsValues(buffer, 4);
    }

    int numBytes = -1;

    int count = 0;
    while (objects.hasNext()) {
      VintValues next = objects.next();
      if (numBytes == -1) {
        numBytes = next.getNumBytes();
      }
      ++count;
    }

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream(4 + (count * 4));
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    int offset = 0;

    try {
      headerBytes.write(Ints.toByteArray(count));

      for (VintValues object : objectsIterable) {
        if (object.getNumBytes() != numBytes) {
          throw new ISE("val.numBytes[%s] != numBytesInValue[%s]", object.getNumBytes(), numBytes);
        }
        byte[] bytes = object.getBytesNoPadding();
        offset += bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(bytes);
      }
      valueBytes.write(new byte[4 - numBytes]);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(headerBytes.size() + valueBytes.size());
    theBuffer.put(headerBytes.toByteArray());
    theBuffer.put(valueBytes.toByteArray());
    theBuffer.flip();

    return new VintsValues(theBuffer.asReadOnlyBuffer(), numBytes);
  }

  private final ByteBuffer theBuffer;
  private final int numBytes;
  private final int size;

  private final int valuesOffset;
  private final int bufferBytes;

  VintsValues(
      ByteBuffer buffer,
      int numBytes
  )
  {
    this.theBuffer = buffer;
    this.numBytes = numBytes;

    size = theBuffer.getInt();
    valuesOffset = theBuffer.position() + (size << 2);
    bufferBytes = 4 - numBytes;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public VintValues get(int index)
  {
    if (index >= size) {
      throw new IllegalArgumentException(String.format("Index[%s] >= size[%s]", index, size));
    }

    ByteBuffer myBuffer = theBuffer.asReadOnlyBuffer();
    int startOffset = 0;
    int endOffset;

    if (index == 0) {
      endOffset = myBuffer.getInt();
    } else {
      myBuffer.position(myBuffer.position() + ((index - 1) * Integer.BYTES));
      startOffset = myBuffer.getInt();
      endOffset = myBuffer.getInt();
    }

    myBuffer.position(valuesOffset + startOffset);
    myBuffer.limit(myBuffer.position() + (endOffset - startOffset) + bufferBytes);
    return myBuffer.hasRemaining() ? new VintValues(myBuffer, numBytes) : null;
  }

  @Override
  public int indexOf(IndexedInts value)
  {
    throw new UnsupportedOperationException("Reverse lookup not allowed.");
  }

  public long getSerializedSize()
  {
    return theBuffer.remaining() + 4 + 4 + 2;
  }

  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = channel.write(ByteBuffer.wrap(new byte[]{version, (byte) numBytes}));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    written += channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    written += channel.write(theBuffer.asReadOnlyBuffer());
    return written;
  }

  @Override
  public int numRows()
  {
    return size;
  }

  @Override
  public Class<? extends IntsValues> provides()
  {
    return VintsValues.class;
  }

  @Override
  public IntsValues get()
  {
    return this;
  }

  @Override
  public Iterator<IndexedInts> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public void close() throws IOException
  {
    // no-op
  }

  public static VintsValues readFromByteBuffer(ByteBuffer buffer)
  {
    byte versionFromBuffer = buffer.get();

    if (version == versionFromBuffer) {
      int numBytes = buffer.get();
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new VintsValues(bufferToUse, numBytes);
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }
}
