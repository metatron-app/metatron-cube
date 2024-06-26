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
import com.google.common.io.Closeables;
import com.google.common.io.CountingOutputStream;
import io.druid.common.utils.IOUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

/**
 * Streams arrays of objects out in the binary format described by VSizeIndexed
 */
public class VintsWriter implements IntsWriter, Closeable
{
  private static final byte VERSION = 0x1;
  private static final byte[] EMPTY_ARRAY = new byte[]{};

  private final IOPeon ioPeon;
  private final String metaFileName;
  private final String headerFileName;
  private final String valuesFileName;
  private final int maxId;
  private final int numBytes;

  private CountingOutputStream headerOut;
  private CountingOutputStream valuesOut;
  private int numWritten;

  public VintsWriter(IOPeon ioPeon, String filenameBase, int maxId)
  {
    this.ioPeon = ioPeon;
    this.metaFileName = String.format("%s.meta", filenameBase);
    this.headerFileName = String.format("%s.header", filenameBase);
    this.valuesFileName = String.format("%s.values", filenameBase);
    this.maxId = maxId;
    this.numBytes = VintValues.getNumBytesForMax(maxId);
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(headerFileName));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valuesFileName));
  }

  private final byte[] scratch = new byte[Integer.BYTES];

  @Override
  public void add(List<Integer> val) throws IOException
  {
    byte[] bytesToWrite = val == null ? EMPTY_ARRAY : VintValues.getBytesNoPaddingfromList(val, maxId);

    valuesOut.write(bytesToWrite);
    headerOut.write(IOUtils.intTo(valuesOut.getCount(), scratch));

    ++numWritten;
  }

  @Override
  public void add(int[] vals) throws IOException
  {
    if (vals == null) {
      vals = EMPTY_ROW;
    }
    for (int i = 0; i < vals.length; i++) {
      byte[] intAsBytes = IOUtils.intTo(vals[i], scratch);
      valuesOut.write(intAsBytes, intAsBytes.length - numBytes, numBytes);
    }
    headerOut.write(IOUtils.intTo(valuesOut.getCount(), scratch));

    ++numWritten;
  }

  @Override
  public void close() throws IOException
  {
    final byte numBytesForMax = VintValues.getNumBytesForMax(maxId);

    valuesOut.write(new byte[Integer.BYTES - numBytesForMax]);

    Closeables.close(headerOut, false);
    Closeables.close(valuesOut, false);

    final long written = headerOut.getCount() + valuesOut.getCount();

    Preconditions.checkState(
        headerOut.getCount() == numWritten * 4L,
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.getCount()
    );

    try (OutputStream metaOut = ioPeon.makeOutputStream(metaFileName)) {
      metaOut.write(new byte[]{VERSION, numBytesForMax});
      metaOut.write(IOUtils.intTo(written + Integer.BYTES, scratch));
      metaOut.write(IOUtils.intTo(numWritten, scratch));
    }
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +    // version
           1 +    // numBytes
           4 +    // numBytesWritten
           4 +    // numElements
           headerOut.getCount() +
           valuesOut.getCount();
  }

  @Override
  public long writeToChannel(WritableByteChannel channel) throws IOException
  {
    long written = 0;
    for (String fileName : Arrays.asList(metaFileName, headerFileName, valuesFileName)) {
      try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(fileName))) {
        written += FileSmoosher.transfer(channel, input);
      }
    }
    return written;
  }
}
