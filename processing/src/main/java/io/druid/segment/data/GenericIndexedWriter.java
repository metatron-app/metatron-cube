/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.data.ValueDesc;
import io.druid.query.sketch.TypedSketch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements ColumnPartWriter<T>
{
  private static Logger LOG = new Logger(GenericIndexedWriter.class);

  public static GenericIndexedWriter<String> dimWriter(
      IOPeon ioPeon,
      String filenameBase,
      boolean sketch
  )
  {
    return new GenericIndexedWriter<>(
        ioPeon,
        filenameBase,
        ObjectStrategy.STRING_STRATEGY,
        sketch
    );
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;
  private final boolean sketch;

  private boolean sorted;
  private T prevObject = null;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valuesOut = null;
  private CountingOutputStream quantileOut = null;
  private CountingOutputStream thetaOut = null;
  private int numWritten = 0;

  private ItemsUnion quantile;
  private Union theta;

  public GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy,
      boolean sketch
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.sketch = sketch && strategy.getClazz() == String.class;
    this.sorted = !(strategy instanceof ObjectStrategy.NotComparable);
  }

  public GenericIndexedWriter(IOPeon ioPeon, String filenameBase, ObjectStrategy<T> strategy)
  {
    this(ioPeon, filenameBase, strategy, false);
  }

  public boolean isSorted()
  {
    return sorted;
  }

  public boolean hasSketch()
  {
    return sketch;
  }

  public ItemsSketch getQuantile()
  {
    return sketch ? quantile.getResult() : null;
  }

  public Sketch getTheta()
  {
    return sketch ? theta.getResult() : null;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("values")));
    if (sketch) {
      quantileOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("quantile")));
      quantile = ItemsUnion.getInstance(1024, Ordering.natural().nullsFirst()); // need not to be exact

      thetaOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("theta")));
      theta = (Union) SetOperation.builder().setNominalEntries(16384).build(Family.UNION); // need not to be exact
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(T objectToWrite) throws IOException
  {
    if (sorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      sorted = false;
    }
    if (sketch) {
      quantile.update(objectToWrite);
      theta.update((String) objectToWrite);
    }

    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    ++numWritten;
    valuesOut.write(Ints.toByteArray(bytesToWrite.length));
    valuesOut.write(bytesToWrite);

    headerOut.write(Ints.toByteArray((int) valuesOut.getCount()));

    prevObject = objectToWrite;
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() throws IOException
  {
    headerOut.close();
    valuesOut.close();

    Preconditions.checkState(
        headerOut.getCount() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.getCount()
    );

    if (quantileOut != null) {
      quantileOut.write(quantile.toByteArray(TypedSketch.toItemsSerDe(ValueDesc.STRING)));
      quantileOut.close();
    }
    if (thetaOut != null) {
      thetaOut.write(theta.getResult().toByteArray());
      thetaOut.close();
    }
  }

  @Override
  public long getSerializedSize()
  {
    return Byte.BYTES +           // version
           Byte.BYTES +           // flag
           (quantileOut == null ? 0 : Ints.BYTES + quantileOut.getCount()) +   // quantile
           (thetaOut == null ? 0 : Ints.BYTES + thetaOut.getCount()) +   // theta
           Ints.BYTES +           // numBytesWritten
           Ints.BYTES +           // numElements
           headerOut.getCount() + // header length
           valuesOut.getCount();  // value length
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    byte flag = 0;
    if (sorted) {
      flag |= GenericIndexed.Feature.SORTED.getMask();
    }
    if (sketch) {
      flag |= GenericIndexed.Feature.SKETCH.getMask();
    }
    channel.write(ByteBuffer.wrap(new byte[] {GenericIndexed.version, flag}));

    if (sketch) {
      // size + quantile
      channel.write(ByteBuffer.wrap(Ints.toByteArray(Ints.checkedCast(quantileOut.getCount()))));
      try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("quantile")))) {
        ByteStreams.copy(input, channel);
      }
      // size + theta
      channel.write(ByteBuffer.wrap(Ints.toByteArray(Ints.checkedCast(thetaOut.getCount()))));
      try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("theta")))) {
        ByteStreams.copy(input, channel);
      }
    }

    // size + count + header + values
    int length = Ints.checkedCast(headerOut.getCount() + valuesOut.getCount() + Integer.BYTES);
    channel.write(ByteBuffer.wrap(Ints.toByteArray(length)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numWritten)));
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("header")))) {
      ByteStreams.copy(input, channel);
    }
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("values")))) {
      ByteStreams.copy(input, channel);
    }
  }
}
