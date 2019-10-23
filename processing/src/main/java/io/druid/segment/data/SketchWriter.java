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

import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
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
public class SketchWriter implements ColumnPartWriter<Pair<String, Integer>>
{
  static Logger LOG = new Logger(SketchWriter.class);

  static byte version = 0x01;

  private final IOPeon ioPeon;
  private final String filenameBase;

  private CountingOutputStream quantileOut = null;
  private CountingOutputStream thetaOut = null;
  private int numWritten = 0;

  private ItemsUnion quantile;
  private Union theta;

  public SketchWriter(IOPeon ioPeon, String filenameBase)
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
  }

  public ItemsSketch getQuantile()
  {
    return quantile.getResult();
  }

  public Sketch getTheta()
  {
    return theta.getResult();
  }

  @Override
  public void open() throws IOException
  {
    quantileOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("quantile")));
    quantile = ItemsUnion.getInstance(1024, Ordering.natural().nullsFirst()); // need not to be exact

    thetaOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("theta")));
    theta = (Union) SetOperation.builder().setNominalEntries(16384).build(Family.UNION); // need not to be exact
  }

  @Override
  @SuppressWarnings("unchecked")
  public void add(Pair<String, Integer> pair) throws IOException
  {
    for (int i = 0; i < pair.rhs; i++) {
      quantile.update(pair.lhs);  // cardinality sensitive
    }
    theta.update(pair.lhs);
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() throws IOException
  {
    quantileOut.write(quantile.toByteArray(TypedSketch.toItemsSerDe(ValueDesc.STRING)));
    quantileOut.close();
    thetaOut.write(theta.getResult().toByteArray());
    thetaOut.close();
  }

  @Override
  public long getSerializedSize()
  {
    return Byte.BYTES +           // version
           Byte.BYTES +           // flag
           Ints.BYTES + quantileOut.getCount() +   // quantile
           Ints.BYTES + thetaOut.getCount();       // theta
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    byte flag = 0;
    channel.write(ByteBuffer.wrap(new byte[]{version, flag}));

    // size + quantile
    final int quantileLen = Ints.checkedCast(quantileOut.getCount());
    channel.write(ByteBuffer.wrap(Ints.toByteArray(quantileLen)));
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("quantile")))) {
      ByteStreams.copy(input, channel);
    }
    // size + theta
    final int thetaLen = Ints.checkedCast(thetaOut.getCount());
    channel.write(ByteBuffer.wrap(Ints.toByteArray(thetaLen)));
    try (ReadableByteChannel input = Channels.newChannel(ioPeon.makeInputStream(makeFilename("theta")))) {
      ByteStreams.copy(input, channel);
    }
  }
}
