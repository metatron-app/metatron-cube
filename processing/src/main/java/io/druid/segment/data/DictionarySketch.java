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

package io.druid.segment.data;

import com.metamx.common.IAE;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.data.ValueDesc;
import io.druid.query.sketch.SketchOp;
import io.druid.query.sketch.TypedSketch;

import java.nio.ByteBuffer;

public class DictionarySketch
{
  public static DictionarySketch of(ByteBuffer buffer)
  {
    final byte version = buffer.get();
    if (version != SketchWriter.version) {
      throw new IAE("Unknown version[%s]", version);
    }
    byte flag = buffer.get();
    ByteBuffer quantile = ByteBufferSerializer.prepareForRead(buffer);
    ByteBuffer theta = ByteBufferSerializer.prepareForRead(buffer);
    return new DictionarySketch(flag, quantile, theta);
  }

  private final byte flag;
  private final ByteBuffer quantile;
  private final ByteBuffer theta;

  public DictionarySketch(byte flag, ByteBuffer quantile, ByteBuffer theta)
  {
    this.flag = flag;
    this.quantile = quantile;
    this.theta = theta;
  }

  public byte getFlag()
  {
    return flag;
  }

  public ItemsSketch getQuantile(ValueDesc valueDesc)
  {
    return (ItemsSketch) TypedSketch.readPart(quantile, SketchOp.QUANTILE, valueDesc);
  }

  public Sketch getTheta(ValueDesc valueDesc)
  {
    return (Sketch) TypedSketch.readPart(theta, SketchOp.THETA, valueDesc);
  }
}
