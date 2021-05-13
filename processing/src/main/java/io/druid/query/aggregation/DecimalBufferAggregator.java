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

package io.druid.query.aggregation;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

/**
 */
public abstract class DecimalBufferAggregator implements BufferAggregator
{
  final int precision;
  final int scale;
  final RoundingMode roundingMode;

  protected DecimalBufferAggregator(DecimalMetricSerde serde)
  {
    this.precision = serde.precision();
    this.scale = serde.scale();
    this.roundingMode = serde.roundingMode();
  }

  protected final BigDecimal read(ByteBuffer buf, int position)
  {
    buf = position(buf, position);
    final byte length = buf.get();
    if (length == 0) {
      return null;
    }
    byte[] value = new byte[length];
    buf.get(value, 0, value.length);
    return new BigDecimal(new BigInteger(value), scale);
  }

  protected final void write(ByteBuffer buf, int position, BigDecimal decimal)
  {
    buf = position(buf, position);
    byte[] value = decimal.unscaledValue().toByteArray();
    Preconditions.checkArgument(value.length < 128, "overflow");
    buf.put((byte) value.length);
    buf.put(value, 0, value.length);
  }

  @Override
  public void init(ByteBuffer buf, int position0, int position1)
  {
    position(buf, position1).put((byte) 0);
  }

  private ByteBuffer position(ByteBuffer buf, int position)
  {
    return (ByteBuffer) buf.position(position);
  }

  @Override
  public BigDecimal get(ByteBuffer buf, int position0, int position1)
  {
    return read(buf, position1);
  }
}
