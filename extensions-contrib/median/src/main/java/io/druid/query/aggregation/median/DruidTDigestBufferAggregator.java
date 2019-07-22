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

package io.druid.query.aggregation.median;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class DruidTDigestBufferAggregator extends BufferAggregator.Abstract
{
  private final ObjectColumnSelector selector;
  private final double compression;

  public DruidTDigestBufferAggregator(
      ObjectColumnSelector selector,
      int compression
  )
  {
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    DruidTDigest digest = new DruidTDigest(compression);
    digest.asBytes(mutationBuffer);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    DruidTDigest digest = DruidTDigest.fromBytes(mutationBuffer);
    digest.add(selector.get());

    mutationBuffer.position(position);
    digest.asBytes(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    return DruidTDigest.fromBytes(mutationBuffer);
  }
}
