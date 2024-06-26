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

package io.druid.segment;

import io.druid.common.guava.ResourceClosingYielder;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.guava.YieldingAccumulator;
import io.druid.common.guava.YieldingSequenceBase;

import java.io.Closeable;
import java.util.List;

/**
 */
public class ReferenceCountingSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;
  private final ReferenceCountingSegment segment;

  public ReferenceCountingSequence(Sequence<T> baseSequence, ReferenceCountingSegment segment)
  {
    this.baseSequence = baseSequence;
    this.segment = segment;
  }

  @Override
  public List<String> columns()
  {
    return baseSequence.columns();
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    final Closeable closeable = segment.increment();
    return new ResourceClosingYielder<OutType>(baseSequence.toYielder(initValue, accumulator), closeable);
  }
}
