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

package io.druid.query.aggregation.range;

import io.druid.query.aggregation.BufferAggregator;

import java.nio.ByteBuffer;

public class RangeBufferAggregator implements BufferAggregator
{
  private final BufferAggregator delegate;
  private final int start, end;

  private RangeAggregatorFactory.RangeState rangeState;
  private int count = 0;

  public RangeBufferAggregator(
      BufferAggregator delegate,
      int start,
      int limit
  )
  {
    this.delegate = delegate;
    this.start = start;
    this.end = start + limit;
    this.rangeState = RangeAggregatorFactory.RangeState.beforeStart;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    rangeState = RangeAggregatorFactory.RangeState.beforeStart;
    delegate.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    switch(rangeState) {
      case beforeStart:
        if (count == start) {
          rangeState = RangeAggregatorFactory.RangeState.beforeLimit;
          delegate.init(buf, position);
        }
        delegate.aggregate(buf, position);
        break;
      case beforeLimit:
        if (count == end) {
          rangeState = RangeAggregatorFactory.RangeState.afterLimit;
        } else {
          delegate.aggregate(buf, position);
        }
        break;
      case afterLimit:
        // do nothing
        break;
    }
    count++;
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return delegate.get(buf, position);
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
