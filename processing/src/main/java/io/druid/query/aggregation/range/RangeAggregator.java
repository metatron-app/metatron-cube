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

package io.druid.query.aggregation.range;

import io.druid.query.aggregation.Aggregator;

public class RangeAggregator implements Aggregator
{
  private final Aggregator delegate;
  private final int start;
  private final int end;
  private int count = 0;
  private RangeAggregatorFactory.RangeState rangeState;

  public RangeAggregator(
      Aggregator delegate,
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
  public void aggregate()
  {
    switch(rangeState) {
      case beforeStart:
        if (count == start) {
          rangeState = RangeAggregatorFactory.RangeState.beforeLimit;
          delegate.reset();
        }
        delegate.aggregate();
        break;
      case beforeLimit:
        if (count == end) {
          rangeState = RangeAggregatorFactory.RangeState.afterLimit;
        } else {
          delegate.aggregate();
        }
        break;
      case afterLimit:
        // do nothing
        break;
    }
    count++;
  }

  @Override
  public void reset()
  {
    delegate.reset();
    count = 0;
    rangeState = RangeAggregatorFactory.RangeState.beforeStart;
  }

  @Override
  public Object get()
  {
    return delegate.get();
  }

  @Override
  public float getFloat()
  {
    return delegate.getFloat();
  }

  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public long getLong()
  {
    return delegate.getLong();
  }

  @Override
  public double getDouble()
  {
    return delegate.getDouble();
  }
}
