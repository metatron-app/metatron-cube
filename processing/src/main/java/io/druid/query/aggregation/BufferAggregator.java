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

import java.nio.ByteBuffer;

/**
 * A BufferAggregator is an object that can aggregate metrics into a ByteBuffer.  Its aggregation-related methods
 * (namely, aggregate(...) and get(...)) only take the ByteBuffer and position because it is assumed that the Aggregator
 * was given something (one or more MetricSelector(s)) in its constructor that it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate(...).
 */
public interface BufferAggregator
{
  /**
   * Initializes the buffer location
   *
   * Implementations of this method must initialize the byte buffer at the given position
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * This method must not exceed the number of bytes returned by {@link AggregatorFactory#getMaxIntermediateSize()}
   * in the corresponding {@link AggregatorFactory}
   * @param buf byte buffer to initialize
   * @param position0
   * @param position1 offset within the byte buffer for initialization
   */
  void init(ByteBuffer buf, int position0, int position1);

  /**
   * Aggregates metric values into the given aggregate byte representation
   *
   * Implementations of this method must read in the aggregate value from the buffer at the given position,
   * aggregate the next element of data and write the updated aggregate value back into the buffer.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *  @param buf byte buffer storing the byte array representation of the aggregate
   * @param position0
   * @param position1 offset within the byte buffer at which the current aggregate value is stored
   */
  void aggregate(ByteBuffer buf, int position0, int position1);

  /**
   * Returns the intermediate object representation of the given aggregate.
   *
   * Converts the given byte buffer representation into an intermediate aggregate Object
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position0
   * @param position1 offset within the byte buffer at which the aggregate value is stored
   * @return the Object representation of the aggregate
   */
  Object get(ByteBuffer buf, int position0, int position1);

  /**
   * Release any resources used by the aggregator
   * @param close
   */
  default void clear(boolean close) {}

  abstract class NullSupport implements BufferAggregator
  {
    protected static final byte NULL = 0x00;
    protected static final byte NOT_NULL = 0x01;

    @Override
    public void init(ByteBuffer buf, int position0, int position1)
    {
      buf.put(position1, NULL);
    }

    protected static boolean isNull(ByteBuffer buf, int position)
    {
      return buf.get(position) == NULL;
    }
  }

  BufferAggregator NULL = new BufferAggregator()
  {
    @Override
    public void init(ByteBuffer buf, int position0, int position1)
    {
    }

    @Override
    public void aggregate(ByteBuffer buf, int position0, int position1)
    {
    }

    @Override
    public Object get(ByteBuffer buf, int position0, int position1)
    {
      return null;
    }
  };

  static BufferAggregator relay(Object value)
  {
    return new BufferAggregator()
    {
      @Override
      public void init(ByteBuffer buf, int position0, int position1) {}

      @Override
      public void aggregate(ByteBuffer buf, int position0, int position1) {}

      @Override
      public Object get(ByteBuffer buf, int position0, int position1)
      {
        return value;
      }
    };
  }

  class Delegated implements BufferAggregator
  {
    private final BufferAggregator delegate;

    public Delegated(BufferAggregator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void init(ByteBuffer buf, int position0, int position1)
    {
      delegate.init(buf, position0, position1);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position0, int position1)
    {
      delegate.aggregate(buf, position0, position1);
    }

    @Override
    public Object get(ByteBuffer buf, int position0, int position1)
    {
      return delegate.get(buf, position0, position1);
    }

    @Override
    public void clear(boolean close)
    {
      delegate.clear(close);
    }
  }
}
