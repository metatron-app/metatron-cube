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

import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;

/**
 * An Aggregator is an object that can aggregate metrics.  Its aggregation-related methods (namely, aggregate() and get())
 * do not take any arguments as the assumption is that the Aggregator was given something in its constructor that
 * it can use to get at the next bit of data.
 * <p>
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate().  This is currently (as of this documentation) implemented through the use of Offset and
 * FloatColumnSelector objects.  The Aggregator has a handle on a FloatColumnSelector object which has a handle on an Offset.
 * QueryableIndex has both the Aggregators and the Offset object and iterates through the Offset calling the aggregate()
 * method on the Aggregators for each applicable row.
 * <p>
 * This interface is old and going away.  It is being replaced by BufferAggregator
 */
public interface Aggregator<T>
{
  T aggregate(T current);

  Object get(T current);

  default void clear(boolean close) {}

  // marker for finalizers (aka. get)
  interface BinaryIdentical { }

  interface Simple<T> extends Aggregator<T>, BinaryIdentical
  {
    @Override
    default T get(T current) {return current;}
  }

  interface Estimable<T> extends Aggregator<T>
  {
    int estimateOccupation(T current);
  }

  interface Streaming<T> extends Aggregator<T>
  {
    boolean supports();

    Object aggregate(IntIterator iterator);
  }

  interface Vectorized<V> extends Closeable
  {
    V init(int length);

    void aggregate(IntIterator iterator, V vector, Int2IntFunction offset, int size);

    Object get(V vector, int offset);
  }

  interface LongType<T> extends Aggregator<T>
  {
    Long get(T current);

    boolean getLong(T current, MutableLong handover);
  }

  interface FromMutableLong extends LongType<MutableLong>, BinaryIdentical
  {
    @Override
    default Long get(MutableLong current)
    {
      return current == null ? null : current.longValue();
    }

    @Override
    default boolean getLong(MutableLong current, MutableLong handover)
    {
      if (current != null) {
        handover.setValue(current.longValue());
        return true;
      }
      return false;
    }
  }

  interface LongStreaming extends LongType<MutableLong>, Streaming<MutableLong>
  {
  }

  interface FloatType<T> extends Aggregator<T>
  {
    Float get(T current);

    boolean getFloat(T current, MutableFloat handover);
  }

  interface FromMutableFloat extends FloatType<MutableFloat>, BinaryIdentical
  {
    @Override
    default Float get(MutableFloat current)
    {
      return current == null ? null : current.floatValue();
    }

    @Override
    default boolean getFloat(MutableFloat current, MutableFloat handover)
    {
      if (current != null) {
        handover.setValue(current.floatValue());
        return true;
      }
      return false;
    }
  }

  interface FloatStreaming extends FloatType<MutableFloat>, Streaming<MutableFloat>
  {
  }

  interface DoubleType<T> extends Aggregator<T>
  {
    Double get(T current);

    boolean getDouble(T current, MutableDouble handover);
  }

  interface FromMutableDouble extends DoubleType<MutableDouble>, BinaryIdentical
  {
    @Override
    default Double get(MutableDouble current)
    {
      return current == null ? null : current.doubleValue();
    }

    @Override
    default boolean getDouble(MutableDouble current, MutableDouble handover)
    {
      if (current != null) {
        handover.setValue(current.doubleValue());
        return true;
      }
      return false;
    }
  }

  interface DoubleStreaming extends DoubleType<MutableDouble>, Streaming<MutableDouble>
  {
  }

  interface StreamingSupport<T> extends Aggregator<T>
  {
    Aggregator<T> streaming();
  }

  Aggregator NULL = new Aggregator()
  {
    @Override
    public Object aggregate(Object current)
    {
      return null;
    }

    @Override
    public Object get(Object current)
    {
      return null;
    }
  };

  static Aggregator relay(Object value)
  {
    return new Aggregator()
    {
      @Override
      public Object aggregate(Object current)
      {
        return current;
      }

      @Override
      public Object get(Object current)
      {
        return value;
      }
    };
  }
}
