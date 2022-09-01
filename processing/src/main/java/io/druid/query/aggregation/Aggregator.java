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

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;

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

  interface Simple<T> extends Aggregator<T>
  {
    @Override
    default T get(T current)
    {
      return current;
    }
  }

  interface Estimable<T> extends Aggregator<T>
  {
    int estimateOccupation(T current);
  }

  interface LongType<T> extends Aggregator<T>
  {
    Long get(T current);

    boolean getLong(T current, MutableLong handover);
  }

  interface FromMutableLong extends LongType<MutableLong>
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

  interface FloatType<T> extends Aggregator<T>
  {
    Float get(T current);

    boolean getFloat(T current, MutableFloat handover);
  }

  interface FromMutableFloat extends FloatType<MutableFloat>
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

  interface DoubleType<T> extends Aggregator<T>
  {
    Double get(T current);

    boolean getDouble(T current, MutableDouble handover);
  }

  interface FromMutableDouble extends DoubleType<MutableDouble>
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
}
