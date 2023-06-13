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

import io.druid.data.ValueDesc;
import io.druid.segment.column.FloatScanner;
import io.druid.segment.column.IntDoubleConsumer;
import org.apache.commons.lang.mutable.MutableFloat;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.util.stream.DoubleStream;

/**
 * An object that gets a metric value.  Metric values are always floats and there is an assumption that the
 * FloatColumnSelector has a handle onto some other stateful object (e.g. an Offset) which is changing between calls
 * to get() (though, that doesn't have to be the case if you always want the same value...).
 */
public interface FloatColumnSelector extends ObjectColumnSelector<Float>
{
  default ValueDesc type()
  {
    return ValueDesc.FLOAT;
  }

  default boolean getFloat(MutableFloat handover)
  {
    Float fv = get();
    if (fv == null) {
      return false;
    } else {
      handover.setValue(fv.floatValue());
      return true;
    }
  }

  interface WithBaggage extends FloatColumnSelector, Closeable { }

  interface Scannable extends WithBaggage
  {
    void consume(IntIterator iterator, IntDoubleConsumer consumer);

    void scan(IntIterator iterator, FloatScanner scanner);

    DoubleStream stream(IntIterator iterator);
  }
}
