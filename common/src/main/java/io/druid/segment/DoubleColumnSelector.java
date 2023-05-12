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

package io.druid.segment;

import io.druid.data.ValueDesc;
import io.druid.segment.column.DoubleScanner;
import org.apache.commons.lang.mutable.MutableDouble;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.util.stream.DoubleStream;

/**
 *
 */
public interface DoubleColumnSelector extends ObjectColumnSelector<Double>
{
  default ValueDesc type()
  {
    return ValueDesc.DOUBLE;
  }

  default boolean getDouble(MutableDouble handover)
  {
    Double dv = get();
    if (dv == null) {
      return false;
    } else {
      handover.setValue(dv.doubleValue());
      return true;
    }
  }

  interface WithBaggage extends DoubleColumnSelector, Closeable { }

  interface Scannable extends WithBaggage
  {
    void scan(IntIterator iterator, DoubleScanner scanner);

    DoubleStream stream(IntIterator iterator);
  }
}
