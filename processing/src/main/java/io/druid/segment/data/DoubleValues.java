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

package io.druid.segment.data;

import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.DoubleScanner;
import io.druid.segment.column.IntDoubleConsumer;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

/**
 */
public interface DoubleValues extends Closeable
{
  int size();

  double get(int index);

  int fill(int index, double[] toFill);

  void scan(final IntIterator iterator, final DoubleScanner scanner);

  void consume(final IntIterator iterator, final IntDoubleConsumer consumer);

  default DoubleStream stream(IntIterator iterator)
  {
    if (iterator == null) {
      return IntStream.range(0, size()).mapToDouble(this::get);
    }
    return IntIterators.stream(iterator).mapToDouble(this::get);
  }
}
