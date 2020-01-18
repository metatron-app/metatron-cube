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

import io.druid.common.guava.BytesRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.ISE;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.UTF8Bytes;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HashAggregator<T extends HashCollector> extends Aggregator.Simple<T>
{
  // aggregator can be shared by multi threads
  private static final ThreadLocal<BytesOutputStream> BUFFERS = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  private static BytesOutputStream getCleared()
  {
    final BytesOutputStream stream = HashAggregator.BUFFERS.get();
    stream.clear();
    return stream;
  }

  private static final byte NULL_VALUE = (byte) 0x00;    // HLL seemingly expects this to be zero
  private static final byte COLUMN_SEPARATOR = (byte) 0x01;
  private static final byte MULTIVALUE_SEPARATOR = (byte) 0x02;

  private static final BytesRef NULL_REF = new BytesRef(new byte[]{NULL_VALUE});

  private final ValueMatcher predicate;
  private final List<DimensionSelector> selectorList;
  private final int[][] groupings;
  private final boolean byRow;

  public HashAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow
  )
  {
    this.predicate = predicate == null ? ValueMatcher.TRUE : predicate;
    this.selectorList = selectorList;
    this.groupings = groupings;
    this.byRow = byRow;
  }

  public HashAggregator(List<DimensionSelector> selectorList, int[][] groupings)
  {
    this(null, selectorList, groupings, true);
  }

  @Override
  public T aggregate(T current)
  {
    if (predicate.matches()) {
      if (current == null) {
        current = newCollector();
      }
      if (selectorList.isEmpty()) {
        current.collect(new Object[0], NULL_REF);
      } else if (byRow) {
        hashRow(selectorList, groupings, current, BUFFERS.get());
      } else {
        hashValues(selectorList, current, BUFFERS.get());
      }
    }
    return current;
  }

  protected T newCollector()
  {
    throw new ISE("implement this");
  }

  public static void hashRow(
      List<DimensionSelector> selectorList,
      int[][] groupings,
      HashCollector collector,
      BytesOutputStream buffer
  )
  {
    buffer.reset();
    final Object[] values = new Object[selectorList.size()];
    for (int i = 0; i < values.length; i++) {
      final DimensionSelector selector = selectorList.get(i);
      values[i] = toValue(selector, selector instanceof DimensionSelector.WithRawAccess, groupings == null);
    }
    if (groupings == null) {
      // concat multi-value
      collect(values, collector, buffer);
    } else {
      // explode multi-value
      populateAndCollect(values, groupings, collector, buffer);
    }
  }

  public static void hashRow(final HashCollector collector, final Object[] values)
  {
    final BytesOutputStream stream = getCleared();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        stream.write(COLUMN_SEPARATOR);
      }
      stream.write(StringUtils.toUtf8WithNullToEmpty(Objects.toString(values[i], null)));
    }
    collector.collect(values, stream.asRef());
  }

  private static Object toValue(final DimensionSelector selector, final boolean rawBytes, final boolean sort)
  {
    final IndexedInts row = selector.getRow();
    // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
    final int length = row.size();
    if (length == 1) {
      return _toValue(selector, row.get(0), rawBytes);
    }
    if (length > 1) {
      final byte[][] multi = new byte[length][];
      for (int j = 0; j < multi.length; ++j) {
        multi[j] = _toValue(selector, row.get(j), rawBytes);
      }
      // Values need to be sorted to ensure consistent multi-value ordering across different segments
      if (sort) {
        Arrays.sort(multi, UTF8Bytes.COMPARATOR_NF);
      }
      return multi;
    }
    return null;
  }

  @Nullable
  private static byte[] _toValue(final DimensionSelector selector, final int id, final boolean rawBytes)
  {
    if (rawBytes) {
      return ((DimensionSelector.WithRawAccess) selector).lookupRaw(id);
    } else {
      return StringUtils.nullableToUtf8(Objects.toString(selector.lookupName(id), null));
    }
  }

  // concat multi-valued dimension
  private static void collect(
      final Object[] values,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    write(values[0], buffer);
    for (int i = 1; i < values.length; i++) {
      buffer.writeByte(COLUMN_SEPARATOR);
      write(values[i], buffer);
    }
    collector.collect(values, buffer.asRef());
  }

  private static void populateAndCollect(
      final Object[] values,
      final int[][] groupings,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    if (groupings.length == 0) {
      populateAndCollect(values, 0, collector, buffer);
    } else {
      for (int[] grouping : groupings) {
        final Object[] copy = new Object[values.length];
        for (int index : grouping) {
          copy[index] = values[index];
        }
        populateAndCollect(copy, 0, collector, buffer);
      }
    }
  }

  // mimics group-by like population of multi-valued dimension
  private static void populateAndCollect(
      final Object[] values,
      final int index,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    if (values.length == index) {
      collector.collect(values, buffer.asRef());
      return;
    }
    if (index > 0) {
      buffer.write(COLUMN_SEPARATOR);
    }
    final int mark = buffer.size();
    final Object value = values[index];
    if (value == null) {
      buffer.write(NULL_VALUE);
      populateAndCollect(values, index + 1, collector, buffer);
    } else if (value instanceof byte[]) {
      buffer.write((byte[]) value);
      populateAndCollect(values, index + 1, collector, buffer);
    } else {
      final byte[][] array = (byte[][]) value;
      for (byte[] element : array) {
        buffer.write(element);
        values[index] = element;
        populateAndCollect(values, index + 1, collector, buffer);
        buffer.reset(mark);
      }
      values[index] = value;
    }
    buffer.reset(mark);
  }

  private static void write(final Object value, final BytesOutputStream buffer)
  {
    if (value == null) {
      buffer.write(NULL_VALUE);
    } else if (value instanceof byte[]) {
      buffer.write((byte[]) value);
    } else {
      final byte[][] values = (byte[][]) value;
      write(values[0], buffer);
      for (int i = 1; i < values.length; i++) {
        buffer.writeByte(MULTIVALUE_SEPARATOR);
        write(values[i], buffer);
      }
    }
  }

  public static void hashValues(
      final List<DimensionSelector> selectors,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    buffer.reset();
    for (final DimensionSelector selector : selectors) {
      final boolean rawAccess = selector instanceof DimensionSelector.WithRawAccess;
      final IndexedInts row = selector.getRow();
      final int size = row.size();
      for (int i = 0; i < size; i++) {
        final byte[] value = _toValue(selector, row.get(i), rawAccess);
        write(value, buffer);
        collector.collect(new Object[] {value}, buffer.asRef());
        buffer.reset();
      }
    }
  }
}
