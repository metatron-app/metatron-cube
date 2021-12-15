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
import io.druid.data.UTF8Bytes;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.HashCollector.ScanSupport;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.Scannable;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class HashIterator<T extends HashCollector>
{
  // aggregator can be shared by multi threads
  protected static final ThreadLocal<BytesOutputStream> BUFFERS = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  protected static final byte NULL_VALUE = (byte) 0x00;    // HLL seemingly expects this to be zero
  protected static final byte COLUMN_SEPARATOR = (byte) 0x01;
  protected static final byte MULTIVALUE_SEPARATOR = (byte) 0x02;

  protected static final BytesRef NULL_REF = new BytesRef(new byte[]{NULL_VALUE});

  protected final ValueMatcher predicate;
  protected final List<DimensionSelector> selectorList;
  protected final int[][] groupings;
  protected final boolean byRow;
  protected final boolean needValue;

  protected final Consumer<T> consumer;

  public HashIterator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow,
      boolean needValue
  )
  {
    this.predicate = predicate == null ? ValueMatcher.TRUE : predicate;
    this.selectorList = selectorList;
    this.groupings = groupings;
    this.byRow = byRow;
    this.needValue = needValue;
    this.consumer = toConsumer(selectorList);
  }

  protected Consumer<T> toConsumer(List<DimensionSelector> selectorList)
  {
    if (selectorList.isEmpty()) {
      return collector -> collector.collect(new Object[0], NULL_REF);
    } else if (byRow) {
      return collector -> hashRow(collector, BUFFERS.get());
    } else {
      return collector -> hashValues(collector);
    }
  }

  private void hashRow(HashCollector collector, BytesOutputStream buffer)
  {
    buffer.clear();
    final Object[] values = new Object[selectorList.size()];
    for (int i = 0; i < values.length; i++) {
      final DimensionSelector selector = selectorList.get(i);
      values[i] = toValue(selector, selector instanceof DimensionSelector.WithRawAccess, groupings == null);
    }
    if (groupings == null) {
      // concat multi-value
      collectConcat(values, collector, buffer);
    } else {
      // explode multi-value
      collectExploded(values, groupings, collector, buffer);
    }
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
  private static void collectConcat(
      final Object[] values,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    concat(values[0], buffer);
    for (int i = 1; i < values.length; i++) {
      buffer.writeByte(COLUMN_SEPARATOR);
      concat(values[i], buffer);
    }
    collector.collect(values, buffer.asRef());
  }

  private static void concat(final Object value, final BytesOutputStream buffer)
  {
    if (value == null) {
      buffer.write(NULL_VALUE);
    } else if (value instanceof byte[]) {
      buffer.write((byte[]) value);
    } else {
      final byte[][] values = (byte[][]) value;
      concat(values[0], buffer);
      for (int i = 1; i < values.length; i++) {
        buffer.writeByte(MULTIVALUE_SEPARATOR);
        concat(values[i], buffer);
      }
    }
  }

  // mimics group-by like population of multi-valued dimension
  private static void collectExploded(
      final Object[] values,
      final int[][] groupings,
      final HashCollector collector,
      final BytesOutputStream buffer
  )
  {
    if (groupings.length == 0) {
      collectExploded(values, 0, collector, buffer);
    } else {
      for (int[] grouping : groupings) {
        final Object[] copy = new Object[values.length];
        for (int index : grouping) {
          copy[index] = values[index];
        }
        collectExploded(copy, 0, collector, buffer);
      }
    }
  }

  private static void collectExploded(
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
      collectExploded(values, index + 1, collector, buffer);
    } else if (value instanceof byte[]) {
      buffer.write((byte[]) value);
      collectExploded(values, index + 1, collector, buffer);
    } else {
      final byte[][] array = (byte[][]) value;
      for (byte[] element : array) {
        buffer.write(element);
        values[index] = element;
        collectExploded(values, index + 1, collector, buffer);
        buffer.position(mark);
      }
      values[index] = value;
    }
    buffer.position(mark);
  }

  private void hashValues(final HashCollector collector)
  {
    for (final DimensionSelector selector : selectorList) {
      final boolean rawAccess = selector instanceof DimensionSelector.WithRawAccess;
      final IndexedInts row = selector.getRow();
      final int size = row.size();
      for (int i = 0; i < size; i++) {
        final byte[] value = _toValue(selector, row.get(i), rawAccess);
        collector.collect(new Object[] {value}, value == null ? NULL_REF : new BytesRef(value));
      }
    }
  }

  protected static abstract class BaseAggregator<T extends HashCollector> extends HashIterator<T>
  {
    public BaseAggregator(
        ValueMatcher predicate,
        List<DimensionSelector> selectorList,
        int[][] groupings,
        boolean byRow,
        boolean needValue
    )
    {
      super(predicate, selectorList, groupings, byRow, needValue);
    }

    @Override
    protected Consumer<T> toConsumer(List<DimensionSelector> selectorList)
    {
      if (collectorClass() != null && ScanSupport.class.isAssignableFrom(collectorClass())) {
        if (selectorList.size() == 1 && selectorList.get(0) instanceof Scannable) {
          final Scannable selector = (Scannable) selectorList.get(0);
          return collector -> ((ScanSupport) collector).collect(selector);
        }
      }
      return super.toConsumer(selectorList);
    }

    protected Class<T> collectorClass()
    {
      return null;  // class of custom collector
    }
  }
}
