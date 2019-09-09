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

package io.druid.query.aggregation.cardinality;

import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.UTF8Bytes;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CardinalityAggregator extends Aggregator.Simple<HyperLogLogCollector>
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

  private final ValueMatcher predicate;
  private final List<DimensionSelector> selectorList;
  private final int[][] groupings;
  private final boolean byRow;

  protected static void hashRow(
      List<DimensionSelector> selectorList,
      int[][] groupings,
      HyperLogLogCollector collector,
      BytesOutputStream buffer
  )
  {
    final Object[] values = new Object[selectorList.size()];
    for (int i = 0; i < values.length; i++) {
      final DimensionSelector selector = selectorList.get(i);
      values[i] = toValue(selector, selector instanceof DimensionSelector.WithRawAccess, groupings == null);
    }
    if (groupings == null) {
      collect(values, collector, buffer);
    } else {
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

  private static final byte NULL_VALUE  = (byte) 0x00;    // HLL seemingly expects this to be zero
  private static final byte COLUMN_SEPARATOR = (byte) 0x01;
  private static final byte MULTIVALUE_SEPARATOR = (byte) 0x02;

  // concat multi-valued dimension
  private static void collect(
      final Object[] values,
      final HyperLogLogCollector collector,
      final BytesOutputStream buffer
  )
  {
    write(values[0], buffer);
    for (int i = 1; i < values.length; i++) {
      buffer.writeByte(COLUMN_SEPARATOR);
      write(values[i], buffer);
    }
    collector.add(Murmur3.hash128(buffer.toByteArray()));
    buffer.reset();
  }

  // mimics group-by like population of multi-valued dimension
  private static void populateAndCollect(
      final Object[] values,
      final int index,
      final HyperLogLogCollector collector,
      final BytesOutputStream buffer
  )
  {
    if (values.length == index) {
      collector.add(Murmur3.hash128(buffer.toByteArray()));
      return;
    }
    if (index > 0) {
      buffer.write(COLUMN_SEPARATOR);
    }
    final Object value = values[index];
    if (value == null) {
      buffer.write(NULL_VALUE);
      populateAndCollect(values, index + 1, collector, buffer);
    } else if (value instanceof byte[]) {
      buffer.write((byte[]) value);
      populateAndCollect(values, index + 1, collector, buffer);
    } else {
      final byte[][] array = (byte[][]) value;
      final int mark = buffer.size();
      for (byte[] element : array) {
        buffer.write(element);
        populateAndCollect(values, index + 1, collector, buffer);
        buffer.reset(mark);
      }
    }
    if (index == 0) {
      buffer.reset();
    }
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

  protected static void hashValues(
      final List<DimensionSelector> selectors,
      final HyperLogLogCollector collector,
      final BytesOutputStream buffer
  )
  {
    for (final DimensionSelector selector : selectors) {
      final boolean rawAccess = selector instanceof DimensionSelector.WithRawAccess;
      for (final Integer index : selector.getRow()) {
        write(_toValue(selector, index, rawAccess), buffer);
        collector.add(Murmur3.hash128(buffer.toByteArray()));
        buffer.reset();
      }
    }
  }

  public CardinalityAggregator(
      ValueMatcher predicate,
      List<DimensionSelector> selectorList,
      int[][] groupings,
      boolean byRow
  )
  {
    this.predicate = predicate;
    this.selectorList = selectorList;
    this.groupings = groupings;
    this.byRow = byRow;
  }

  public CardinalityAggregator(List<DimensionSelector> selectorList, boolean byRow)
  {
    this(ValueMatcher.TRUE, selectorList, null, byRow);
  }

  @Override
  public HyperLogLogCollector aggregate(HyperLogLogCollector current)
  {
    if (predicate.matches()) {
      if (current == null) {
        current = HyperLogLogCollector.makeLatestCollector();
      }
      if (selectorList.isEmpty()) {
        current.add(Murmur3.hash128(new byte[]{NULL_VALUE}));
        return current;
      }
      if (byRow) {
        hashRow(selectorList, groupings, current, BUFFERS.get());
      } else {
        hashValues(selectorList, current, BUFFERS.get());
      }
    }
    return current;
  }
}
