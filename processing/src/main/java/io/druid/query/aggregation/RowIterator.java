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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public abstract class RowIterator<T extends RowCollector>
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

  protected static final BytesRef NULL_REF = BytesRef.of(new byte[]{NULL_VALUE});

  protected final ValueMatcher predicate;
  protected final List<DimensionSelector> selectorList;
  protected final int[][] groupings;
  protected final boolean byRow;

  protected final Consumer<T> consumer;

  public RowIterator(
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
    this.consumer = toConsumer(selectorList);
  }

  // class of collector
  protected abstract Class<T> collectorClass();

  private Consumer<T> toConsumer(List<DimensionSelector> selectors)
  {
    if (selectors.isEmpty()) {
      return collector -> collector.collect(new BytesRef[] {NULL_REF}, NULL_REF);
    }
    final BytesOutputStream buffer = BUFFERS.get();
    final BytesRef[] values = new BytesRef[selectors.size()];
    if (Iterables.all(selectors, s -> s instanceof SingleValued)) {
      if (selectors.size() == 1) {
        Supplier<byte[]> supplier = toSingleValueSupplier(selectors.get(0));
        return collector -> collector.collect(values, values[0] = BytesRef.of(supplier.get()));
      } else if (groupings == null || groupings.length == 0) {
        final List<Supplier<byte[]>> suppliers = GuavaUtils.transform(selectors, s -> toSingleValueSupplier(s));
        return collector -> {
          buffer.clear();
          buffer.write(suppliers.get(0).get());
          values[0] = buffer.asRef();
          for (int i = 1; i < suppliers.size(); i++) {
            final int prev = buffer.size();
            buffer.write(COLUMN_SEPARATOR);
            buffer.write(suppliers.get(i).get());
            values[i] = buffer.asRef(prev + 1);
          }
          collector.collect(values, buffer.asRef());
        };
      }
      // todo: test case for this ?
    }
    if (byRow) {
      final List<Supplier<byte[]>> svs = GuavaUtils.transform(selectors, s -> toSingleValueSupplier(s));
      final List<IntFunction<byte[]>> lookups = GuavaUtils.transform(selectors, s -> toLookupFunction(s));
      final byte[][][] source = new byte[selectorList.size()][][];
      final Supplier<byte[][][]> supplier = () -> {
        for (int i = 0; i < source.length; i++) {
          source[i] = _toValues(selectors.get(i).getRow(), svs.get(i), lookups.get(i), groupings == null);
        }
        return source;
      };
      if (groupings == null) {
        return collector -> collectConcat(supplier.get(), values, collector, buffer.clear());
      } else {
        return collector -> collectExploded(supplier.get(), values, groupings, collector, buffer.clear());
      }
    }
    return collector -> hashValues(collector);
  }

  // called only when size == 1
  private static Supplier<byte[]> toSingleValueSupplier(DimensionSelector selector)
  {
    if (selector instanceof ObjectColumnSelector) {
      ValueDesc type = ((ObjectColumnSelector) selector).type();
      ValueType serializer = type.isDimension() || type.isMultiValued() ? ValueType.STRING : type.type();
      return () -> serializer.toBytes(((ObjectColumnSelector) selector).get());
    }
    if (selector instanceof WithRawAccess) {
      return () -> ((WithRawAccess) selector).getAsRaw(selector.getRow().get(0));
    } else {
      return () -> StringUtils.toUtf8WithNullToEmpty(selector.lookupName(selector.getRow().get(0)));
    }
  }

  @Nullable
  private static IntFunction<byte[]> toLookupFunction(DimensionSelector selector)
  {
    if (selector instanceof WithRawAccess) {
      return x -> ((WithRawAccess) selector).getAsRaw(x);
    } else {
      return x -> StringUtils.toUtf8WithNullToEmpty(selector.lookupName(x));
    }
  }

  private static byte[] lookup(DimensionSelector selector, int x)
  {
    if (selector instanceof WithRawAccess) {
      return ((WithRawAccess) selector).getAsRaw(x);
    } else {
      return StringUtils.toUtf8WithNullToEmpty(selector.lookupName(x));
    }
  }

  @Nullable
  private static byte[][] _toValues(IndexedInts row, Supplier<byte[]> sv, IntFunction<byte[]> lookup, boolean sort)
  {
    final int length = row.size();
    if (length == 1) {
      return new byte[][]{sv.get()};
    }
    final byte[][] mvs = new byte[length][];
    for (int i = 0; i < mvs.length; i++) {
      mvs[i] = lookup.apply(row.get(i));
    }
    // Values need to be sorted to ensure consistent multi-value ordering across different segments
    if (sort) {
      Arrays.sort(mvs, UTF8Bytes.COMPARATOR_NF);
    }
    return mvs;
  }

  // concat multi-valued dimension
  private static void collectConcat(
      byte[][][] source,
      BytesRef[] values,
      RowCollector collector,
      BytesOutputStream buffer
  )
  {
    buffer.clear();
    buffer.write(source[0], MULTIVALUE_SEPARATOR);
    values[0] = buffer.asRef();
    for (int i = 1; i < source.length; i++) {
      final int prev = buffer.size();
      buffer.write(COLUMN_SEPARATOR);
      buffer.write(source[i], MULTIVALUE_SEPARATOR);
      values[i] = buffer.asRef(prev + 1);
    }
    collector.collect(values, buffer.asRef());
  }

  // mimics group-by like population of multi-valued dimension
  private static void collectExploded(
      final byte[][][] source,
      final BytesRef[] values,
      final int[][] groupings,
      final RowCollector collector,
      final BytesOutputStream buffer
  )
  {
    if (groupings.length == 0) {
      collectExploded(source, values, 0, collector, buffer);
    } else {
      for (int[] grouping : groupings) {
        final byte[][][] copy = new byte[source.length][][];
        for (int index : grouping) {
          copy[index] = source[index];
        }
        collectExploded(copy, values, 0, collector, buffer);
      }
    }
  }

  private static void collectExploded(
      final byte[][][] source,
      final BytesRef[] values,
      final int index,
      final RowCollector collector,
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
    if (source[index] == null) {
      buffer.write(NULL_VALUE);
      values[index] = NULL_REF;
      collectExploded(source, values, index + 1, collector, buffer);
    } else if (source[index].length == 1) {
      buffer.write(source[index][0]);
      values[index] = BytesRef.of(source[index][0]);
      collectExploded(source, values, index + 1, collector, buffer);
    } else {
      for (byte[] element : source[index]) {
        buffer.write(element);
        values[index] = BytesRef.of(element);
        collectExploded(source, values, index + 1, collector, buffer);
        buffer.position(mark);
      }
    }
    buffer.position(mark);
  }

  private void hashValues(final RowCollector collector)
  {
    final BytesRef[] values = new BytesRef[1];
    for (final DimensionSelector selector : selectorList) {
      final IndexedInts row = selector.getRow();
      final int size = row.size();
      for (int i = 0; i < size; i++) {
        collector.collect(values, values[0] = BytesRef.of(lookup(selector, row.get(i))));
      }
    }
  }
}
