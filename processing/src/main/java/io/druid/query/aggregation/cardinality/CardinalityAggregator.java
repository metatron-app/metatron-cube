/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.cardinality;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CardinalityAggregator implements Aggregator
{
  private static final String NULL_STRING = "\u0000";

  private final ValueMatcher predicate;
  private final List<DimensionSelector> selectorList;
  private final int[][] groupings;
  private final boolean byRow;

  private static final HashFunction hashFn = Hashing.murmur3_128();
  public static final char SEPARATOR = '\u0001';

  protected static void hashRow(List<DimensionSelector> selectorList, int[][] groupings, HyperLogLogCollector collector)
  {
    final Object[] values = new Object[selectorList.size()];
    for (int i = 0; i < values.length; i++) {
      final DimensionSelector selector = selectorList.get(i);
      final IndexedInts row = selector.getRow();
      // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
      if (row.size() == 1) {
        values[i] = Objects.toString(selector.lookupName(row.get(0)), NULL_STRING);
      } else {
        final String[] multi = new String[row.size()];
        for (int j = 0; j < multi.length; ++j) {
          multi[j] = Objects.toString(selector.lookupName(row.get(j)), NULL_STRING);
        }
        // Values need to be sorted to ensure consistent multi-value ordering across different segments
        Arrays.sort(multi);
        values[i] = multi;
      }
    }
    if (groupings == null) {
      collector.add(makeHash(values).hash().asBytes());
    } else {
      if (groupings.length == 0) {
        populateAndHash(values, 0, collector);
      } else {
        for (int[] grouping : groupings) {
          final Object[] copy = new Object[values.length];
          for (int index : grouping) {
            copy[index] = values[index];
          }
          populateAndHash(copy, 0, collector);
        }
      }
    }
  }

  private static void populateAndHash(final Object[] values, final int index, final HyperLogLogCollector collector)
  {
    if (values.length == index) {
      collector.add(makeHash(values).hash().asBytes());
      return;
    }
    final Object value = values[index];
    if (value == null || value instanceof String) {
      populateAndHash(values, index + 1, collector);
    } else {
      for (String element : (String[]) value) {
        Object[] copy = Arrays.copyOf(values, values.length);
        copy[index] = element;
        populateAndHash(copy, index + 1, collector);
      }
    }
  }
  private static Hasher makeHash(final Object[] values)
  {
    final Hasher hasher = hashFn.newHasher();
    for (int i = 0; i < values.length; i++) {
      if (i != 0) {
        hasher.putByte((byte) 0);
      }
      final Object value = values[i];
      if (value == null) {
        hasher.putUnencodedChars(NULL_STRING);
      } else if (value instanceof String) {
        hasher.putUnencodedChars((String) value);
      } else {
        final String[] multi = (String[]) value;
        for (int j = 0; j < multi.length; j++) {
          if (j != 0) {
            hasher.putChar(SEPARATOR);
          }
          hasher.putUnencodedChars(multi[j]);
        }
      }
    }
    if (values.length == 0) {
      hasher.putUnencodedChars(NULL_STRING);
    }
    return hasher;
  }

  protected static void hashValues(final List<DimensionSelector> selectors, HyperLogLogCollector collector)
  {
    for (final DimensionSelector selector : selectors) {
      for (final Integer index : selector.getRow()) {
        final String value = Objects.toString(selector.lookupName(index), null);
        collector.add(hashFn.hashUnencodedChars(value == null ? NULL_STRING : value).asBytes());
      }
    }
    if (selectors.isEmpty()) {
      collector.add(hashFn.hashUnencodedChars(NULL_STRING).asBytes());
    }
  }

  private HyperLogLogCollector collector;

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
    this.collector = HyperLogLogCollector.makeLatestCollector();
    this.byRow = byRow;
  }

  public CardinalityAggregator(List<DimensionSelector> selectorList, boolean byRow)
  {
    this(ValueMatcher.TRUE, selectorList, null, byRow);
  }

  @Override
  public void aggregate()
  {
    if (predicate.matches()) {
      if (byRow) {
        hashRow(selectorList, groupings, collector);
      } else {
        hashValues(selectorList, collector);
      }
    }
  }

  @Override
  public void reset()
  {
    collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("CardinalityAggregator does not support getDouble()");
  }

  @Override
  public Aggregator clone()
  {
    return new CardinalityAggregator(predicate, selectorList, groupings, byRow);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
