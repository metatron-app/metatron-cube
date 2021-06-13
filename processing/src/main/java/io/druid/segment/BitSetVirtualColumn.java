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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.BufferRef;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.BitSet;

/**
 */
public class BitSetVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x09;

  public static VirtualColumn implicit(String metric)
  {
    return new BitSetVirtualColumn(metric, metric);
  }

  private final String columnName;
  private final String outputName;

  @JsonCreator
  public BitSetVirtualColumn(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("outputName") String outputName
  )
  {
    this.columnName = Preconditions.checkNotNull(columnName, "columnName should not be null");
    this.outputName = outputName == null ? columnName : outputName;
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    if (column.equals(columnName)) {
      return types.resolve(columnName);
    }
    final int index = column.indexOf('.', outputName.length());
    final Integer access = Ints.tryParse(column.substring(index + 1));
    if (access == null || access < 0) {
      throw new IAE("expects index attached in %s", column);
    }
    return types.resolve(columnName).isBitSet() ? ValueDesc.BOOLEAN : null;
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    final int index = column.indexOf('.', outputName.length());
    if (index < 0) {
      return factory.makeObjectColumnSelector(columnName);
    }
    final Integer access = Ints.tryParse(column.substring(index + 1));
    if (access == null || access < 0) {
      throw new IAE("expects index attached in %s", column);
    }
    final ValueDesc indexed = factory.resolve(columnName);
    if (indexed.isBitSet()) {
      final ObjectColumnSelector selector = factory.makeObjectColumnSelector(columnName);
      if (selector instanceof ObjectColumnSelector.WithRawAccess) {
        final ObjectColumnSelector.WithRawAccess rawAccess = (ObjectColumnSelector.WithRawAccess) selector;
        return ColumnSelectors.asSelector(ValueDesc.BOOLEAN, () -> {
          final BufferRef ref = rawAccess.getAsRef();
          return ref.remaining() == 0 ? null : ref.get(access);
        });
      }
      return ColumnSelectors.asSelector(ValueDesc.BOOLEAN, () -> {
        final BitSet bitSet = (BitSet) selector.get();
        return bitSet == null ? null : bitSet.get(access);
      });
    }
    return null;
  }

  @Override
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (selector == null) {
      if (extractionFn == null) {
        return NullDimensionSelector.STRING_TYPE;
      } else {
        return new ColumnSelectors.SingleValuedDimensionSelector(extractionFn.apply(null));
      }
    } else if (extractionFn != null) {
      return VirtualColumns.mimicDimensionSelector(ValueDesc.STRING, () -> extractionFn.apply(selector.get()));
    }
    return new DimensionSelector()
    {
      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public IndexedInts getRow()
      {
        final BitSet bitSet = (BitSet) selector.get();
        if (bitSet == null) {
          return EmptyIndexedInts.EMPTY_INDEXED_INTS;
        }
        final int cardinality = bitSet.cardinality();
        if (cardinality == 1) {
          return new IndexedInts.SingleValued()
          {
            @Override
            protected int get()
            {
              return bitSet.nextSetBit(0);
            }
          };
        }
        return new IndexedInts.PreferIterator()
        {
          @Override
          public it.unimi.dsi.fastutil.ints.IntIterator iterator()
          {
            return new IntIterator()
            {
              private int index = bitSet.nextSetBit(0);

              @Override
              public int nextInt()
              {
                final int x = index;
                index = bitSet.nextSetBit(x + 1);
                return x;
              }

              @Override
              public boolean hasNext()
              {
                return index >= 0;
              }
            };
          }

          @Override
          public int size()
          {
            return cardinality;
          }

          @Override
          public int get(int index)
          {
            int x = bitSet.nextSetBit(0);
            for (int i = 0; i < index; i++) {
              x = bitSet.nextSetBit(x + 1);
            }
            return x;
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return -1;
      }

      @Override
      public Comparable lookupName(int id)
      {
        return String.valueOf(id);
      }

      @Override
      public int lookupId(Comparable name)
      {
        return Integer.parseInt((String) name);
      }

      @Override
      public boolean withSortedDictionary()
      {
        return false;
      }
    };
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new BitSetVirtualColumn(columnName, outputName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(columnName, outputName);
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BitSetVirtualColumn)) {
      return false;
    }

    BitSetVirtualColumn that = (BitSetVirtualColumn) o;

    if (!columnName.equals(that.columnName)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = columnName.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "BitSetVirtualColumn{" +
           "columnName='" + columnName + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
