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

import io.druid.common.guava.BufferRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.IntIntConsumer;
import io.druid.segment.column.IntScanner;
import io.druid.segment.column.RowSupplier;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import org.apache.commons.lang.mutable.MutableInt;
import org.roaringbitmap.IntIterator;

import java.util.BitSet;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  IndexedInts getRow();

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * @return the value cardinality
   */
  int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) =&gt; [0 1]
   * getRow(1) =&gt; [0]
   * getRow(2) =&gt; [0 1]
   * getRow(3) =&gt; [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) =&gt; A
   * lookupName(1) =&gt; B
   *
   * @param id id to lookup the field name for
   * @return the field name for the given id
   */
  Object lookupName(int id);

  /**
   * returns without dimension or multivalue prefix
   *
   * @return type
   */
  ValueDesc type();

  /**
   * The ID is the int id value of the field.
   *
   * @param name field name to look up the id for
   * @return the id for the given field name
   */
  int lookupId(Object name);

  default boolean withSortedDictionary()
  {
    return false;
  }

  interface WithDictionary extends DimensionSelector
  {
    boolean isUnique();

    Stream<String> values();
  }

  interface SingleValued extends DimensionSelector
  {
  }

  // aka. dictionary without extract function
  interface WithRawAccess extends DimensionSelector.WithDictionary
  {
    Dictionary getDictionary();

    byte[] getAsRaw(int id);

    BufferRef getAsRef(int id);

    default UTF8Bytes getAsWrap(int id)
    {
      return UTF8Bytes.of(getAsRaw(id));
    }
  }

  // aka. dictionary with single value without extract function
  interface Scannable extends SingleValued, WithRawAccess
  {
    RowSupplier rows();

    void scan(IntIterator iterator, IntScanner scanner);    // rowID to dictId

    void consume(IntIterator iterator, IntIntConsumer consumer);    // rowID to dictId

    void scan(Tools.Scanner scanner);         // on current

    <T> T apply(Tools.Function<T> function);  // on current

    // no duplication (if it's needed, use IntList)
    default void scan(IntIterator iterator, Tools.Scanner scanner)
    {
      getDictionary().scan(BitSets.iterator(collect(iterator)), scanner);
    }

    default BitSet collect(IntIterator iterator)
    {
      BitSet dictIds = new BitSet(getDictionary().size());
      consume(iterator, (x, v) -> dictIds.set(v));
      return dictIds;
    }
  }

  class Delegated implements DimensionSelector
  {
    protected final DimensionSelector delegate;

    public Delegated(DimensionSelector delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public IndexedInts getRow()
    {
      return delegate.getRow();
    }

    @Override
    public int getValueCardinality()
    {
      return delegate.getValueCardinality();
    }

    @Override
    public Object lookupName(int id)
    {
      return delegate.lookupName(id);
    }

    @Override
    public ValueDesc type()
    {
      return delegate.type();
    }

    @Override
    public int lookupId(Object name)
    {
      return delegate.lookupId(name);
    }

    @Override
    public boolean withSortedDictionary()
    {
      return delegate.withSortedDictionary();
    }
  }

  interface Mimic extends DimensionSelector, ObjectColumnSelector
  {
  }

  static IntFunction[] convert(
      ScanContext context,
      String[] dimensions,
      ValueDesc[] dimensionTypes,
      DimensionSelector[] selectors,
      boolean useRawUTF8
  )
  {
    MutableInt available = new MutableInt(ColumnSelectors.THRESHOLD);
    IntFunction[] rawAccess = new IntFunction[selectors.length];
    for (int i = 0; i < rawAccess.length; i++) {
      DimensionSelector selector = selectors[i];
      if (selector instanceof Scannable) {
        IntFunction cache = ColumnSelectors.toDictionaryCache((Scannable) selector, context, dimensions[i], available);
        if (cache != null) {
          if (useRawUTF8 && (dimensionTypes == null || dimensionTypes[i].isStringOrDimension())) {
            rawAccess[i] = cache;
          } else {
            rawAccess[i] = ix -> StringUtils.emptyToNull(String.valueOf(cache.apply(ix)));
          }
        }
      }
      if (rawAccess[i] == null) {
        rawAccess[i] = ix -> StringUtils.emptyToNull(selector.lookupName(ix));
      }
    }
    return rawAccess;
  }

  static DimensionSelector asSelector(
      DictionaryEncodedColumn encoded, ExtractionFn extractionFn, ScanContext context, Offset offset)
  {
    return asSelector(encoded, extractionFn, context, offset, Function.identity());
  }

  static DimensionSelector asSelector(
      DictionaryEncodedColumn encoded,
      ExtractionFn extractionFn,
      ScanContext context,
      Offset offset,
      Function<IndexedInts, IndexedInts> indexer
  )
  {
    final Dictionary<String> dictionary = encoded.dictionary().dedicated();
    if (encoded.hasMultipleValues()) {
      if (extractionFn != null) {
        return new DimensionSelector.WithDictionary()
        {
          @Override
          public boolean isUnique()
          {
            return false;
          }

          @Override
          public Stream<String> values()
          {
            return dictionary.stream().map(v -> extractionFn.apply(v));
          }

          @Override
          public IndexedInts getRow()
          {
            return indexer.apply(encoded.getMultiValueRow(offset.get()));
          }

          @Override
          public int getValueCardinality()
          {
            return dictionary.size();
          }

          @Override
          public Object lookupName(int id)
          {
            return extractionFn.apply(dictionary.get(id));
          }

          @Override
          public ValueDesc type()
          {
            return ValueDesc.STRING;
          }

          @Override
          public int lookupId(Object name)
          {
            throw new UnsupportedOperationException(
                "cannot perform lookup when applying an extraction function"
            );
          }

          @Override
          public boolean withSortedDictionary()
          {
            return dictionary.isSorted() && extractionFn.preservesOrdering();
          }
        };
      } else {
        return new DimensionSelector.WithRawAccess()
        {
          @Override
          public boolean isUnique()
          {
            return true;
          }

          @Override
          public Stream<String> values()
          {
            return dictionary.stream();
          }

          @Override
          public Dictionary getDictionary()
          {
            return dictionary;
          }

          @Override
          public IndexedInts getRow()
          {
            return indexer.apply(encoded.getMultiValueRow(offset.get()));
          }

          @Override
          public int getValueCardinality()
          {
            return dictionary.size();
          }

          @Override
          public Object lookupName(int id)
          {
            return dictionary.get(id);
          }

          @Override
          public ValueDesc type()
          {
            return ValueDesc.STRING;
          }

          @Override
          public byte[] getAsRaw(int id)
          {
            return dictionary.getAsRaw(id);
          }

          @Override
          public BufferRef getAsRef(int id)
          {
            return dictionary.getAsRef(id);
          }

          @Override
          public int lookupId(Object name)
          {
            return dictionary.indexOf((String) name);
          }

          @Override
          public boolean withSortedDictionary()
          {
            return dictionary.isSorted();
          }
        };
      }
    } else {
      if (extractionFn != null) {
        final RowSupplier supplier = encoded.row(context.filtering());
        final IndexedInts row = IndexedInts.from(() -> supplier.row(offset.get()));
        return new DimensionSelector.WithDictionary()
        {
          @Override
          public boolean isUnique()
          {
            return false;
          }

          @Override
          public Stream<String> values()
          {
            return dictionary.stream().map(v -> extractionFn.apply(v));
          }

          @Override
          public IndexedInts getRow()
          {
            return indexer.apply(row);
          }

          @Override
          public int getValueCardinality()
          {
            return dictionary.size();
          }

          @Override
          public Object lookupName(int id)
          {
            return extractionFn.apply(dictionary.get(id));
          }

          @Override
          public ValueDesc type()
          {
            return ValueDesc.STRING;
          }

          @Override
          public int lookupId(Object name)
          {
            throw new UnsupportedOperationException(
                "cannot perform lookup when applying an extraction function"
            );
          }

          @Override
          public boolean withSortedDictionary()
          {
            return dictionary.isSorted() && extractionFn.preservesOrdering();
          }
        };
      } else {
        // using an anonymous class is faster than creating a class that stores a copy of the value
        final RowSupplier supplier = encoded.row(context.filtering());
        final IndexedInts row = IndexedInts.from(() -> supplier.row(offset.get()));
        return new DimensionSelector.Scannable()
        {
          @Override
          public boolean isUnique()
          {
            return true;
          }

          @Override
          public Stream<String> values()
          {
            return dictionary.stream();
          }

          @Override
          public RowSupplier rows()
          {
            return supplier;
          }

          @Override
          public Dictionary getDictionary()
          {
            return dictionary;
          }

          @Override
          public IndexedInts getRow()
          {
            return indexer.apply(row);
          }

          @Override
          public int getValueCardinality()
          {
            return dictionary.size();
          }

          @Override
          public Object lookupName(int id)
          {
            return dictionary.get(id);
          }

          @Override
          public ValueDesc type()
          {
            return ValueDesc.STRING;
          }

          @Override
          public byte[] getAsRaw(int id)
          {
            return dictionary.getAsRaw(id);
          }

          @Override
          public BufferRef getAsRef(int id)
          {
            return dictionary.getAsRef(id);
          }

          @Override
          public void scan(Tools.Scanner scanner)
          {
            dictionary.scan(row.get(0), scanner);
          }

          @Override
          public <R> R apply(Tools.Function<R> function)
          {
            return dictionary.apply(row.get(0), function);
          }

          @Override
          public int lookupId(Object name)
          {
            return dictionary.indexOf((String) name);
          }

          @Override
          public boolean withSortedDictionary()
          {
            return dictionary.isSorted();
          }

          @Override
          public void scan(IntIterator iterator, IntScanner scanner)
          {
            encoded.scan(iterator, scanner);
          }

          @Override
          public void consume(IntIterator iterator, IntIntConsumer consumer)
          {
            encoded.consume(iterator, consumer);
          }
        };
      }
    }
  }
}
