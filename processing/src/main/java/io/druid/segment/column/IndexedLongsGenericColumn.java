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

package io.druid.segment.column;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.IndexedLongs;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 */
public final class IndexedLongsGenericColumn extends GenericColumn.LongType
{
  private final IndexedLongs column;
  private final CompressionStrategy compressionType;
  private final ImmutableBitmap nulls;

  private int from = -1;
  private int to = -1;
  private final long[] buffered;

  public IndexedLongsGenericColumn(IndexedLongs column, CompressionStrategy compressionType, ImmutableBitmap nulls)
  {
    this.column = column;
    this.compressionType = compressionType;
    this.nulls = nulls;
    this.buffered = new long[DEFAULT_PREFETCH];
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return compressionType;
  }

  @Override
  public int size()
  {
    return column.size();
  }

  @Override
  public Long getValue(final int rowNum)
  {
    return nulls.get(rowNum) ? null : _getValue(rowNum);
  }

  private long _getValue(int rowNum)
  {
    if (rowNum >= from && rowNum < to) {
      return buffered[rowNum - from];
    }
    final int loaded = column.fill(rowNum, buffered);
    from = rowNum;
    to = rowNum + loaded;
    return buffered[0];
  }

  @Override
  public boolean getLong(int rowNum, MutableLong handover)
  {
    if (nulls.get(rowNum)) {
      return false;
    } else {
      handover.setValue(_getValue(rowNum));
      return true;
    }
  }

  @Override
  public void scan(IntIterator include, LongScanner scanner)
  {
    column.scan(include, scanner);
  }

  @Override
  public LongStream stream(IntIterator iterator)
  {
    return column.stream(IntIterators.filter(iterator, nulls, size()));
  }

  @Override
  public ImmutableBitmap getNulls()
  {
    return nulls;
  }

  @Override
  public void close() throws IOException
  {
    column.close();
  }
}
