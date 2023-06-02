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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.Cacheable;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.Cursor;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnCapabilities;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.function.IntFunction;

public class FilterContext implements Closeable
{
  private static final Logger LOG = new Logger(Filters.class);

  private DimFilter root;

  protected final BitmapIndexSelector selector;
  protected final BitmapFactory factory;

  private ImmutableBitmap baseBitmap;
  private DimFilter matcher;

  private final Map<String, IntFunction> attached;    // vc from filter (like lucene)
  private final Map<Object, ImmutableBitmap> possibles;
  private final Map<String, ImmutableBitmap> ranges;  // range on dictionary

  public FilterContext(BitmapIndexSelector selector)
  {
    this.selector = Preconditions.checkNotNull(selector);
    this.factory = Preconditions.checkNotNull(selector.getBitmapFactory());
    this.attached = Maps.newHashMap();
    this.possibles = Maps.newHashMap();
    this.ranges = Maps.newHashMap();
  }

  public FilterContext root(DimFilter filter)
  {
    root = filter;
    return this;
  }

  public boolean isRoot(DimFilter current)
  {
    return root == current;
  }

  public BitmapHolder createBitmap(DimFilter filter)
  {
    return createBitmap(filter, () -> Filters.leafToBitmap(filter, this));
  }

  public BitmapHolder createBitmap(Cacheable filter, Supplier<BitmapHolder> populator)
  {
    long start = System.currentTimeMillis();
    BitmapHolder holder = populator.get();
    if (holder != null && LOG.isDebugEnabled()) {
      long elapsed = System.currentTimeMillis() - start;
      LOG.debug("%s : %,d / %,d (%,d msec)", filter, holder.bitmap().size(), numRows(), elapsed);
    }
    return holder;
  }

  public void cache(Cacheable filter, BitmapHolder holder) {}

  public QueryableIndex internal()
  {
    return selector.internal();
  }

  public BitmapIndexSelector indexSelector()
  {
    return selector;
  }

  public ColumnCapabilities getCapabilities(String dimension)
  {
    return selector.getCapabilities(dimension);
  }

  public BitmapFactory bitmapFactory()
  {
    return factory;
  }

  public ImmutableBitmap baseBitmap()
  {
    return baseBitmap;
  }

  public DimFilter matcher()
  {
    return matcher;
  }

  public IntIterator rowIterator()
  {
    return baseBitmap == null ? null : baseBitmap.iterator();
  }

  public void andBaseBitmap(ImmutableBitmap bitmap)
  {
    baseBitmap = baseBitmap == null ? bitmap : DimFilters.intersection(factory, baseBitmap, bitmap);
  }

  public void matcher(DimFilter matcher)
  {
    this.matcher = matcher;
  }

  public int difference(ImmutableBitmap bitmap)
  {
    return baseBitmap == null ? selector.getNumRows() - bitmap.size() :
           baseBitmap.isEmpty() ? 0 : DimFilters.difference(factory, baseBitmap, bitmap, selector.getNumRows()).size();
  }

  public float selectivity()
  {
    return baseBitmap == null ? 1f : baseBitmap.isEmpty() ? 0f : numRows() / (float) baseBitmap.size();
  }

  public void dictionaryRef(String dimension, ImmutableBitmap range)
  {
    ranges.compute(dimension, (k, prev) -> prev == null ? range : factory.intersection(Arrays.asList(prev, range)));
  }

  public ImmutableBitmap dictionaryRef(String dimension)
  {
    return ranges.get(dimension);
  }

  public IntIterator dictionaryIterator(String dimension)
  {
    final ImmutableBitmap range = dictionaryRef(dimension);
    return range == null ? null : range.iterator();
  }

  public int dictionaryRange(String dimension, int cardinality)
  {
    final ImmutableBitmap range = dictionaryRef(dimension);
    return range == null ? cardinality : range.size();
  }

  public void attach(String column, IntFunction attachment)
  {
    attached.put(column, attachment);
  }

  public void attach(Object column, ImmutableBitmap attachment)
  {
    possibles.put(column, attachment);
  }

  public IntFunction attachmentOf(String name)
  {
    return attached.get(name);
  }

  public Map<Object, ImmutableBitmap> getAmbiguous()
  {
    return possibles;
  }

  public int numRows()
  {
    return selector.getNumRows();
  }

  public int targetNumRows()
  {
    return baseBitmap == null ? selector.getNumRows() : baseBitmap.size();
  }

  public boolean bitmapFiltered()
  {
    return baseBitmap != null && baseBitmap.size() < selector.getNumRows();
  }

  public boolean isAll(ImmutableBitmap bitmap)
  {
    return bitmap.size() == selector.getNumRows();
  }

  public Map<String, ImmutableBitmap> ranges()
  {
    return ranges;
  }

  @Override
  public void close()
  {
    selector.close();
  }

  public MatcherContext matcher(Cursor cursor)
  {
    return new MatcherContext(attached, possibles)
    {
      @Override
      public int offset()
      {
        return cursor.offset();
      }
    };
  }
}
