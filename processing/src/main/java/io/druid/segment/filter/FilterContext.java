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
import io.druid.query.filter.Filter;
import io.druid.segment.QueryableIndex;

import java.io.Closeable;
import java.util.Map;
import java.util.function.IntFunction;

public class FilterContext implements Closeable
{
  private static final Logger LOG = new Logger(Filters.class);

  protected final BitmapIndexSelector selector;
  protected final BitmapFactory factory;
  protected ImmutableBitmap baseBitmap;
  protected Map<String, IntFunction> attached;    // vc from filter (like lucene)

  private Filter matcher;
  private boolean fullScan;

  public FilterContext(BitmapIndexSelector selector)
  {
    this.selector = Preconditions.checkNotNull(selector);
    this.factory = Preconditions.checkNotNull(selector.getBitmapFactory());
    this.attached = Maps.newHashMap();
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

  public QueryableIndex internal()
  {
    return selector.internal();
  }

  public BitmapIndexSelector indexSelector()
  {
    return selector;
  }

  public BitmapFactory bitmapFactory()
  {
    return factory;
  }

  public ImmutableBitmap getBaseBitmap()
  {
    return baseBitmap;
  }

  public Filter getMatcher()
  {
    return matcher;
  }

  public boolean isFullScan()
  {
    return fullScan;
  }

  public void andBaseBitmap(ImmutableBitmap newBaseBitmap)
  {
    baseBitmap = baseBitmap == null ? newBaseBitmap : DimFilters.intersection(factory, baseBitmap, newBaseBitmap);
  }

  public void attach(String column, IntFunction attachment)
  {
    attached.put(column, attachment);
  }

  public IntFunction getAttachment(String name)
  {
    return attached.get(name);
  }

  public int numRows()
  {
    return selector.getNumRows();
  }

  public int targetNumRows()
  {
    return baseBitmap == null ? selector.getNumRows() : baseBitmap.size();
  }

  public boolean isAll(ImmutableBitmap bitmap)
  {
    return bitmap.size() == targetNumRows();
  }

  public void prepared(ImmutableBitmap baseBitmap, Filter matcher, boolean fullscan)
  {
    this.baseBitmap = baseBitmap;
    this.matcher = matcher;
    this.fullScan = fullscan;
  }

  @Override
  public void close()
  {
    selector.close();
  }
}
