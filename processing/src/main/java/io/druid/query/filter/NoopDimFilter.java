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

package io.druid.query.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.BooleanValueMatcher;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class NoopDimFilter implements DimFilter
{
  @Override
  public byte[] getCacheKey()
  {        
    return ByteBuffer.allocate(1).put(DimFilterCacheHelper.NOOP_CACHE_ID).array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public DimFilter withRedirection(Map<String, String> mapping)
  {
    return this;
  }

  @Override
  public void addDependent(Set<String> handler)
  {
  }

  @Override
  public Filter toFilter()
  {
    return new Filter()
    {
      @Override
      public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, EnumSet<BitmapType> using)
      {
        throw new UnsupportedOperationException("getBitmapIndex");
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        return new BooleanValueMatcher(true);
      }

    };
  }
}
