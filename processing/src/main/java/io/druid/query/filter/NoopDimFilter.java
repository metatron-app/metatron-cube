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

package io.druid.query.filter;

import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.MatcherContext;

import java.util.Set;

/**
 */
public class NoopDimFilter implements DimFilter
{
  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.NOOP_CACHE_ID);
  }

  @Override
  public void addDependent(Set<String> handler)
  {
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {

      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        throw new UnsupportedOperationException("getBitmapIndex");
      }

      @Override
      public ValueMatcher makeMatcher(
          MatcherContext context,
          ColumnSelectorFactory factory
      )
      {
        return BooleanValueMatcher.of(true);
      }
    };
  }
}
