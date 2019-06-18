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

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.Filter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import org.mozilla.javascript.Context;

import java.util.EnumSet;

public class JavaScriptFilter implements Filter
{
  private final String dimension;
  private final JavaScriptDimFilter.JavaScriptPredicate predicate;

  public JavaScriptFilter(
      String dimension,
      JavaScriptDimFilter.JavaScriptPredicate predicate
  )
  {
    this.dimension = dimension;
    this.predicate = predicate;
  }

  @Override
  public ImmutableBitmap getValueBitmap(final BitmapIndexSelector selector)
  {
    final Context cx = Context.enter();
    try {
      final Predicate<String> contextualPredicate = new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return predicate.applyInContext(cx, input);
        }
      };

      return Filters.matchPredicateValues(dimension, selector, contextualPredicate);
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(
      final BitmapIndexSelector selector,
      EnumSet<BitmapType> using,
      ImmutableBitmap baseBitmap
  )
  {
    final Context cx = Context.enter();
    try {
      final Predicate<String> contextualPredicate = new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return predicate.applyInContext(cx, input);
        }
      };

      return Filters.matchPredicate(dimension, selector, contextualPredicate);
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(factory, dimension, predicate);
  }

  @Override
  public String toString()
  {
    return "JavaScriptFilter{" +
           "dimension='" + dimension + '\'' +
           ", predicate=" + predicate +
           '}';
  }
}
