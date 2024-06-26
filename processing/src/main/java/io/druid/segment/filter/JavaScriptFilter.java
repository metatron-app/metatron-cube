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

import io.druid.query.filter.Filter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import org.mozilla.javascript.Context;

public class JavaScriptFilter implements Filter
{
  private final String dimension;
  private final JavaScriptDimFilter.JavaScriptPredicate predicate;

  public JavaScriptFilter(String dimension, JavaScriptDimFilter.JavaScriptPredicate predicate)
  {
    this.dimension = dimension;
    this.predicate = predicate;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    final Context cx = Context.enter();
    try {
      return BitmapHolder.exact(Filters.matchPredicate(dimension, v -> predicate.applyInContext(cx, v), context));
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(factory, dimension, predicate);
  }

  @Override
  public String toString()
  {
    return "JavaScriptFilter{" +
           "dimension='" + dimension + '\'' +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           '}';
  }
}
