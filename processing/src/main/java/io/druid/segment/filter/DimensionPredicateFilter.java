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
import com.google.common.base.Predicate;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;

/**
 */
public class DimensionPredicateFilter implements Filter
{
  protected final String dimension;
  protected final ExtractionFn extractionFn;
  protected final Predicate<String> predicate;

  public DimensionPredicateFilter(
      final String dimension,
      final Predicate<String> predicate,
      final ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.predicate = Preconditions.checkNotNull(predicate, "predicate");
    this.extractionFn = extractionFn;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    return BitmapHolder.exact(Filters.matchDictionary(dimension, context, toMatcher(combine(predicate, extractionFn))));
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(factory, dimension, combine(predicate, extractionFn));
  }

  protected Filters.DictionaryMatcher<String> toMatcher(Predicate<String> predicate)
  {
    return v -> predicate.apply(v);
  }

  private static Predicate<String> combine(Predicate<String> predicate, ExtractionFn extractionFn)
  {
    return extractionFn == null ? predicate : s -> predicate.apply(extractionFn.apply(s));
  }
}
