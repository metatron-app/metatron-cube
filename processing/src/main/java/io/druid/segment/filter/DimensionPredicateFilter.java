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
import io.druid.common.guava.BinaryRef;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.ExtractionFns;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.Dictionary;

/**
 */
public class DimensionPredicateFilter implements Filter
{
  protected final String dimension;
  protected final ExtractionFn extractionFn;
  protected final Predicate<String> predicate1;
  protected final Predicate<BinaryRef> predicate2;

  public DimensionPredicateFilter(
      final String dimension,
      final Predicate<String> predicate1,
      final Predicate<BinaryRef> predicate2,
      final ExtractionFn extractionFn
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.predicate1 = Preconditions.checkNotNull(predicate1, "predicate1");
    this.predicate2 = extractionFn == null ? predicate2 : null;
    this.extractionFn = extractionFn;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    Predicate<String> predicate = ExtractionFns.combine(predicate1, extractionFn);
    if (predicate2 == null) {
      return BitmapHolder.exact(Filters.matchDictionary(dimension, context, toMatcher(predicate)));
    }
    return BitmapHolder.exact(Filters.matchDictionary(dimension, context, toRawMatcher(predicate, predicate2)));
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(factory, dimension, ExtractionFns.combine(predicate1, extractionFn));
  }

  protected DictionaryMatcher toMatcher(Predicate<String> predicate)
  {
    return d -> predicate;
  }

  protected DictionaryMatcher.RawSupport toRawMatcher(Predicate<String> predicate1, Predicate<BinaryRef> predicate2)
  {
    return new DictionaryMatcher.RawSupport()
    {
      @Override
      public Predicate<String> matcher(Dictionary<String> dictionary)
      {
        return predicate1;
      }

      @Override
      public Predicate<BinaryRef> rawMatcher(Dictionary<String> dictionary)
      {
        return predicate2;
      }
    };
  }
}
