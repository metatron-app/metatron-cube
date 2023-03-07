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
import com.google.common.base.Strings;
import io.druid.common.guava.BinaryRef;
import io.druid.data.UTF8Bytes;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.ColumnCapabilities;

/**
 */
public class PrefixFilter extends DimensionPredicateFilter
{
  private final String prefix;

  public static PrefixFilter of(String dimension, String prefix)
  {
    UTF8Bytes bytes = UTF8Bytes.of(prefix);
    return new PrefixFilter(dimension, prefix, s -> s != null && s.startsWith(prefix), b -> b.startsWith(bytes));
  }

  private PrefixFilter(String dimension, String prefix, Predicate<String> predicate1, Predicate<BinaryRef> predicate2)
  {
    super(dimension, predicate1, predicate2, null);
    this.prefix = prefix;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    final ColumnCapabilities capabilities = selector.getCapabilities(dimension);
    if (capabilities == null) {
      return BitmapHolder.exact(selector.createBoolean(Strings.isNullOrEmpty(prefix)));
    }
    if (capabilities.isDictionaryEncoded()) {
      return BitmapHolder.exact(
          Filters.matchDictionary(dimension, context, new DictionaryMatcher.WithPrefix(prefix, predicate1))
      );
    }
    return null;
  }

  @Override
  public String toString()
  {
    return "PrefixFilter{" +
           "dimension='" + dimension + '\'' +
           ", prefix='" + prefix + '\'' +
           '}';
  }
}
