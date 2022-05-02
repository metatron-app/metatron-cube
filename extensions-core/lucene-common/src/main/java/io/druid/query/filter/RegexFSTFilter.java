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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.collections.IntList;
import io.druid.data.TypeResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.AutomatonMatcher;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;

/**
 */
@JsonTypeName("regex.fst")
public class RegexFSTFilter extends RegexDimFilter
{
  private final Supplier<Automaton> automatonSupplier;

  @JsonCreator
  public RegexFSTFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("pattern") String pattern
  )
  {
    super(dimension, pattern, true, null);
    final Supplier<Automaton> supplier = Suppliers.memoize(() -> new RegExp(pattern).toAutomaton());
    this.automatonSupplier = () -> {
      Automaton automaton = new Automaton(0, 0);
      automaton.copy(supplier.get());
      return automaton;
    };
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      private final Filter filter = RegexFSTFilter.super.toFilter(resolver);

      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        final String dimension = getDimension();
        final BitmapIndexSelector selector = context.indexSelector();
        final Column column = selector.getColumn(dimension);
        if (column != null && column.getCapabilities().hasDictionaryFST()) {
          final DictionaryEncodedColumn dictionary = column.getDictionaryEncoding();
          try {
            @SuppressWarnings("unchecked")
            final FST<Long> fst = dictionary.getFST().unwrap(FST.class);
            final IntList matched = AutomatonMatcher.match(automatonSupplier.get(), fst).sort();
            return BitmapHolder.exact(column.getBitmapIndex().union(matched));
          }
          catch (IOException e) {
            // fallback to scanning matcher
          }
        }
        return filter.getBitmapIndex(context);
      }

      @Override
      public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
      {
        return filter.makeMatcher(columnSelectorFactory);
      }
    };
  }
}
