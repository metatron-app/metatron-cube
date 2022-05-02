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

package io.druid.segment.column;

import com.google.common.collect.Lists;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.util.Arrays;
import java.util.List;

/**
 */
public interface LuceneIndex extends SecondaryIndex<Query>
{
  TopDocs query(Query query);

  IndexSearcher searcher();

  @Override
  default BitmapHolder compare(String op, boolean withNot, String column, Comparable constant, FilterContext context)
  {
    switch (op) {
      case "<":
        return filterFor(withNot ? Lucenes.atLeast(column, constant) : Lucenes.lessThan(column, constant), context);
      case ">":
        return filterFor(withNot ? Lucenes.atMost(column, constant) : Lucenes.greaterThan(column, constant), context);
      case "<=":
        return filterFor(withNot ? Lucenes.greaterThan(column, constant) : Lucenes.atMost(column, constant), context);
      case ">=":
        return filterFor(withNot ? Lucenes.lessThan(column, constant) : Lucenes.atLeast(column, constant), context);
      case "==":
        if (withNot) {
          return BitmapHolder.union(
              context.bitmapFactory(),
              Arrays.asList(
                  filterFor(Lucenes.lessThan(column, constant), context),
                  filterFor(Lucenes.greaterThan(column, constant), context)
              )
          );
        }
        return filterFor(Lucenes.point(column, constant), context);
    }
    return null;
  }

  @Override
  default BitmapHolder between(
      boolean withNot, String column, Comparable lower, Comparable upper, FilterContext context
  )
  {
    if (withNot) {
      return BitmapHolder.union(
          context.bitmapFactory(),
          Arrays.asList(
              filterFor(Lucenes.lessThan(column, lower), context),
              filterFor(Lucenes.greaterThan(column, upper), context)
          )
      );
    }
    return filterFor(Lucenes.closed(column, lower, upper), context);
  }

  @Override
  default BitmapHolder in(String column, List<Comparable> values, FilterContext context)
  {
    final List<BitmapHolder> holders = Lists.newArrayList();
    for (Comparable value : values) {
      holders.add(filterFor(Lucenes.point(column, value), context));
    }
    return BitmapHolder.union(context.bitmapFactory(), holders);
  }
}
