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

package io.druid.segment;

import com.google.common.collect.Iterables;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import java.util.Optional;

/**
 */
public interface CursorFactory extends SchemaProvider
{
  default Sequence<Cursor> makeCursors(Query<?> query)
  {
    return makeCursors(query, null);
  }

  default Sequence<Cursor> makeCursors(Query<?> query, SessionCache cache)
  {
    return makeCursors(
        BaseQuery.getDimFilter(query),
        Iterables.getOnlyElement(query.getIntervals()),
        RowResolver.of(this, query),
        Optional.ofNullable(query.getGranularity()).orElse(Granularities.ALL),
        query.isDescending(),
        cache
    );
  }

  Sequence<Cursor> makeCursors(
      DimFilter filter,
      Interval interval,
      RowResolver resolver,
      Granularity granularity,
      boolean descending,
      SessionCache cache
  );
}
