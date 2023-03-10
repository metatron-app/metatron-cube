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

package io.druid.query.search;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

/**
 *
 */
public class SearchQueryRunnerFactory extends QueryRunnerFactory.Abstract<Result<SearchResultValue>>
{
  @Inject
  public SearchQueryRunnerFactory(SearchQueryQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new SearchQueryRunner(segment, cache);
  }
}
