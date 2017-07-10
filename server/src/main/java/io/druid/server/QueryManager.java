/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.Progressing;
import io.druid.query.Query;
import io.druid.query.QueryWatcher;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QueryManager implements QueryWatcher
{

  private final SetMultimap<String, ListenableFuture> queries;
  private final SetMultimap<String, String> queryDatasources;

  public QueryManager()
  {
    this.queries = Multimaps.synchronizedSetMultimap(
        HashMultimap.<String, ListenableFuture>create()
    );
    this.queryDatasources = Multimaps.synchronizedSetMultimap(
        HashMultimap.<String, String>create()
    );
  }

  public boolean cancelQuery(String id)
  {
    queryDatasources.removeAll(id);
    Set<ListenableFuture> futures = queries.removeAll(id);
    boolean success = true;
    for (ListenableFuture future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  public void registerQuery(Query query, final ListenableFuture future)
  {
    final String id = query.getId();
    final List<String> datasources = query.getDataSource().getNames();
    queries.put(id, future);
    queryDatasources.putAll(id, datasources);
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            queries.remove(id, future);
            for (String datasource : datasources) {
              queryDatasources.remove(id, datasource);
            }
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }

  public Set<String> getQueryDatasources(final String queryId)
  {
    return queryDatasources.get(queryId);
  }

  public float progress(String queryId) throws IOException, InterruptedException
  {
    Set<ListenableFuture> futures = queries.get(queryId);
    if (futures != null && futures.size() == 1) {
      ListenableFuture future = Iterables.getFirst(futures, null);
      if (future instanceof Progressing) {
        return ((Progressing)future).progress();
      }
    }
    return -1;
  }
}
