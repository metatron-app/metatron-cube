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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.druid.common.Progressing;
import io.druid.query.Query;
import io.druid.query.QueryWatcher;
import io.druid.server.router.TieredBrokerConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class QueryManager implements QueryWatcher, Runnable
{
  private static final long DEFAULT_EXPIRE = 3600000;   // 1hour

  private final Map<String, QueryStatus> queries;
  private final boolean broker;

  @Inject
  public QueryManager(@Named("serviceName") String serviceName)
  {
    this.queries = Maps.newConcurrentMap();
    this.broker = TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME.equals(serviceName);
  }

  public QueryManager()
  {
    this(null);
  }

  public boolean cancelQuery(String id)
  {
    QueryStatus status = queries.get(id);
    if (status != null) {
      return status.cancel();
    }
    return true;
  }

  public void registerQuery(final Query query, final ListenableFuture future)
  {
    final String id = query.getId();
    final List<String> dataSources = query.getDataSource().getNames();
    final QueryStatus status = queries.computeIfAbsent(
        id, new Function<String, QueryStatus>()
        {
          @Override
          public QueryStatus apply(String s)
          {
            return new QueryStatus();
          }
        }
    );
    status.futures.add(future);
    status.dataSources.addAll(dataSources);
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            status.futures.remove(future);
            status.dataSources.removeAll(dataSources);
            if (status.futures.isEmpty() && status.dataSources.isEmpty()) {
              Preconditions.checkArgument(status.end < 0);
              status.end = System.currentTimeMillis();
              if (!broker) {
                queries.remove(id);
              }
            }
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }

  public Set<String> getQueryDatasources(final String queryId)
  {
    QueryStatus status = queries.get(queryId);
    return status == null ? Sets.<String>newHashSet() : Sets.newHashSet(status.dataSources);
  }

  public long getQueryStartTime(final String queryId)
  {
    QueryStatus status = queries.get(queryId);
    return status == null ? -1 : status.start;
  }

  public float progress(String queryId) throws IOException, InterruptedException
  {
    QueryStatus status = queries.get(queryId);
    if (status != null && status.futures != null && status.futures.size() == 1) {
      ListenableFuture future = Iterables.getFirst(status.futures, null);
      if (future instanceof Progressing) {
        return ((Progressing) future).progress();
      }
    }
    return -1;
  }

  private static class QueryStatus
  {
    private final long start = System.currentTimeMillis();
    private final Set<String> dataSources = Sets.newConcurrentHashSet();
    private final Set<ListenableFuture> futures = Sets.newConcurrentHashSet();

    private volatile long end = -1;

    private boolean cancel()
    {
      end = System.currentTimeMillis();

      boolean success = true;
      for (ListenableFuture future : futures) {
        success = success & future.cancel(true);
      }
      futures.clear();
      dataSources.clear();
      return success;
    }

    private boolean isExpired(long expire)
    {
      return end > 0 && (System.currentTimeMillis() - end) > expire;
    }
  }

  @Override
  public void run()
  {
    for (String queryId : Maps.filterValues(
        queries, new Predicate<QueryStatus>()
        {
          @Override
          public boolean apply(QueryStatus input)
          {
            return input.isExpired(DEFAULT_EXPIRE);
          }
        }
    ).keySet()) {
      queries.remove(queryId);
    }
  }
}
