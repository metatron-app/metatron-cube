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

package io.druid.query;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.cache.Cache;
import io.druid.utils.StopWatch;

import java.io.Closeable;

/**
 * This interface is in a very early stage and should not be considered stable.
 * <p>
 * The purpose of the QueryWatcher is to give overall visibility into queries running
 * or pending at the QueryRunner level. This is currently used to cancel all the
 * parts of a pending query, but may be expanded in the future to offer more direct
 * visibility into query execution and resource usage.
 * <p>
 * QueryRunners executing any computation asynchronously must register their queries
 * with the QueryWatcher.
 */
public interface QueryWatcher
{
  default StopWatch register(Query query, ListenableFuture future)
  {
    return register(query, future, null);
  }

  /**
   * QueryRunners must use this method to register any pending queries.
   * <p>
   * The given future may have cancel(true) called at any time, if cancellation of this query has been requested.
   *
   * @param query  a query, which may be a subset of a larger query, as long as the underlying queryId is unchanged
   * @param future the future holding the execution status of the query
   */
  StopWatch register(Query query, ListenableFuture future, Closeable resource);

  void unregister(Query query, Closeable resource);

  long remainingTime(String queryId);

  void cancel(String queryId);

  void finished(String queryId);

  boolean isCancelled(String queryId);

  boolean isTimedOut(String queryId);

  QueryConfig getQueryConfig();

  Cache getSessionCache(String queryId);

  class Abstract implements QueryWatcher
  {
    private static final QueryConfig DUMMY = new QueryConfig();

    @Override
    public StopWatch register(Query query, ListenableFuture future, Closeable resource)
    {
      return new StopWatch(60_000L);
    }

    @Override
    public void unregister(Query query, Closeable resource) {}

    @Override
    public long remainingTime(String queryId) { return 60_000L;}

    @Override
    public void cancel(String queryId) {}

    @Override
    public void finished(String queryId) {}

    @Override
    public boolean isCancelled(String queryId) { return false;}

    @Override
    public boolean isTimedOut(String queryId) { return false;}

    @Override
    public QueryConfig getQueryConfig()
    {
      return DUMMY;
    }

    @Override
    public Cache getSessionCache(String queryId)
    {
      return null;
    }
  }
}
