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

package io.druid.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.druid.cache.Cache;
import io.druid.cache.SessionCache;
import io.druid.common.DateTimes;
import io.druid.common.Progressing;
import io.druid.common.Tagged;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryException;
import io.druid.query.QueryWatcher;
import io.druid.utils.StopWatch;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryManager implements QueryWatcher, Runnable
{
  private static final Logger LOG = new Logger(QueryManager.class);

  private static final long DEFAULT_EXPIRE = 180_000;   // 3 min
  private static final long LOG_THRESHOLD_MSEC = 200;

  private final QueryConfig config;
  private final Map<String, QueryStatus> queries = Maps.newConcurrentMap();
  private final ListeningExecutorService executor = Execs.newDirectExecutorService();

  @VisibleForTesting
  public QueryManager()
  {
    this(new QueryConfig());
  }

  @Inject
  public QueryManager(QueryConfig config)
  {
    this.config = config;
  }

  @Override
  public QueryConfig getQueryConfig()
  {
    return config;
  }

  public void start(long intervalSec)
  {
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor(
        Execs.simpleDaemonFactory("QueryManager")
    );
    background.scheduleWithFixedDelay(this, intervalSec, intervalSec, TimeUnit.SECONDS);
  }

  @Override
  public void cancel(String queryId)
  {
    QueryStatus status = queries.get(queryId);
    if (status != null) {
      status.cancel();
    }
  }

  @Override
  public boolean isCancelled(String queryId)
  {
    if (queryId == null) {
      return false;
    }
    QueryStatus status = queries.get(queryId);
    return status != null && status.isCancelled();
  }

  @Override
  public boolean isTimedOut(String queryId)
  {
    if (queryId == null) {
      return false;
    }
    QueryStatus status = queries.get(queryId);
    return status != null && status.remaining() <= 0;
  }

  @Override
  public void finished(String queryId)
  {
    if (queryId != null) {
      QueryStatus status = queries.remove(queryId);
      if (status != null) {
        status.finished();
      }
    }
  }

  @Override
  public StopWatch register(final Query query, final ListenableFuture future, final Closeable resource)
  {
    final String id = query.getId();
    final List<String> dataSources = query.getDataSource().getNames();
    if (id == null) {
      LOG.warn("Query id for %s%s is null.. fix that", query.getType(), dataSources);
      return new StopWatch(config.getMaxQueryTimeout());
    }
    final QueryStatus status = queries.computeIfAbsent(
        id, k -> new QueryStatus(query.getType(), dataSources, BaseQuery.getTimeout(query, config.getMaxQueryTimeout()))
    );
    final long remaining = status.start(future, resource);
    future.addListener(() -> status.end(future), executor);
    return new StopWatch(remaining);
  }

  @Override
  public void unregister(Query query, Closeable resource)
  {
    if (query.getId() == null) {
      LOG.warn("Query id for %s is null.. fix that", query.getType());
      return;
    }
    final QueryStatus status = queries.get(query.getId());
    if (status != null) {
      status.unregisterResource(resource);
    }
  }

  @Override
  public long remainingTime(String queryId)
  {
    if (queryId == null) {
      return config.getMaxQueryTimeout();  // bug
    }
    final QueryStatus status = queries.get(queryId);  // some internal queries?
    return status == null ? config.getMaxQueryTimeout() : status.remaining();
  }

  @Override
  public Cache getSessionCache(String queryId)
  {
    if (queryId != null) {
      QueryStatus status = queries.get(queryId);
      if (status != null) {
        return status.cache;
      }
    }
    return null;
  }

  public List<String> getQueryDatasources(final String queryId)
  {
    QueryStatus status = queries.get(queryId);
    return status == null ? ImmutableList.of() : status.dataSources;
  }

  public long getQueryStartTime(final String queryId)
  {
    QueryStatus status = queries.get(queryId);
    return status == null ? -1 : status.start;
  }

  public float progress(String queryId) throws IOException, InterruptedException
  {
    QueryStatus status = queries.get(queryId);
    if (status != null && status.pendings.size() == 1) {
      ListenableFuture future = Iterables.getFirst(status.pendings.keySet(), null);
      if (future instanceof Progressing) {
        return ((Progressing) future).progress();
      }
    }
    return -1;
  }

  public List<Map<String, Object>> getRunningQueryStatus()
  {
    final List<Map<String, Object>> result = Lists.newArrayList();
    for (Map.Entry<String, QueryStatus> entry : queries.entrySet()) {
      final QueryStatus status = entry.getValue();
      if (status.isFinished()) {
        continue;
      }
      result.add(ImmutableMap.of(
          "queryId", entry.getKey(),
          "queryType", status.type,
          "start", DateTimes.utc(status.start).toString(),
          "pendingTags", status.pendingTags()
      ));
    }
    return result;
  }

  public void dumpAll()
  {
    LOG.info("Dumping query manager..");
    for (Map.Entry<String, QueryStatus> entry : queries.entrySet()) {
      QueryStatus status = entry.getValue();
      if (status.isFinished()) {
        LOG.info(
            "-- %s (%s) : started=%s, end=%s, duration=%d, canceled=%s, pending=%d, tagged=%s",
            entry.getKey(),
            status.type,
            DateTimes.utc(status.start),
            DateTimes.utc(status.end),
            status.end - status.start,
            status.canceled,
            status.pendings.size(),
            status.pendingTags()
        );
      } else {
        LOG.info(
            "-- %s (%s) : started=%s, pending=%d, tagged=%s",
            entry.getKey(),
            status.type,
            DateTimes.utc(status.start),
            status.pendings.size(),
            status.pendingTags()
        );
      }
    }
  }

  private static class QueryStatus
  {
    private final String type;
    private final long timeout;
    private final List<String> dataSources;

    private final long start = System.currentTimeMillis();
    private final Map<ListenableFuture, Timer> pendings = new IdentityHashMap<>();
    private final List<Closeable> resources = Lists.newArrayList();   // for cancel
    private final Cache cache = new SessionCache();

    private volatile boolean canceled;
    private volatile long end = -1;

    public QueryStatus(String type, List<String> dataSources, long timeout)
    {
      this.type = type;
      this.timeout = timeout;
      this.dataSources = dataSources;
      assert timeout > 0;
    }

    private synchronized boolean isCancelled()
    {
      return canceled;
    }

    private synchronized boolean isFinished()
    {
      return end > 0;
    }

    private synchronized long start(ListenableFuture future, Closeable resource)
    {
      final long remaining = remaining();
      if (canceled || end > 0 || remaining <= 0) {
        Execs.cancelQuietly(future);
        IOUtils.closeQuietly(resource);
        throw QueryException.wrapIfNeeded(canceled ? new CancellationException() : new TimeoutException());
      }
      pendings.put(future, Timer.of(future));
      if (resource != null) {
        resources.add(resource);
      }
      return remaining;
    }

    private synchronized void end(ListenableFuture future)
    {
      // check for prevent concurrent modification exception
      if (!isFinished()) {
        pendings.remove(future);
      }
    }

    private synchronized void cancel()
    {
      if (!isFinished()) {
        canceled = true;
        end = System.currentTimeMillis();
        clear();
      }
    }

    private synchronized void finished()
    {
      if (!isFinished()) {
        end = System.currentTimeMillis();
        clear();
      }
    }

    private void clear()
    {
      for (Closeable closeable : resources) {
        IOUtils.closeQuietly(closeable);
      }
      resources.clear();
      for (ListenableFuture future : pendings.keySet()) {
        Execs.cancelQuietly(future);    // induces end() call
      }
      if (!pendings.isEmpty()) {
        log(Lists.newArrayList(Iterables.filter(pendings.values(), Predicates.notNull())));
      }
      pendings.clear();
      cache.close(null);
    }

    private synchronized long remaining()
    {
      return timeout - (System.currentTimeMillis() - start);
    }

    private synchronized boolean isExpired(long current, long expire)
    {
      final long endTime = end < 0 ? start + timeout : end;
      return endTime > 0 && (current - endTime) > expire;
    }

    private synchronized boolean isTimedOut(long current)
    {
      return !isFinished() && start + timeout < current;
    }

    private synchronized void log(List<Timer> pendings)
    {
      if (pendings.isEmpty()) {
        return;
      }
      Collections.sort(pendings);
      long current = System.currentTimeMillis();
      long total = 0;
      int counter = 0;
      for (Timer timer : pendings) {
        total += timer.end(current);
        counter++;
      }
      if (pendings.get(0).elapsed < LOG_THRESHOLD_MSEC) {
        // skip for trivial queries (meta queries, etc.)
        return;
      }
      final long mean = total / counter;
      final double threshold = Math.max(LOG_THRESHOLD_MSEC / 2f, mean * 1.5f);

      List<Timer> log = ImmutableList.copyOf(
          Iterables.limit(Iterables.filter(pendings, input -> input.elapsed > threshold), 8)
      );
      if (log.isEmpty()) {
        log = Arrays.asList(pendings.get(0));
      }

      LOG.info("%d item(s) averaging %,d msec.. mostly from %s", counter, mean, log);
    }

    private synchronized List<String> pendingTags()
    {
      return ImmutableList.copyOf(
          Iterables.transform(Iterables.filter(pendings.values(), Predicates.notNull()), timer -> timer.tag)
      );
    }

    public synchronized void unregisterResource(Closeable resource)
    {
      resources.remove(resource);
    }
  }

  public void stop()
  {
    for (QueryStatus status : queries.values()) {
      status.cancel();
    }
    queries.clear();
  }

  private long lastCleanup;

  @Override
  public void run()
  {
    final long current = System.currentTimeMillis();
    for (QueryStatus status : Maps.filterValues(queries, input -> input.isTimedOut(current)).values()) {
      status.cancel();
    }
    if (current > lastCleanup + DEFAULT_EXPIRE) {
      for (String queryId : Maps.filterValues(queries, input -> input.isExpired(current, DEFAULT_EXPIRE)).keySet()) {
        queries.remove(queryId);
      }
      lastCleanup = current;
    }
  }

  private static class Timer implements Comparable<Timer>
  {
    private static Timer of(ListenableFuture future)
    {
      return future instanceof Tagged ? new Timer(((Tagged) future).getTag()) : null;
    }

    private final String tag;
    private final long start = System.currentTimeMillis();
    private long elapsed = -1;

    private Timer(String tag) {this.tag = tag;}

    private long end(long current)
    {
      return elapsed = current - start;
    }

    @Override
    public int compareTo(Timer o)
    {
      return Long.compare(start, o.start);
    }

    @Override
    public String toString()
    {
      return StringUtils.format("%s=%,dms", tag, elapsed);
    }
  }
}
