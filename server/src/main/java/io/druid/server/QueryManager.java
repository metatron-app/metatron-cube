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
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.druid.common.DateTimes;
import io.druid.common.Progressing;
import io.druid.common.Tagged;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryWatcher;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

public class QueryManager implements QueryWatcher, Runnable
{
  private static final Logger LOG = new Logger(QueryManager.class);

  private static final long DEFAULT_EXPIRE = 180_000;   // 3 min
  private static final long LOG_THRESHOLD = 200;

  private final long queryTimeout;
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
    this.queryTimeout = config.getMaxQueryTimeout();
  }

  @Override
  public boolean cancelQuery(String queryId)
  {
    QueryStatus status = queries.get(queryId);
    if (status != null) {
      return status.cancel();
    }
    return true;
  }

  public boolean isCanceled(String queryId)
  {
    if (queryId == null) {
      return false;
    }
    QueryStatus status = queries.get(queryId);
    if (status != null) {
      return status.canceled;
    }
    return false;
  }

  @Override
  public void registerQuery(final Query query, final ListenableFuture future, final Closeable resource)
  {
    final String id = query.getId();
    final List<String> dataSources = query.getDataSource().getNames();
    if (id == null) {
      LOG.warn("Query id for %s%s is null.. fix that", query.getType(), dataSources);
      return;
    }
    final QueryStatus status = queries.computeIfAbsent(
        id, new Function<String, QueryStatus>()
        {
          @Override
          public QueryStatus apply(String id)
          {
            long timeout = query.getContextLong(Query.TIMEOUT, queryTimeout);
            return new QueryStatus(query.getType(), dataSources, timeout);
          }
        }
    );
    status.start(future, resource);
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            if (!status.isCancelled() && status.end(future)) {
              // query completed
              status.log();
            }
          }
        },
        executor
    );
  }

  @Override
  public void unregisterResource(Query query, Closeable resource)
  {
    final QueryStatus status = queries.get(query.getId());
    if (status != null) {
      status.unregisterResource(resource);
    }
  }

  @Override
  public long remainingTime(String queryId)
  {
    if (queryId == null) {
      return -1L;  // bug
    }
    QueryStatus status = queries.get(queryId);
    return status == null ? -1 : status.remaining();
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
      return canceled || end > 0 && pendings.isEmpty();
    }

    private synchronized void start(ListenableFuture future, Closeable resource)
    {
      if (canceled) {
        Execs.cancelQuietly(future);
        IOUtils.closeQuietly(resource);
        throw new CancellationException();
      }
      pendings.put(future, Timer.of(future));
      if (resource != null) {
        resources.add(resource);
      }
    }

    private synchronized boolean end(ListenableFuture future)
    {
      Timer timer = pendings.remove(future);
      if (timer != null) {
        timer.end();
      }
      // this is possible because druid registers queries before fire to historical nodes
      if (!canceled && end < 0 && pendings.isEmpty()) {
        end = System.currentTimeMillis();
        return true;
      }
      return false;
    }

    private synchronized boolean cancel()
    {
      if (canceled) {
        return true;
      }
      canceled = true;
      end = System.currentTimeMillis();
      boolean success = true;
      for (Map.Entry<ListenableFuture, Timer> entry : pendings.entrySet()) {
        final ListenableFuture future = entry.getKey();
        final Timer timer = entry.getValue();
        success = success & Execs.cancelQuietly(future);  // cancel all
        if (timer != null) {
          timer.end();
        }
      }
      pendings.clear();

      for (Closeable closeable : resources) {
        IOUtils.closeQuietly(closeable);
      }
      resources.clear();
      return success;
    }

    private synchronized long remaining()
    {
      return timeout - (System.currentTimeMillis() - start);
    }

    private synchronized boolean isExpired(long current, long expire)
    {
      long endTime = end < 0 ? start + timeout : end;
      return endTime > 0 && (current - endTime) > expire;
    }

    private synchronized boolean isTimedOut(long current)
    {
      return !isFinished() && start + timeout < current;
    }

    public synchronized void log()
    {
      if (pendings.isEmpty()) {
        return;
      }
      List<Timer> filtered = ImmutableList.copyOf(
          Iterables.filter(pendings.values(), timer -> timer != null && timer.elapsed >= 0)
      );
      pendings.clear();
      if (filtered.isEmpty()) {
        return;
      }
      Collections.sort(filtered);
      if (filtered.get(0).elapsed < LOG_THRESHOLD) {
        // skip for trivial queries (meta queries, etc.)
        return;
      }
      long total = 0;
      int counter = 0;
      for (Timer timer : filtered) {
        if (timer.elapsed >= 0) {
          total += timer.elapsed;
          counter++;
        }
      }
      final long mean = total / counter;
      final double threshold = Math.max(LOG_THRESHOLD / 2, mean * 1.5);

      List<Timer> log = ImmutableList.copyOf(
          Iterables.limit(Iterables.filter(filtered, input -> input.elapsed > threshold), 8)
      );
      if (log.isEmpty()) {
        log = Arrays.asList(filtered.get(0));
      }

      LOG.info("%d item(s) averaging %,d msec.. mostly from %s", counter, mean, log);
    }

    private synchronized List<String> pendingTags()
    {
      return Lists.newArrayList(
          Iterables.transform(Iterables.filter(pendings.values(), Predicates.notNull()), timer -> timer.tag)
      );
    }

    public synchronized void unregisterResource(Closeable resource)
    {
      resources.remove(resource);
    }
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

    private void end()
    {
      elapsed = System.currentTimeMillis() - start;
    }

    @Override
    public int compareTo(Timer o)
    {
      return -Longs.compare(elapsed, o.elapsed);  // descending
    }

    @Override
    public String toString()
    {
      return StringUtils.format("%s=%,dms", tag, elapsed);
    }
  }
}
