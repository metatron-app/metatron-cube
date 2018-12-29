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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.Progressing;
import io.druid.common.Tagged;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryWatcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

public class QueryManager implements QueryWatcher, Runnable
{
  private static final Logger LOG = new Logger(QueryManager.class);

  private static final long DEFAULT_EXPIRE = 300_000;   // 5 min

  private final Map<String, QueryStatus> queries;

  public QueryManager()
  {
    this.queries = Maps.newConcurrentMap();
  }

  public boolean cancelQuery(String id)
  {
    QueryStatus status = queries.get(id);
    if (status != null) {
      return status.cancel();
    }
    return true;
  }

  public boolean isCanceled(Query query)
  {
    String id = query.getId();
    if (id == null) {
      return false;
    }
    QueryStatus status = queries.get(id);
    if (status != null) {
      return status.canceled;
    }
    return false;
  }

  @Override
  public void registerQuery(final Query query, final ListenableFuture future)
  {
    final String id = query.getId();
    if (id == null) {
      LOG.warn("Query id for %s is null.. fix that", query.getType());
      return;
    }
    final QueryStatus status = queries.computeIfAbsent(
        id, new Function<String, QueryStatus>()
        {
          @Override
          public QueryStatus apply(String s)
          {
            int timeout = query.getContextInt(QueryContextKeys.TIMEOUT, -1);
            return new QueryStatus(query.getType(), timeout);
          }
        }
    );
    if (status.canceled) {
      throw new CancellationException();
    }
    final List<String> dataSources = query.getDataSource().getNames();

    status.start(future, dataSources);
    future.addListener(
        new Runnable()
        {
          @Override
          public void run()
          {
            if (status.end(future, dataSources)) {
              // query completed
              status.log();
            }
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
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

  public void dumpAll()
  {
    LOG.info("Dumping query manager..");
    for (Map.Entry<String, QueryStatus> entry : queries.entrySet()) {
      QueryStatus status = entry.getValue();
      LOG.info("-- %s (%s) : started=%s, end=%s, duration=%d, canceled=%s, pending=%d, tagged=%s",
               entry.getKey(),
               status.type,
               DateTimes.utc(status.start),
               status.end < 0 ? "<not>" : DateTimes.utc(status.end),
               status.end < 0 ? -1 : status.end - status.start,
               status.canceled,
               status.futures.size(),
               status.timers.values()
      );
    }
  }

  private static class QueryStatus
  {
    private final String type;
    private final int timeout;
    private final long start = System.currentTimeMillis();
    private final Set<String> dataSources = Sets.newConcurrentHashSet();
    private final Set<ListenableFuture> futures = Sets.newConcurrentHashSet();
    private final Map<ListenableFuture, Timer> timers =
        Collections.synchronizedMap(Maps.<ListenableFuture, Timer>newIdentityHashMap());

    private volatile boolean canceled;
    private volatile long end = -1;

    public QueryStatus(String type, int timeout)
    {
      this.type = type;
      this.timeout = timeout;
    }

    private void start(ListenableFuture future, List<String> dataSource)
    {
      futures.add(future);
      dataSources.addAll(dataSource);
      if (future instanceof Tagged) {
        timers.put(future, new Timer(((Tagged) future).getTag()));
      }
    }

    private boolean end(ListenableFuture future, List<String> dataSource)
    {
      futures.remove(future);
      dataSources.removeAll(dataSource);
      if (future instanceof Tagged) {
        Timer timer = timers.get(future);
        if (timer != null) {
          timer.end();
        }
      }
      // this is possible because druid registers queries before fire to historical nodes
      if (!canceled && futures.isEmpty() && dataSources.isEmpty()) {
        end = System.currentTimeMillis();
        return true;
      }
      return false;
    }

    private boolean cancel()
    {
      canceled = true;
      end = System.currentTimeMillis();
      return clear();
    }

    private boolean clear()
    {
      boolean success = true;
      for (ListenableFuture future : futures) {
        success = success & Execs.cancelQuietly(future);  // cancel all
        if (future instanceof Tagged) {
          Timer timer = timers.get(future);
          if (timer != null) {
            timer.end();
          }
        }
      }
      futures.clear();
      dataSources.clear();
      return success;
    }

    private long remaining()
    {
      return timeout <= 0 ? timeout : Math.max(1000, timeout - (System.currentTimeMillis() - start));
    }

    private boolean isExpired(long expire)
    {
      long endTime = end < 0 ? start + timeout : end;
      return endTime > 0 && (System.currentTimeMillis() - endTime) > expire;
    }

    public void log()
    {
      if (timers.isEmpty()) {
        return;
      }
      List<Timer> filtered = Lists.newArrayList(
          Iterables.filter(
              timers.values(), new Predicate<Timer>()
              {
                @Override
                public boolean apply(Timer input)
                {
                  return input.elapsed >= 0;
                }
              }
          )
      );
      timers.clear();
      if (filtered.isEmpty()) {
        return;
      }
      Collections.sort(filtered);
      long total = 0;
      int counter = 0;
      for (Timer timer : filtered) {
        if (timer.elapsed >= 0) {
          total += timer.elapsed;
          counter++;
        }
      }
      if (filtered.get(0).elapsed < 100) {
        // skip for trivial queries (meta queries, etc.)
        return;
      }
      final long mean = total / counter;
      final double threshold = mean * 1.2;

      List<Timer> log = Lists.newArrayList(
          Iterables.filter(
              filtered, new Predicate<Timer>()
              {
                @Override
                public boolean apply(Timer input)
                {
                  return input.elapsed > threshold;
                }
              }
          )
      );
      if (log.isEmpty()) {
        log = Arrays.asList(filtered.get(0));
      }

      LOG.info("%d item(s) averaging %,d msec.. mostly from %s", counter, mean, log);
    }
  }

  @Override
  public void run()
  {
    List<String> expiredQueries = ImmutableList.copyOf(
        Maps.filterValues(
            queries, new Predicate<QueryStatus>()
            {
              @Override
              public boolean apply(QueryStatus input)
              {
                return input.isExpired(DEFAULT_EXPIRE);
              }
            }
        ).keySet()
    );
    if (!expiredQueries.isEmpty()) {
      LOG.info("Expiring %d queries", expiredQueries.size());
    }
    for (String queryId : expiredQueries) {
      QueryStatus status = queries.remove(queryId);
      if (status != null) {
        try {
          status.clear();
        }
        catch (Exception e) {
          // ignore
        }
      }
    }
  }

  private static class Timer implements Comparable<Timer>
  {
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
