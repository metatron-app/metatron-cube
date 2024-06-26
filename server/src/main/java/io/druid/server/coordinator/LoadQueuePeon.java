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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.timeline.DataSegment;
import io.druid.utils.StopWatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 */
public class LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(LoadQueuePeon.class);

  private static final int DROP = 0;
  private static final int LOAD = 1;

  private static final byte[] NOOP_PAYLOAD = StringUtils.toUtf8("{\"action\": \"noop\"}");

  private static void executeCallbacks(List<LoadPeonCallback> callbacks, boolean success)
  {
    for (LoadPeonCallback callback : callbacks) {
      if (callback != null) {
        callback.execute(success);
      }
    }
  }

  private final CuratorFramework curator;
  private final String server;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;
  private final ExecutorService callBackExecutor;
  private final DruidCoordinatorConfig config;

  private final AtomicLong queuedSize = new AtomicLong(0);
  private final AtomicInteger assignSuccessCount = new AtomicInteger(0);
  private final AtomicInteger assignFailCount = new AtomicInteger(0);

  private final NavigableMap<DataSegment, SegmentHolder> segmentsToLoad = new TreeMap<>(DataSegment.TIME_DESCENDING);
  private final NavigableMap<DataSegment, SegmentHolder> segmentsToDrop = new TreeMap<>(DataSegment.TIME_DESCENDING);

  private final Object lock = new Object();

  private final Map<DataSegment, SegmentHolder> inProcessing = Maps.newHashMap();
  private final Map<DataSegment, SegmentHolder> failQueue = Maps.newHashMap();    // jail

  LoadQueuePeon(
      CuratorFramework curator,
      String loadQueuePath,
      String server,
      ObjectMapper jsonMapper,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.server = server;
    this.basePath = ZKPaths.makePath(loadQueuePath, server);
    this.jsonMapper = jsonMapper;
    this.callBackExecutor = callbackExecutor;
    this.processingExecutor = processingExecutor;
    this.config = config;
  }

  public String getBasePath()
  {
    return basePath;
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad.keySet();
  }

  @JsonProperty
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop.keySet();
  }

  public int getNumSegmentsToLoad()
  {
    synchronized (lock) {
      return segmentsToLoad.size() + Maps.filterValues(inProcessing, h -> h.type == LOAD).size();
    }
  }

  public int getNumSegmentsToDrop()
  {
    synchronized (lock) {
      return segmentsToDrop.size() + Maps.filterValues(inProcessing, h -> h.type == DROP).size();
    }
  }

  public void getSegmentsToLoad(Consumer<DataSegment> sink)
  {
    synchronized (lock) {
      segmentsToLoad.keySet().forEach(sink);
      Maps.filterValues(inProcessing, h -> h.type == LOAD).keySet().forEach(sink);
    }
  }

  public void getSegmentsToDrop(Consumer<DataSegment> sink)
  {
    synchronized (lock) {
      segmentsToDrop.keySet().forEach(sink);
      Maps.filterValues(inProcessing, h -> h.type == DROP).keySet().forEach(sink);
    }
  }

  private static final SegmentHolder DUMMY = new SegmentHolder(DataSegment.asKey("dummy"), 99, null, null);

  public boolean isLoadingSegment(DataSegment segment)
  {
    synchronized (lock) {
      return segmentsToLoad.containsKey(segment) || inProcessing.getOrDefault(segment, DUMMY).type == LOAD;
    }
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    synchronized (lock) {
      return segmentsToDrop.containsKey(segment) || inProcessing.getOrDefault(segment, DUMMY).type == DROP;
    }
  }

  public int getNumberOfQueuedSegments()
  {
    synchronized (lock) {
      return segmentsToDrop.size() + segmentsToLoad.size() + inProcessing.size();
    }
  }

  public long getLoadQueueSize()
  {
    return queuedSize.get();
  }

  public int getAndResetAssignSuccessCount()
  {
    return assignSuccessCount.getAndSet(0);
  }

  public int getAndResetAssignFailCount()
  {
    return assignFailCount.getAndSet(0);
  }

  @VisibleForTesting
  void loadSegment(DataSegment segment, LoadPeonCallback callback)
  {
    loadSegment(segment, "test", callback);
  }

  @VisibleForTesting
  void dropSegment(DataSegment segment, final LoadPeonCallback callback)
  {
    dropSegment(segment, "test", callback);
  }

  public void loadSegment(DataSegment segment, String reason, LoadPeonCallback callback)
  {
    checkIn(new SegmentHolder(segment, LOAD, reason, callback));
  }

  public void dropSegment(DataSegment segment, String reason, LoadPeonCallback callback)
  {
    checkIn(new SegmentHolder(segment, DROP, reason, callback));
  }

  private void checkIn(SegmentHolder holder)
  {
    log.info("Asking server [%s] to [%s] for [%s]", server, holder, holder.reason);

    Map<DataSegment, SegmentHolder> queue = holder.type == LOAD ? segmentsToLoad : segmentsToDrop;
    synchronized (lock) {
      failQueue.remove(holder.segment);   // discard whatever
      for (SegmentHolder running : inProcessing.values()) {
        if (running.merge(holder)) {
          return;
        }
      }
      SegmentHolder existing = queue.get(holder.segment);
      if (existing != null && existing.merge(holder)) {
        return;
      }
      existing = queue.put(holder.segment, holder);  // possibly overwrite
      if (existing != null && existing.type == LOAD) {
        queuedSize.addAndGet(-existing.getSegmentSize());
      }
    }
    if (holder.type == LOAD) {
      queuedSize.addAndGet(holder.getSegmentSize());
    }
    execute();
  }

  private static final int PENDING_THRESHOLD = 16;
  private static final long WAIT_ON_PENDING = 10000;

  private SegmentHolder work()
  {
    if (!StopWatch.wainOn(lock, () -> inProcessing.size() < PENDING_THRESHOLD, WAIT_ON_PENDING)) {
      log.info("Pending [%d] tasks for Server[%s]..", inProcessing.size(), server);
      return null;
    }
    synchronized (lock) {
      if (!segmentsToDrop.isEmpty()) {
        SegmentHolder holder = segmentsToDrop.pollFirstEntry().getValue();
        inProcessing.put(holder.segment, holder);
        return holder;
      } else if (!segmentsToLoad.isEmpty()) {
        SegmentHolder holder = segmentsToLoad.pollFirstEntry().getValue();
        inProcessing.put(holder.segment, holder);
        return holder;
      }
      return null;
    }
  }

  private void execute()
  {
    // single threaded
    processingExecutor.execute(
        () -> {
          for (SegmentHolder work = work(); work != null; work = work()) {

            log.debug("Server[%s] processing [%s]", server, work);

            final SegmentHolder current = work;
            final int generation = current.generation();
            try {
              final String path = ZKPaths.makePath(basePath, current.getSegmentIdentifier());
              final byte[] payload = jsonMapper.writeValueAsBytes(current.toChangeRequest());

              curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

              // register cleanup
              current.cleanup = processingExecutor.schedule(
                  () -> {
                    try {
                      if (current.generation() == generation && curator.checkExists().forPath(path) != null) {
                        failed(current, new TimeoutException("Timed-out!!"));
                      }
                    }
                    catch (Exception e) {
                      failed(current, e);
                    }
                  },
                  config.getLoadTimeoutDelay().getMillis(),
                  TimeUnit.MILLISECONDS
              );

              final Stat stat = curator.checkExists().usingWatcher((CuratorWatcher) event -> {
                if (event.getType() == EventType.NodeDeleted) {
                  success(current, event.getPath());
                }
              }).forPath(path);

              if (stat == null) {

                // Create a node and then delete it to remove the registered watcher.  This is a work-around for
                // a zookeeper race condition.  Specifically, when you set a watcher, it fires on the next event
                // that happens for that node.  If no events happen, the watcher stays registered foreverz.
                // Couple that with the fact that you cannot set a watcher when you create a node, but what we
                // want is to create a node and then watch for it to get deleted.  The solution is that you *can*
                // set a watcher when you check to see if it exists so, we first create the node and then set a
                // watcher on its existence.  However, if already does not exist by the time the existence check
                // returns, then the watcher that was set will never fire (nobody will ever create the node
                // again) and thus lead to a slow, but real, memory leak.  So, we create another node to cause
                // that watcher to fire and delete it right away.
                //
                // We do not create the existence watcher first, because then it will fire when we create the
                // node, and we'll have the same race when trying to refresh that watcher.
                curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, NOOP_PAYLOAD);

                success(current, path);
              }
            }
            catch (Exception e) {
              failed(current, e);
            }
          }
        }
    );
  }

  private void success(SegmentHolder processing, String path)
  {
    if (!processing.executed() && ZKPaths.getNodeFromPath(path).equals(processing.getSegmentIdentifier())) {
      log.debug("Server[%s] done processing [%s]", server, processing);
      assignSuccessCount.getAndIncrement();
      _done(processing, true);
    }
  }

  private void failed(SegmentHolder processing, Exception e)
  {
    if (!processing.executed()) {
      log.info("Failed to assign [%s] to Server[%s] by [%s]", processing, server, e);
      assignFailCount.getAndIncrement();
      _done(processing, false);
      synchronized (failQueue) {
        failQueue.put(processing.segment, processing);   // wait for revive
      }
    }
  }

  private void _done(SegmentHolder processing, boolean success)
  {
    synchronized (lock) {
      inProcessing.remove(processing.segment);
      lock.notifyAll();
    }
    if (processing.type == LOAD) {
      queuedSize.addAndGet(-processing.getSegmentSize());
    }
    processing.finalize(callBackExecutor, success);
  }

  public void stop()
  {
    synchronized (lock) {
      for (SegmentHolder holder : segmentsToDrop.values()) {
        holder.finalize(null, false);
      }
      segmentsToDrop.clear();

      for (SegmentHolder holder : segmentsToLoad.values()) {
        holder.finalize(null, false);
      }
      segmentsToLoad.clear();

      queuedSize.set(0L);
      assignFailCount.set(0);
      assignSuccessCount.set(0);
    }
  }

  public void tick(final int threshold)
  {
    synchronized (lock) {
      Iterator<SegmentHolder> iterator = segmentsToLoad.values().iterator();
      while (iterator.hasNext()) {
        final SegmentHolder holder = iterator.next();
        if (++holder.tick > threshold) {
          iterator.remove();
          if (holder.type == LOAD) {
            queuedSize.addAndGet(-holder.getSegmentSize());
          }
          holder.finalize(callBackExecutor, false);
          log.info("Dropped [%s] from load queue of Server[%s]", holder.getSegmentIdentifier(), server);
        }
      }
    }
    synchronized (failQueue) {
      Iterator<SegmentHolder> iterator = failQueue.values().iterator();
      while (iterator.hasNext()) {
        final SegmentHolder holder = iterator.next();
        if (holder.revive++ > holder.generation()) {
          iterator.remove();
          checkIn(holder.reset());
        }
      }
    }
  }

  private static class SegmentHolder
  {
    private static final String[] OP = new String[] {"DROP", "LOAD"};

    private final DataSegment segment;
    private final int type;
    private final String reason;
    private final List<LoadPeonCallback> callbacks = Lists.newLinkedList();

    private int tick;
    private int generation;
    private int revive;

    private boolean executed;
    private ScheduledFuture<?> cleanup;

    private SegmentHolder(DataSegment segment, int type, String reason, LoadPeonCallback callback)
    {
      this.segment = segment;
      this.type = type;
      this.reason = reason;
      if (callback != null) {
        this.callbacks.add(callback);
      }
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public String getSegmentIdentifier()
    {
      return segment.getIdentifier();
    }

    public long getSegmentSize()
    {
      return segment.getSize();
    }

    public boolean merge(SegmentHolder other)
    {
      if (type != other.type || !segment.equals(other.segment)) {
        return false;
      }
      if (!GuavaUtils.isNullOrEmpty(other.callbacks)) {
        synchronized (segment) {
          if (executed) {
            return false;
          }
          callbacks.addAll(other.callbacks);
        }
      }
      // merge reason ??
      tick = Math.max(tick, other.tick);
      generation = Math.max(generation, other.generation);
      return true;
    }

    private List<LoadPeonCallback> checkoutCallbacks()
    {
      synchronized (segment) {
        executed = true;
        return callbacks;
      }
    }

    private void finalize(ExecutorService executor, boolean success)
    {
      final List<LoadPeonCallback> callbacks = checkoutCallbacks();
      if (!callbacks.isEmpty()) {
        if (executor != null) {
          executor.execute(() -> executeCallbacks(callbacks, success));
        } else {
          executeCallbacks(callbacks, success);
        }
      }
      if (cleanup != null) {
        Execs.cancelQuietly(cleanup);
      }
      cleanup = null;
    }

    public SegmentHolder reset()
    {
      synchronized (segment) {
        cleanup = null;
        executed = false;
        generation++;
        revive = 0;
      }
      return this;
    }

    public boolean executed()
    {
      synchronized (segment) {
        return executed;
      }
    }

    public int generation()
    {
      synchronized (segment) {
        return generation;
      }
    }

    public DataSegmentChangeRequest toChangeRequest()
    {
      return type == LOAD ? new SegmentChangeRequestLoad(segment) : new SegmentChangeRequestDrop(segment);
    }

    @Override
    public String toString()
    {
      return OP[type] + ":" + segment.getIdentifier();
    }
  }
}
