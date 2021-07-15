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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import io.druid.concurrent.Execs;
import io.druid.curator.inventory.CuratorInventoryManager;
import io.druid.curator.inventory.CuratorInventoryManagerStrategy;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public abstract class AbstractCuratorServerInventoryView<InventoryType> implements ServerInventoryView
{
  private final EmittingLogger log;
  private final CuratorInventoryManager<DruidServer, InventoryType> inventoryManager;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private final ObjectMapper jsonMapper;
  private final TypeReference<InventoryType> inventoryType;

  private final ConcurrentMap<ServerCallback, Executor> serverCallbacks = new MapMaker().makeMap();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new MapMaker().makeMap();

  public AbstractCuratorServerInventoryView(
      final EmittingLogger log,
      final String announcementsPath,
      final String inventoryPath,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper,
      final TypeReference<InventoryType> inventoryType
  )
  {
    this.log = log;
    this.inventoryManager = new CuratorInventoryManager<>(
        curator,
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return announcementsPath;
          }

          @Override
          public String getInventoryPath()
          {
            return inventoryPath;
          }
        },
        Execs.singleThreaded("ServerInventoryView-%s"),
        new CuratorInventoryManagerStrategy<DruidServer, InventoryType>()
        {
          @Override
          public DruidServer deserializeContainer(byte[] bytes)
          {
            try {
              return AbstractCuratorServerInventoryView.this.deserializeContainer(bytes);
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public InventoryType deserializeInventory(byte[] bytes)
          {
            try {
              return AbstractCuratorServerInventoryView.this.deserializeInventory(bytes);
            }
            catch (IOException e) {
              CharBuffer charBuffer = Charsets.UTF_8.decode(ByteBuffer.wrap(bytes));
              log.error(e, "Could not parse json: %s", charBuffer.toString());
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void newContainer(DruidServer container)
          {
            log.info("Server[%s:%s] Registered", container.getType(), container.getName());
            runServerCallback(container, ServerCallback.Type.ADDED);
          }

          @Override
          public void deadContainer(DruidServer deadContainer)
          {
            log.info("Server[%s:%s] Disappeared", deadContainer.getType(), deadContainer.getName());
            runServerCallback(deadContainer, ServerCallback.Type.REMOVED);
          }

          @Override
          public DruidServer updateContainer(DruidServer oldContainer, DruidServer newContainer)
          {
            log.info("Server[%s:%s] Updated", oldContainer.getType(), newContainer);
            DruidServer updated = newContainer.addDataSegments(oldContainer);
            runServerCallback(updated, ServerCallback.Type.UPDATED);
            return updated;
          }

          @Override
          public DruidServer addInventory(DruidServer container, String inventoryKey, InventoryType inventory)
          {
            return addInnerInventory(container, inventoryKey, inventory);
          }

          @Override
          public DruidServer updateInventory(DruidServer container, String inventoryKey, InventoryType inventory)
          {
            return updateInnerInventory(container, inventoryKey, inventory);
          }

          @Override
          public DruidServer removeInventory(DruidServer container, String inventoryKey)
          {
            return removeInnerInventory(container, inventoryKey);
          }

          @Override
          public void inventoryInitialized()
          {
            log.info("Inventory Initialized");
            runSegmentCallbacks(input -> input.segmentViewInitialized());
          }
        }
    );
    this.jsonMapper = jsonMapper;
    this.inventoryType = inventoryType;
  }

  protected DruidServer deserializeContainer(byte[] bytes) throws IOException
  {
    return jsonMapper.readValue(bytes, DruidServer.class);
  }

  protected InventoryType deserializeInventory(byte[] bytes) throws IOException
  {
    return jsonMapper.readValue(bytes, inventoryType);
  }

  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (started) {
      if (!started.get()) {
        inventoryManager.start();
        started.set(true);
      }
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (started) {
      if (started.getAndSet(false)) {
        inventoryManager.stop();
      }
    }
  }

  @Override
  public boolean isStarted()
  {
    return started.get();
  }

  @Override
  public int getInventorySize()
  {
    return inventoryManager.getInventorySize();
  }

  @Override
  public DruidServer getInventoryValue(String containerKey)
  {
    return inventoryManager.getInventoryValue(containerKey);
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    return inventoryManager.getInventory();
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  @Override
  public void removeSegmentCallback(SegmentCallback callback)
  {
    segmentCallbacks.remove(callback);
  }

  public InventoryManagerConfig getInventoryManagerConfig()
  {
    return inventoryManager.getConfig();
  }

  protected void runSegmentCallbacks(
      final Function<SegmentCallback, CallbackAction> fn
  )
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
                segmentCallbackRemoved(entry.getKey());
                segmentCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  protected void runServerCallback(final DruidServer server, final ServerCallback.Type type)
  {
    for (final Map.Entry<ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == type.execute(entry.getKey(), server)) {
                serverCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  protected void addSingleInventory(final DruidServer container, final DataSegment segment)
  {
    log.debug("Server[%s] added segment[%s]", container.getName(), segment.getIdentifier());

    if (container.addDataSegment(segment)) {
      runSegmentCallbacks(input -> input.segmentAdded(container.getMetadata(), segment));
    }
  }

  protected void removeSingleInventory(final DruidServer container, final String inventoryKey)
  {
    log.debug("Server[%s] removed segment[%s]", container.getName(), inventoryKey);

    final DataSegment removed = container.removeDataSegment(inventoryKey);
    if (removed != null) {
      runSegmentCallbacks(input -> input.segmentRemoved(container.getMetadata(), removed));
    }
  }

  protected abstract DruidServer addInnerInventory(
      final DruidServer container,
      String inventoryKey,
      final InventoryType inventory
  );

  protected abstract DruidServer updateInnerInventory(
      final DruidServer container,
      String inventoryKey,
      final InventoryType inventory
  );

  protected abstract DruidServer removeInnerInventory(
      final DruidServer container,
      String inventoryKey
  );

  protected abstract void segmentCallbackRemoved(SegmentCallback callback);
}
