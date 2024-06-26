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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 */
@ManageLifecycle
public class BatchServerInventoryView extends AbstractCuratorServerInventoryView<Set<DataSegment>>
    implements FilteredServerInventoryView
{
  private static final EmittingLogger log = new EmittingLogger(BatchServerInventoryView.class);

  final private ConcurrentMap<String, Set<String>> zNodes = new ConcurrentHashMap<>();
  final private ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates = new ConcurrentHashMap<>();
  final private Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;

  @Inject
  public BatchServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter
  )
  {
    super(
        log,
        zkPaths.getAnnouncementsPath(),
        zkPaths.getLiveSegmentsPath(),
        curator,
        jsonMapper,
        new TypeReference<Set<DataSegment>>()
        {
        }
    );

    this.defaultFilter = Preconditions.checkNotNull(defaultFilter);
  }

  @Override
  protected DruidServer addInnerInventory(
      final DruidServer container,
      final String inventoryKey,
      final Set<DataSegment> inventory
  )
  {
    Set<String> added = Sets.newHashSet();
    for (DataSegment segment : filterInventory(container, inventory)) {
      addSingleInventory(container, segment);
      added.add(segment.getIdentifier());
    }
    zNodes.put(inventoryKey, added);
    return container;
  }

  private Iterable<DataSegment> filterInventory(final DruidServer container, Set<DataSegment> inventory)
  {
    Predicate<Pair<DruidServerMetadata, DataSegment>> predicate = Predicates.or(
        defaultFilter, Predicates.or(segmentPredicates.values())
    );

    // make a copy of the set and not just a filtered view, in order to not keep all the segment data in memory
    return Iterables.transform(
        Iterables.filter(Iterables.transform(inventory, ds -> Pair.of(container.getMetadata(), ds)), predicate),
        Pair.rhsFn()
    );
  }

  @Override
  protected DruidServer updateInnerInventory(DruidServer container, String inventoryKey, Set<DataSegment> inventory)
  {
    final Set<String> existing = zNodes.get(inventoryKey);
    if (existing == null) {
      throw new ISE("Trying to update an inventoryKey[%s] that didn't exist?!", inventoryKey);
    }
    final Set<DataSegment> filtered = Sets.newHashSet(filterInventory(container, inventory));

    for (DataSegment segment : Iterables.filter(filtered, segment -> !existing.contains(segment.getIdentifier()))) {
      addSingleInventory(container, segment);
    }
    for (String segmentId : Iterables.filter(existing, segmentId -> !filtered.contains(DataSegment.asKey(segmentId)))) {
      removeSingleInventory(container, segmentId);
    }
    zNodes.put(inventoryKey, Sets.newHashSet(Iterables.transform(filtered, DataSegment::getIdentifier)));

    return container;
  }

  @Override
  protected DruidServer removeInnerInventory(DruidServer container, String inventoryKey)
  {
    log.debug("Server[%s] removed container[%s]", container.getName(), inventoryKey);
    Set<String> segmentIds = zNodes.remove(inventoryKey);

    if (segmentIds == null) {
      log.warn("Told to remove container[%s], which didn't exist", inventoryKey);
      return container;
    }

    for (String segmentId : segmentIds) {
      removeSingleInventory(container, segmentId);
    }
    return container;
  }

  @Override
  public void registerSegmentCallback(
      final Executor exec,
      final SegmentCallback callback,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    SegmentCallback filteringCallback = new SingleServerInventoryView.FilteringSegmentCallback(callback, filter);
    segmentPredicates.put(filteringCallback, filter);
    registerSegmentCallback(
        exec,
        filteringCallback
    );
  }

  @Override
  protected void segmentCallbackRemoved(SegmentCallback callback)
  {
    segmentPredicates.remove(callback);
  }
}
