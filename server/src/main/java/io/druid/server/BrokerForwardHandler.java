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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.core.Emitter;
import io.druid.client.BrokerServerView;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.common.utils.PropUtils;
import io.druid.data.output.Formatters;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.StorageHandler;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.server.log.Events;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class BrokerForwardHandler extends ForwardHandler
{
  private final TimelineServerView brokerServerView;
  private final DataSegmentPusher pusher;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final CoordinatorClient coordinator;

  private final Emitter eventEmitter;
  private final IndexMergerV9 merger;

  @Inject
  public BrokerForwardHandler(
      @Self DruidNode node,
      @Json ObjectMapper jsonMapper,
      QueryToolChestWarehouse warehouse,
      TimelineServerView brokerServerView,
      Map<String, StorageHandler> writerMap,
      DataSegmentPusher pusher,
      QuerySegmentWalker segmentWalker,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      CoordinatorClient coordinator,
      Emitter eventEmitter,
      IndexMergerV9 merger
  )
  {
    super(node, jsonMapper, warehouse, writerMap, segmentWalker);
    this.pusher = pusher;
    this.brokerServerView = brokerServerView;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.coordinator = coordinator;
    this.eventEmitter = eventEmitter;
    this.merger = merger;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Sequence wrapForwardResult(Query query, Map<String, Object> forwardContext, Map<String, Object> result)
      throws IOException
  {
    if (Formatters.isIndexFormat(forwardContext) && PropUtils.parseBoolean(forwardContext, "registerTable", false)) {
      result = Maps.newLinkedHashMap(result);
      result.put("broker", node.getHostAndPort());
      result.put("queryId", query.getId());
      Map<String, Object> dataMeta = (Map<String, Object>) result.get("data");
      if (dataMeta == null) {
        LOG.info("Nothing to publish..");
        return Sequences.simple(Arrays.asList(result));
      }
      URI location = (URI) dataMeta.get("location");
      DataSegment segment = (DataSegment) dataMeta.get("segment");
      ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      builder.put("feed", "BrokerQueryResource");
      builder.put("broker", node.getHostAndPort());
      builder.put("payload", segment);
      if (PropUtils.parseBoolean(forwardContext, "temporary", true)) {
        LOG.info("Publishing index to temporary table..");
        BrokerServerView serverView = (BrokerServerView) brokerServerView;
        serverView.addedLocalSegment(segment, merger.getIndexIO().loadIndex(new File(location.getPath())), result);
        builder.put("type", "localPublish");
      } else {
        LOG.info("Publishing index to table..");
        segment = pusher.push(new File(location.getPath()), segment);   // rewrite load spec
        Set<DataSegment> segments = Sets.newHashSet(segment);
        indexerMetadataStorageCoordinator.announceHistoricalSegments(segments);
        try {
          long assertTimeout = PropUtils.parseLong(forwardContext, "waitTimeout", 0L);
          boolean assertLoaded = PropUtils.parseBoolean(forwardContext, "assertLoaded");
          coordinator.scheduleNow(segments, assertTimeout, assertLoaded);
        }
        catch (Exception e) {
          // ignore
          LOG.info("failed to notify coordinator directly by %s.. just wait next round of coordination", e);
        }
        builder.put("type", "publish");
      }
      eventEmitter.emit(new Events.SimpleEvent(builder.put("createTime", System.currentTimeMillis()).build()));
    }
    return Sequences.simple(Arrays.asList(result));
  }
}
