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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.BrokerServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.output.Formatters;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.StorageHandler;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.QueryableIndex;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.server.log.Events.SimpleEvent;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BrokerForwardHandler extends ForwardHandler
{
  private final BrokerServerView brokerServerView;
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
      BrokerServerView brokerServerView,
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
  protected Map<String, Object> prepareContext(Query query, Map<String, Object> context)
  {
    if (Formatters.isIndexFormat(context) && PropUtils.parseBoolean(context, REGISTER_TABLE, false)) {
      context = Maps.newLinkedHashMap(context);
      context.put("broker", node.getHostAndPort());
      context.put(Query.QUERYID, query.getId());
      context.putIfAbsent(DATASOURCE, "___temporary_" + new DateTime());
      if (PropUtils.parseBoolean(context, TEMPORARY, true) && !PropUtils.parseBoolean(context, OVERWRITE, false)) {
        brokerServerView.addLocalDataSource(PropUtils.parseString(context, DATASOURCE));
      }
    }
    return context;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Sequence wrapForwardResult(Query query, Map<String, Object> context, Map<String, Object> result)
      throws IOException
  {
    if (Formatters.isIndexFormat(context) && PropUtils.parseBoolean(context, REGISTER_TABLE, false)) {
      String dataSource = PropUtils.parseString(context, DATASOURCE);
      boolean temporary = PropUtils.parseBoolean(context, TEMPORARY, true);
      boolean overwrite = PropUtils.parseBoolean(context, OVERWRITE, false);
      Map<String, Object> metaData = ImmutableMap.of(Query.QUERYID, query.getId());

      if (temporary && overwrite) {
        brokerServerView.dropLocalDataSource(dataSource);
      }
      List<Map<String, Object>> segmentsData = (List<Map<String, Object>>) result.get("data");
      if (GuavaUtils.isNullOrEmpty(segmentsData)) {
        LOG.info("Nothing to publish..");
        return Sequences.simple(Arrays.asList(result));
      }

      LOG.info("Publishing [%d] segments for %s...", segmentsData.size(), dataSource);

      final Set<DataSegment> segments = Sets.newHashSet();
      for (Map<String, Object> segmentData : segmentsData) {
        URI location = (URI) segmentData.get("location");
        DataSegment segment = (DataSegment) segmentData.get("segment");

        if (temporary) {
          QueryableIndex index = merger.getIndexIO().loadIndex(new File(location.getPath()));
          brokerServerView.addLocalSegment(segment, index, metaData);
        } else {
          segment = pusher.push(new File(location.getPath()), segment);   // rewrite load spec
          segments.add(segment);
        }
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("feed", "BrokerForwardHandler");
        builder.put("type", "loadTable");
        builder.put("broker", node.getHostAndPort());
        builder.put("payload", segment);
        builder.put("temporary", temporary);
        builder.put("createdTime", System.currentTimeMillis());
        eventEmitter.emit(new SimpleEvent(builder.build()));
      }
      if (temporary) {
        LOG.info("Segments are registered to %stable %s", temporary ? "temporary " : "", dataSource);
      } else {
        indexerMetadataStorageCoordinator.announceHistoricalSegments(segments);
        try {
          long assertTimeout = PropUtils.parseLong(context, WAIT_TIMEOUT, 0L);
          boolean assertLoaded = PropUtils.parseBoolean(context, ASSERT_LOADED);
          coordinator.scheduleNow(segments, assertTimeout, assertLoaded);
        }
        catch (Exception e) {
          // ignore
          LOG.info("failed to notify coordinator directly by %s.. just wait next round of coordination", e);
        }
      }
    }
    return Sequences.simple(Arrays.asList(result));
  }
}
