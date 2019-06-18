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

package io.druid.client.selector;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class ServerSelector
{
  public static ServerSelector dummy(final QueryableDruidServer server)
  {
    return new ServerSelector(null)
    {
      @Override
      public QueryableDruidServer pick(TierSelectorStrategy strategy, Predicate<QueryableDruidServer> predicate)
      {
        return predicate == null || predicate.apply(server) ? server : null;
      }
    };
  }

  private static final EmittingLogger log = new EmittingLogger(ServerSelector.class);

  private DataSegment segment;
  private final List<QueryableDruidServer> servers = Lists.newArrayListWithCapacity(1);

  public ServerSelector(DataSegment segment)
  {
    this.segment = segment;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public synchronized void addServerAndUpdateSegment(QueryableDruidServer server, DataSegment segment)
  {
    if (!servers.contains(server)) {
      servers.add(server);
    }
    this.segment = segment;
  }

  public synchronized boolean removeServer(QueryableDruidServer server)
  {
    return servers.remove(server);
  }

  public synchronized boolean isEmpty()
  {
    return servers.isEmpty();
  }

  public synchronized List<DruidServerMetadata> getCandidates()
  {
    List<DruidServerMetadata> result = Lists.newArrayList();
    for (QueryableDruidServer server : servers) {
      result.add(server.getServer().getMetadata());
    }
    return result;
  }

  public synchronized QueryableDruidServer pick(
      TierSelectorStrategy strategy,
      Predicate<QueryableDruidServer> predicate
  )
  {
    List<QueryableDruidServer> targets = servers;
    if (predicate != null) {
      targets = Lists.newArrayList(Iterables.filter(targets, predicate));
    }
    if (targets.isEmpty()) {
      return null;
    }
    if (targets.size() == 1) {
      return Iterables.getFirst(targets, null);
    }
    final TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers = new TreeMap<>(strategy.getComparator());
    for (QueryableDruidServer server : targets) {
      Set<QueryableDruidServer> theServers = prioritizedServers.get(server.getServer().getPriority());
      if (theServers == null) {
        theServers = Sets.newHashSet();
        prioritizedServers.put(server.getServer().getPriority(), theServers);
      }
      theServers.add(server);
    }
    return strategy.pick(prioritizedServers, segment);
  }

  public synchronized void clear()
  {
    servers.clear();
  }
}
