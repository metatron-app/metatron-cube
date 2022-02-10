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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class ServerSelector
{
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
      final Predicate<QueryableDruidServer> predicate,
      final Map<QueryableDruidServer, MutableInt> counts
  )
  {
    Iterator<QueryableDruidServer> iterator = servers.iterator();
    if (predicate != null) {
      iterator = Iterators.filter(iterator, predicate);
    }
    if (!iterator.hasNext()) {
      return null;
    }
    QueryableDruidServer selected = iterator.next();
    if (!iterator.hasNext()) {
      return selected;
    }
    while (iterator.hasNext()) {
      selected = pick(selected, iterator.next(), counts);
    }
    return selected;
  }

  private static final MutableInt ZERO = new MutableInt();

  private static QueryableDruidServer pick(
      final QueryableDruidServer server1,
      final QueryableDruidServer server2,
      final Map<QueryableDruidServer, MutableInt> counts
  )
  {
    int count1 = -server1.getServer().getPriority();
    int count2 = -server2.getServer().getPriority();
    if (count1 == count2) {
      count1 = counts.getOrDefault(server1, ZERO).intValue();
      count2 = counts.getOrDefault(server2, ZERO).intValue();
    }
    if (count1 == count2) {
      count1 = server1.getClient().getNumOpenConnections();
      count2 = server2.getClient().getNumOpenConnections();
    }
    return count1 <= count2 ? server1 : server2;
  }

  public synchronized void clear()
  {
    servers.clear();
  }
}
