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

package io.druid.server.http;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.BrokerServerView;
import io.druid.guice.annotations.Self;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.DruidNode;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/druid/broker/v1")
@ResourceFilters(StateResourceFilter.class)
public class BrokerResource
{
  private final DruidNode node;
  private final BrokerServerView brokerServerView;

  @Inject
  public BrokerResource(@Self DruidNode node, BrokerServerView brokerServerView)
  {
    this.node = node;
    this.brokerServerView = brokerServerView;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("inventoryInitialized", brokerServerView.isInitialized())).build();
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response handleJMX(@QueryParam("dumpLongestStack") boolean dumpLongestStack)
  {
    Map<String, Object> results = JMXQueryRunnerFactory.queryJMX(node, null, dumpLongestStack);
    return Response.ok(results).build();
  }
}
