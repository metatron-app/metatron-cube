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

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.common.config.JacksonConfigManager;
import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.server.http.security.ConfigResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/coordinator/v1/config/compaction")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorCompactionConfigsResource
{
  private final JacksonConfigManager manager;

  @Inject
  public CoordinatorCompactionConfigsResource(JacksonConfigManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactConfig()
  {
    // return mock config. not real data.
    return Response.ok(
        CoordinatorCompactionConfig.empty()
    ).build();
  }

}
