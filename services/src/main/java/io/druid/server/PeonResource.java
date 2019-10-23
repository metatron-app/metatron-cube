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
import com.google.inject.Inject;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 */
@Path("/druid/v2/")
public class PeonResource extends QueryResource
{
  private final TaskRunner taskRunner;

  @Inject
  public PeonResource(
      @Self DruidNode node,
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryManager queryManager,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      AuthConfig authConfig,
      QueryToolChestWarehouse warehouse,
      TaskRunner taskRunner
  )
  {
    super(node,
        config,
        jsonMapper,
        smileMapper,
        queryManager,
        texasRanger,
        warehouse,
        emitter,
        requestLogger,
        authConfig
    );
    this.taskRunner = taskRunner;
  }

  @GET
  @Path("/task/{taskid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@PathParam("taskid") String taskid) throws IOException, InterruptedException
  {
    TaskRunnerWorkItem workItem = taskRunner.getWorkerItem(taskid);
    if (!(workItem instanceof ThreadPoolTaskRunner.ThreadPoolTaskRunnerWorkItem)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    float progress = ((ThreadPoolTaskRunner.ThreadPoolTaskRunnerWorkItem) workItem).getTask().progress();
    return Response.status(Response.Status.OK).entity(progress).build();
  }
}
