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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.collections.String2LongMap;
import io.druid.guice.PropertiesModule;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.commons.lang.mutable.MutableLong;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 */
@Path("/druid/coordinator/v1")
@ResourceFilters(StateResourceFilter.class)
public class CoordinatorResource
{
  private final DruidCoordinator coordinator;

  @Inject
  public CoordinatorResource(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  @GET
  @Path("/leader")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLeader()
  {
    return Response.ok(coordinator.getCurrentLeader()).build();
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus(
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple != null) {
      return Response.ok(coordinator.getSegmentAvailability()).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getReplicationStatus()).build();
    }
    return Response.ok(coordinator.getLoadStatus()).build();
  }

  @GET
  @Path("/loadqueue")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadQueue(
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple != null) {
      return Response.ok(
          Maps.transformValues(
              coordinator.getLoadManagementPeons(),
              new Function<LoadQueuePeon, Object>()
              {
                @Override
                public Object apply(LoadQueuePeon input)
                {
                  final MutableLong loadSize = new MutableLong();
                  input.getSegmentsToLoad(segment -> loadSize.add(segment.getSize()));

                  final MutableLong dropSize = new MutableLong();
                  input.getSegmentsToDrop(segment -> dropSize.add(segment.getSize()));

                  return new ImmutableMap.Builder<>()
                      .put("segmentsToLoad", input.getNumSegmentsToLoad())
                      .put("segmentsToDrop", input.getNumSegmentsToDrop())
                      .put("segmentsToLoadSize", loadSize.longValue())
                      .put("segmentsToDropSize", dropSize.longValue())
                      .build();
                }
              }
          )
      ).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getLoadManagementPeons()).build();
    }

    return Response.ok(
        Maps.transformValues(
            coordinator.getLoadManagementPeons(),
            peon -> {
              List<String> loading = Lists.newArrayList();
              peon.getSegmentsToLoad(segment -> loading.add(segment.getIdentifier()));
              List<String> dropping = Lists.newArrayList();
              peon.getSegmentsToDrop(segment -> dropping.add(segment.getIdentifier()));
              return ImmutableMap.of("segmentsToLoad", loading, "segmentsToDrop", dropping);
            }
        )
    ).build();
  }

  @GET
  @Path("/configs/{configName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfigs(@PathParam("configName") String configName)
  {
    List<String> configNames = ImmutableList.of("common", "broker", "coordinator", "historical", "middleManager",
                                                "overlord");
    if (!configNames.contains(configName)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", "unknown nodeType")).build();
    }
    Properties properties = new Properties();
    String propertyLocation;
    try {
      if ("common".equals(configName)) {
        propertyLocation = PropertiesModule.DEFAULT_PROPERTIES_LOC;
      } else {
        propertyLocation = configName + "/runtime.properties";
      }
      PropertiesModule.load(properties, ImmutableList.of(propertyLocation));
    } catch (Exception e) {
      Response.serverError().build();
    }

    return Response.ok(Maps.fromProperties(properties)).build();
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response handleJMX(@QueryParam("dumpLongestStack") boolean dumpLongestStack)
  {
    Map<String, Object> results = JMXQueryRunnerFactory.queryJMX(coordinator.getSelf(), null, dumpLongestStack);
    return Response.ok(results).build();
  }

  private static final long DEFAULT_ASSERT_TIMEOUT = 30_000;

  @POST
  @Path("/scheduleNow")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public Response scheduleNow(
      @QueryParam("assertLoaded") boolean assertLoaded,
      @QueryParam("assertTimeout") long assertTimeout,
      Set<DataSegment> segments)
  {
    // seemed not very useful
    assertTimeout = assertTimeout <= 0 ? DEFAULT_ASSERT_TIMEOUT : assertTimeout;
    Object2LongMap<String> stats;
    try {
      stats = coordinator.scheduleNow(segments, assertLoaded, assertTimeout).getGlobalStats();
    }
    catch (Exception e) {
      // ignore
      stats = new String2LongMap();
    }
    return Response.ok(stats).build();
  }

  @POST
  @Path("/report/segment/FileNotFound/{server}")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public Response reportSegmentFileNotFound(@PathParam("server") String server, Set<DataSegment> segments)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    coordinator.reportSegmentFileNotFound(server, segments);
    return Response.ok().build();
  }

  @GET
  @Path("/loadqueue/blacklist")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBlacklist()
  {
    return Response.ok(Iterables.transform(coordinator.getBlacklisted(false), DataSegment::getIdentifier)).build();
  }

  @DELETE
  @Path("/loadqueue/blacklist")
  @Produces(MediaType.APPLICATION_JSON)
  public Response clearBlacklist()
  {
    coordinator.clearReports();
    return Response.ok().build();
  }
}
