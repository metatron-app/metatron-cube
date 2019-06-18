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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.BrokerServerView;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.FilteredServerInventoryView;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.server.http.security.DatasourceResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
@Path("/druid/v2/datasources")
public class ClientInfoResource
{
  private static final Logger log = new Logger(ClientInfoResource.class);

  private static final String KEY_DIMENSIONS = "dimensions";
  private static final String KEY_METRICS = "metrics";

  private FilteredServerInventoryView serverInventoryView;
  private TimelineServerView timelineServerView;
  private SegmentMetadataQueryConfig segmentMetadataQueryConfig;
  private final AuthConfig authConfig;

  @Inject
  public ClientInfoResource(
      FilteredServerInventoryView serverInventoryView,
      TimelineServerView timelineServerView,
      SegmentMetadataQueryConfig segmentMetadataQueryConfig,
      AuthConfig authConfig
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.timelineServerView = timelineServerView;
    this.segmentMetadataQueryConfig = (segmentMetadataQueryConfig == null) ?
                                      new SegmentMetadataQueryConfig() : segmentMetadataQueryConfig;
    this.authConfig = authConfig;
  }

  private Map<String, List<DataSegment>> getSegmentsForDatasources()
  {
    final Map<String, List<DataSegment>> dataSourceMap = Maps.newHashMap();
    for (DruidServer server : serverInventoryView.getInventory()) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        if (!dataSourceMap.containsKey(dataSource.getName())) {
          dataSourceMap.put(dataSource.getName(), Lists.<DataSegment>newArrayList());
        }
        List<DataSegment> segments = dataSourceMap.get(dataSource.getName());
        segments.addAll(dataSource.getSegments());
      }
    }
    for (List<DataSegment> segments : dataSourceMap.values()) {
      Collections.sort(segments);
    }
    return dataSourceMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Iterable<String> getDataSources(@Context final HttpServletRequest request)
  {
    Set<String> dataSources = getSegmentsForDatasources().keySet();
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      return filterDataSources(request, dataSources);
    }
    return dataSources;
  }

  private Iterable<String> filterDataSources(
      final HttpServletRequest request,
      final Collection<String> dataSources
  )
  {
    final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
    final AuthorizationInfo authorizationInfo = (AuthorizationInfo) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    return Collections2.filter(
        dataSources,
        new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            Resource resource = new Resource(input, ResourceType.DATASOURCE);
            Pair<Resource, Action> key = new Pair<>(resource, Action.READ);
            Access access = resourceAccessMap.get(key);
            if (access == null) {
              resourceAccessMap.put(key, access = authorizationInfo.isAuthorized(key.lhs, key.rhs));
            }
            return access.isAllowed();
          }
        }
    );
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Map<String, Object> getDatasource(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval,
      @QueryParam("full") String full
  )
  {
    if (full == null) {
      return ImmutableMap.<String, Object>of(
          KEY_DIMENSIONS, getDatasourceDimensions(dataSourceName, interval),
          KEY_METRICS, getDatasourceMetrics(dataSourceName, interval)
      );
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = new Interval(interval);
    }

    TableDataSource dataSource = new TableDataSource(dataSourceName);
    TimelineLookup<String, ServerSelector> timeline = timelineServerView.getTimeline(dataSource);
    if (timeline == null) {
      return Collections.emptyMap();
    }
    return toClientResult(theInterval, timeline);
  }

  private Map<String, Object> toClientResult(
      Interval theInterval,
      TimelineLookup<String, ServerSelector> timeline
  )
  {
    Iterable<TimelineObjectHolder<String, ServerSelector>> serversLookup = timeline.lookup(theInterval);
    if (Iterables.isEmpty(serversLookup)) {
      return Collections.emptyMap();
    }
    Map<Interval, Object> servedIntervals = new TreeMap<>(
        new Comparator<Interval>()
        {
          @Override
          public int compare(Interval o1, Interval o2)
          {
            if (o1.equals(o2) || o1.overlaps(o2)) {
              return 0;
            } else {
              return o1.isBefore(o2) ? -1 : 1;
            }
          }
        }
    );

    for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
      final Set<Object> dimensions = Sets.newHashSet();
      final Set<Object> metrics = Sets.newHashSet();
      final PartitionHolder<ServerSelector> partitionHolder = holder.getObject();
      if (partitionHolder.isComplete()) {
        for (ServerSelector server : partitionHolder.payloads()) {
          final DataSegment segment = server.getSegment();
          dimensions.addAll(segment.getDimensions());
          metrics.addAll(segment.getMetrics());
        }
      }

      servedIntervals.put(
          holder.getInterval(),
          ImmutableMap.of(KEY_DIMENSIONS, dimensions, KEY_METRICS, metrics)
      );
    }

    //collapse intervals if they abut and have same set of columns
    Map<String, Object> result = Maps.newLinkedHashMap();
    Interval curr = null;
    Map<String, Set<String>> cols = null;
    for (Map.Entry<Interval, Object> e : servedIntervals.entrySet()) {
      Interval ival = e.getKey();
      if (curr != null && curr.abuts(ival) && cols.equals(e.getValue())) {
        curr = curr.withEnd(ival.getEnd());
      } else {
        if (curr != null) {
          result.put(curr.toString(), cols);
        }
        curr = ival;
        cols = (Map<String, Set<String>>) e.getValue();
      }
    }
    //add the last one in
    if (curr != null) {
      result.put(curr.toString(), cols);
    }
    return result;
  }

  @GET
  @Path("/{dataSourceName}/dimensions")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Iterable<String> getDatasourceDimensions(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> dims = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return dims;
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        dims.addAll(segment.getDimensions());
      }
    }

    return dims;
  }

  @GET
  @Path("/{dataSourceName}/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Iterable<String> getDatasourceMetrics(
      @PathParam("dataSourceName") String dataSourceName,
      @QueryParam("interval") String interval
  )
  {
    final List<DataSegment> segments = getSegmentsForDatasources().get(dataSourceName);
    final Set<String> metrics = Sets.newHashSet();

    if (segments == null || segments.isEmpty()) {
      return metrics;
    }

    Interval theInterval;
    if (interval == null || interval.isEmpty()) {
      DateTime now = getCurrentTime();
      theInterval = new Interval(segmentMetadataQueryConfig.getDefaultHistory(), now);
    } else {
      theInterval = new Interval(interval);
    }

    for (DataSegment segment : segments) {
      if (theInterval.overlaps(segment.getInterval())) {
        metrics.addAll(segment.getMetrics());
      }
    }

    return metrics;
  }

  @DELETE
  @Path("/local/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropLocalDataSource(@PathParam("dataSource") String dataSource, @Context final HttpServletRequest req)
  {
    log.debug("Received drop local dataSource [%s]", dataSource);
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );
      Access authResult = authorizationInfo.isAuthorized(
          new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE
      );
      if (!authResult.isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", authResult).build();
      }
    }
    if (((BrokerServerView) timelineServerView).dropLocalDataSource(dataSource)) {
      return Response.ok().build();
    }
    return Response.noContent().build();
  }

  @GET
  @Path("/local")
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getLocalDataSources(@Context HttpServletRequest request)
  {
    List<String> dataSources = ((BrokerServerView) timelineServerView).getLocalDataSources();
    if (authConfig.isEnabled()) {
      return Lists.newArrayList(filterDataSources(request, dataSources));
    }
    return dataSources;
  }

  @GET
  @Path("/local/{dataSource}/coverage")
  @Produces(MediaType.APPLICATION_JSON)
  public Interval getLocalDataSourceCoverage(
      @PathParam("dataSource") String dataSource,
      @Context HttpServletRequest request
  )
  {
    if (authConfig.isEnabled() &&
        Iterables.isEmpty(filterDataSources(request, Arrays.asList(dataSource)))) {
      return null;
    }
    return ((BrokerServerView) timelineServerView).getLocalDataSourceCoverage(dataSource);
  }

  @GET
  @Path("/local/check/{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getLocalDataSourceMeta(
      @PathParam("queryId") String queryId,
      @Context HttpServletRequest request)
  {
    List<String> dataSources = ((BrokerServerView) timelineServerView).getLocalDataSources();
    if (authConfig.isEnabled()) {
      dataSources = Lists.newArrayList(filterDataSources(request, dataSources));
    }
    return ((BrokerServerView) timelineServerView).getLocalDataSourceMeta(dataSources, queryId);
  }

  protected DateTime getCurrentTime()
  {
    return new DateTime();
  }
}
