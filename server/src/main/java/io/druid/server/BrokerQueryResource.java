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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RegexDataSource;
import io.druid.query.ResultWriter;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.UnionAllQuery;
import io.druid.query.UnionDataSource;
import io.druid.query.select.SelectForwardQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthConfig;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@Path("/druid/v2/")
public class BrokerQueryResource extends QueryResource
{
  private final CoordinatorClient coordinator;
  private final TimelineServerView brokerServerView;

  @Inject
  public BrokerQueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      AuthConfig authConfig,
      CoordinatorClient coordinator,
      TimelineServerView brokerServerView,
      @Self DruidNode node,
      QueryToolChestWarehouse warehouse,
      Map<String, ResultWriter> writerMap
  )
  {
    super(
        config,
        jsonMapper,
        smileMapper,
        texasRanger,
        emitter,
        requestLogger,
        queryManager,
        authConfig,
        node,
        warehouse,
        writerMap
    );
    this.coordinator = coordinator;
    this.brokerServerView = brokerServerView;
  }

  @POST
  @Path("/candidates")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response getQueryTargets(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used only to get request content-type and remote address
  ) throws IOException
  {
    final RequestContext context = new RequestContext(req, pretty != null);
    try {
      Query<?> query = context.getInputMapper().readValue(in, Query.class);
      return context.ok(getTargetLocations(query.getDataSource(), query.getIntervals()));
    }
    catch (Exception e) {
      return context.gotError(e);
    }
  }

  @GET
  @Path("/candidates")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryTargets(
      @QueryParam("datasource") String datasource,
      @QueryParam("intervals") String intervals,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final RequestContext context = new RequestContext(req, pretty != null);
    List<Interval> intervalList = Lists.newArrayList();
    for (String interval : intervals.split(",")) {
      intervalList.add(Interval.parse(interval.trim()));
    }
    List<LocatedSegmentDescriptor> located = getTargetLocations(
        new TableDataSource(datasource),
        JodaUtils.condenseIntervals(intervalList)
    );
    try {
      return context.ok(located);
    }
    catch (Exception e) {
      return context.gotError(e);
    }
  }

  private List<LocatedSegmentDescriptor> getTargetLocations(DataSource datasource, List<Interval> intervals)
  {
    TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(datasource);
    if (timeline == null) {
      return Collections.emptyList();
    }
    List<LocatedSegmentDescriptor> located = Lists.newArrayList();
    for (Interval interval : intervals) {
      for (TimelineObjectHolder<String, ServerSelector> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
          ServerSelector selector = chunk.getObject();
          final SegmentDescriptor descriptor = new SegmentDescriptor(
              holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
          );
          long size = selector.getSegment().getSize();
          List<DruidServerMetadata> candidates = selector.getCandidates();
          located.add(new LocatedSegmentDescriptor(descriptor, size, candidates));
        }
      }
    }
    return located;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Query prepareQuery(Query query) throws Exception
  {
    query = super.prepareQuery(query);
    query = rewriteDataSources(query);
    if (BaseQuery.optimizeQuery(query, false)) {
      query = warehouse.getToolChest(query).optimizeQuery(query, texasRanger);
    }
    if (BaseQuery.isParallelForwarding(query)) {
      // todo support partitioned group-by or join queries
      if (!(query instanceof SelectQuery || query instanceof StreamQuery)) {
        throw new IllegalArgumentException("parallel forwarding is supported only for select/stream query, for now");
      }
      if (BaseQuery.getContextBySegment(query, false)) {
        throw new IllegalArgumentException("parallel forwarding cannot be used with 'bySegment'");
      }
      URI uri = new URI(BaseQuery.getResultForwardURL(query));
      if (!"hdfs".equals(uri.getScheme())) {
        throw new IllegalArgumentException("parallel forwarding is supported only for hdfs, for now");
      }
      // make a copy
      Map<String, Object> context = Maps.newHashMap(query.getContext());

      Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);
      forwardContext.put(Query.FORWARD_PARALLEL, false);
      forwardContext.put(Query.FORWARD_PREFIX_LOCATION, true);

      query = query.withOverriddenContext(
          ImmutableMap.of(
              Query.FINALIZE, true,
              Query.FORWARD_CONTEXT, forwardContext
          )
      );
      // disable forwarding for self
      context.remove(Query.FORWARD_URL);
      context.remove(Query.FORWARD_CONTEXT);
      query = new SelectForwardQuery(query, context);
    }
    return query;
  }

  private Query rewriteDataSources(Query query)
  {
    if (query instanceof UnionAllQuery) {
      UnionAllQuery<?> union = (UnionAllQuery)query;
      if (union.getQueries() != null) {
        List<Query> rewritten = Lists.newArrayList();
        for (Query element : union.getQueries()) {
          rewritten.add(rewriteDataSources(element));
        }
        return union.withQueries(rewritten);
      }
      return union.withQuery(rewriteDataSources(union.getQuery()));
    }
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof RegexDataSource) {
      List<String> exploded = coordinator.findDatasources(dataSource.getNames());
      if (exploded.size() == 1) {
        query = query.withDataSource(new TableDataSource(exploded.get(0)));
      } else {
        query = query.withDataSource(new UnionDataSource(TableDataSource.of(exploded)));
      }
    } else if (dataSource instanceof QueryDataSource) {
      Query subQuery = rewriteDataSources(((QueryDataSource) dataSource).getQuery());
      query = query.withDataSource(new QueryDataSource(subQuery));
    }
    return query;
  }
}
