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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.Progressing;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Logs;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.output.ForwardConstants;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.IAE;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DataSources;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.QueryVisitor;
import io.druid.query.RegexDataSource;
import io.druid.query.RowSignature;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.select.SelectForwardQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
@Path("/druid/v2/")
public class BrokerQueryResource extends QueryResource
{
  private final CoordinatorClient coordinator;
  private final TimelineServerView brokerServerView;
  private final ListeningExecutorService exec;

  @Inject
  public BrokerQueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Self DruidNode node,
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryManager queryManager,
      QuerySegmentWalker segmentWalker,
      QueryToolChestWarehouse warehouse,
      AuthorizerMapper authorizerMapper,
      CoordinatorClient coordinator,
      TimelineServerView brokerServerView,
      @Processing ExecutorService exec
  )
  {
    super(
        queryLifecycleFactory,
        node,
        config,
        jsonMapper,
        smileMapper,
        queryManager,
        segmentWalker,
        warehouse,
        authorizerMapper
    );
    this.coordinator = coordinator;
    this.brokerServerView = brokerServerView;
    this.exec = MoreExecutors.listeningDecorator(exec);
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
    RequestContext context = new RequestContext(req, pretty != null);
    Query<?> query;
    try {
      query = context.getInputMapper(false).readValue(in, Query.class);
    }
    catch (Exception e) {
      return context.gotError(e, Status.BAD_REQUEST);
    }
    return context.ok(getTargetLocations(DataSources.getName(query), query.getIntervals()));
  }

  @GET
  @Path("/candidates")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQueryTargets(
      @QueryParam("datasource") String datasource,
      @QueryParam("intervals") String intervals,
      @QueryParam("pretty") String pretty,
      @Context HttpServletRequest req
  ) throws IOException
  {
    final RequestContext context = new RequestContext(req, pretty != null);
    List<Interval> intervalList = Lists.newArrayList();
    for (String interval : intervals.split(",")) {
      intervalList.add(Interval.parse(interval.trim()));
    }
    List<LocatedSegmentDescriptor> located = getTargetLocations(
        datasource, JodaUtils.condenseIntervals(intervalList)
    );
    try {
      return context.ok(located);
    }
    catch (Exception e) {
      return context.gotError(e);
    }
  }

  @POST
  @Path("/geojson")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response getFeatures(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @QueryParam("smile") String smile,
      @Context final HttpServletRequest req // used to get request content-type, remote address and AuthorizationInfo
  ) throws IOException
  {
    req.setAttribute(GET_FEATURE, true);    // marker
    return doPost(in, pretty, smile, req);
  }

  private List<LocatedSegmentDescriptor> getTargetLocations(String dataSourceName, List<Interval> intervals)
  {
    TimelineLookup<ServerSelector> timeline = brokerServerView.getTimeline(dataSourceName);
    if (timeline == null) {
      return Collections.emptyList();
    }
    List<LocatedSegmentDescriptor> located = Lists.newArrayList();
    for (Interval interval : intervals) {
      for (TimelineObjectHolder<ServerSelector> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
          ServerSelector selector = chunk.getObject();
          final SegmentDescriptor descriptor = new SegmentDescriptor(
              dataSourceName, holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
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
  protected Query toLoggingQuery(Query<?> query)
  {
    return QueryUtils.forLog(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Query prepareQuery(Query baseQuery, RequestContext context) throws Exception
  {
    Query prepared = super.prepareQuery(baseQuery, context);
    if (context.request.getAttribute(GET_FEATURE) != null) {
      RowSignature signature = Queries.relaySchema(prepared, segmentWalker);
      int geomIndex = signature.getColumnTypes().indexOf(ValueDesc.GEOMETRY);
      Map<String, Object> decorator = ImmutableMap.of(
          "format", "geojson",
          "geomIndex", geomIndex,
          "columnNames", signature.getColumnNames()
      );
      prepared = prepared.withOverriddenContext(ImmutableMap.of(Query.DECORATOR_CONTEXT, decorator));
    }

    Query query = rewriteDataSource(prepared);
    query = QueryUtils.rewriteRecursively(query, segmentWalker);
    query = QueryUtils.resolveRecursively(query, segmentWalker);

    if (query.getContextBoolean(Query.OPTIMIZE_QUERY, false)) {
      QueryToolChest toolChest = warehouse.getToolChest(query);
      if (toolChest != null) {
        query = toolChest.optimizeQuery(query, segmentWalker);
      }
    }

    if (query != prepared) {
      Query logging = query;
      log.debug("Base query is rewritten to %s", Logs.lazy(() -> toLoggingQuery(logging).toString()));
    }

    if (BaseQuery.isParallelForwarding(query)) {
      // todo support partitioned group-by or join queries
      if (!(query instanceof SelectQuery || query instanceof StreamQuery)) {
        throw new IllegalArgumentException("parallel forwarding is supported only for select/stream query, for now");
      }
      if (BaseQuery.isBySegment(query)) {
        throw new IllegalArgumentException("parallel forwarding cannot be used with 'bySegment'");
      }
      URI uri = new URI(BaseQuery.getResultForwardURL(query));
      if (!"hdfs".equals(uri.getScheme())) {
        throw new IllegalArgumentException("parallel forwarding is supported only for hdfs, for now");
      }
      // make a copy
      Map<String, Object> forward = BaseQuery.copyContext(query);

      Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);
      forwardContext.put(ForwardConstants.PARALLEL, false);
      forwardContext.put(ForwardConstants.REWRITE_URI_WITH_LOCAL, true);

      query = query.withOverriddenContext(
          ImmutableMap.of(
              Query.FINALIZE, true,
              Query.FORWARD_CONTEXT, forwardContext
          )
      );
      // disable forwarding for self
      forward.remove(Query.FORWARD_URL);
      forward.remove(Query.FORWARD_CONTEXT);
      query = new SelectForwardQuery(query, forward);
    }
    return query;
  }

  private Query rewriteDataSource(Query query)
  {
    return Queries.iterate(
        query, new QueryVisitor()
        {
          @Override
          public Query out(Query input)
          {
            DataSource dataSource = input.getDataSource();
            if (dataSource instanceof RegexDataSource) {
              List<String> exploded = coordinator.findDatasources(dataSource.getNames());
              if (exploded.isEmpty()) {
                throw new IAE("cannot find matching datasource from regex %s", dataSource.getNames());
              }
              if (exploded.size() == 1) {
                input = input.withDataSource(TableDataSource.of(exploded.get(0)));
              } else {
                input = input.withDataSource(UnionDataSource.of(exploded));
              }
            }
            return input;
          }
        }
    );
  }

  @POST
  @Path("/load")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  @SuppressWarnings("unchecked")
  public Response loadToIndex(
      FileLoadSpec loadSpec,
      @QueryParam("async") Boolean async,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req
  ) throws Exception
  {
    if (!(segmentWalker instanceof ForwardingSegmentWalker)) {
      throw new UnsupportedOperationException("loadToIndex");
    }
    final ForwardingSegmentWalker forward = (ForwardingSegmentWalker) segmentWalker;
    final RequestContext context = new RequestContext(req, pretty != null);

    final Pair<Query, Sequence> pair;
    try {
      pair = loadSpec.readFrom(forward);
    }
    catch (IOException e) {
      return context.gotError(e, Status.BAD_REQUEST);
    }
    log.info("Start loading.. %s into index", loadSpec.getPaths());
    try {
      final Query query = pair.lhs;         // dummy forward query
      final Sequence sequence = pair.rhs;   // progressing sequence
      final QueryRunner runner = forward.handle(query, QueryRunners.wrap(sequence));
      if (async) {
        final ListenableFuture<Sequence> future = exec.submit(QueryRunners.asCallable(runner, query));
        queryManager.register(query, new ProgressingFuture(future, sequence));
        return context.ok(ImmutableMap.of("queryId", query.getId(), "broker", node.getHostAndPort()));
      } else {
        List result = Sequences.toList(QueryRunners.run(query, runner));
        return context.ok(result.size() == 1 ? result.get(0) : result);
      }
    }
    catch (Throwable e) {
      log.warn(e, "Failed loading");
      return context.gotError(e);
    }
  }

  @GET
  @Path("/progress/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(
      @PathParam("id") String queryId,
      @Context final HttpServletRequest req
  ) throws Exception
  {
    final RequestContext context = new RequestContext(req, false);
    try {
      return context.ok(queryManager.progress(queryId));
    }
    catch (Throwable e) {
      return context.gotError(e);
    }
  }

  @GET
  @Path("/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningIds(@Context final HttpServletRequest req) throws Exception
  {
    final RequestContext context = new RequestContext(req, false);
    try {
      return context.ok(queryManager.getRunningQueryStatus());
    }
    catch (Throwable e) {
      return context.gotError(e);
    }
  }

  private static class ProgressingFuture<T> extends ForwardingListenableFuture<T> implements Progressing
  {
    private final ListenableFuture<T> delegate;
    private final Object progressing;

    private ProgressingFuture(ListenableFuture<T> delegate, Object progressing)
    {
      this.delegate = delegate;
      this.progressing = progressing;
    }

    @Override
    protected ListenableFuture<T> delegate()
    {
      return delegate;
    }

    @Override
    public float progress()
    {
      return progressing instanceof Progressing ? ((Progressing)progressing).progress() : -1;
    }
  }
}
