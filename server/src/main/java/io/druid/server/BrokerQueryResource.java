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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.Progressing;
import io.druid.common.utils.JodaUtils;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.output.ForwardConstants;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DummyQuery;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.RegexDataSource;
import io.druid.query.SegmentDescriptor;
import io.druid.query.StorageHandler;
import io.druid.query.TableDataSource;
import io.druid.query.UnionDataSource;
import io.druid.query.select.SelectForwardQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
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
import javax.ws.rs.PathParam;
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
import java.util.UUID;
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
      @Self DruidNode node,
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryManager queryManager,
      QuerySegmentWalker segmentWalker,
      QueryToolChestWarehouse warehouse,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      AuthConfig authConfig,
      CoordinatorClient coordinator,
      TimelineServerView brokerServerView,
      @Processing ExecutorService exec
  )
  {
    super(
        node,
        config,
        jsonMapper,
        smileMapper,
        queryManager,
        segmentWalker,
        warehouse,
        emitter,
        requestLogger,
        authConfig
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
    final RequestContext context = new RequestContext(req, pretty != null);
    try {
      Query<?> query = context.getInputMapper(false).readValue(in, Query.class);
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
  protected Query toLoggingQuery(Query<?> query)
  {
    return query;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Query prepareQuery(Query baseQuery) throws Exception
  {
    Query prepared = super.prepareQuery(baseQuery);

    Query query = rewriteDataSource(prepared);
    query = QueryUtils.rewriteRecursively(query, segmentWalker, warehouse.getQueryConfig());
    query = QueryUtils.resolveRecursively(query, segmentWalker);

    if (BaseQuery.isOptimizeQuery(query, false)) {
      QueryToolChest toolChest = warehouse.getToolChest(query);
      if (toolChest != null) {
        query = toolChest.optimizeQuery(query, segmentWalker);
      }
    }

    if (query != prepared) {
      log.info("Base query is rewritten to %s", query);
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
      Map<String, Object> context = BaseQuery.copyContext(query);

      Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);
      forwardContext.put(Query.FORWARD_PARALLEL, false);
      forwardContext.put(Query.LOCAL_POST_PROCESSING, true);

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

  private Query rewriteDataSource(Query query)
  {
    return Queries.iterate(
        query, new Function<Query, Query>()
        {
          @Override
          public Query apply(Query input)
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
      BrokerLoadSpec loadSpec,
      @Deprecated @QueryParam("rollup") Boolean rollup,
      @QueryParam("temporary") Boolean temporary,
      @QueryParam("async") Boolean async,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req
  ) throws Exception
  {
    if (!(segmentWalker instanceof ForwardingSegmentWalker)) {
      throw new UnsupportedOperationException("loadToIndex");
    }
    final RequestContext context = new RequestContext(req, pretty != null);
    try {
      final DataSchema schema = loadSpec.getSchema();
      final InputRowParser parser = loadSpec.getParser();

      List<URI> locations = loadSpec.getURIs();
      String scheme = locations.get(0).getScheme();
      StorageHandler writer = ((ForwardingSegmentWalker) segmentWalker).getHandler(scheme);
      if (writer == null) {
        throw new IAE("Unsupported scheme '%s'", scheme);
      }

      final BaseTuningConfig tuningConfig = loadSpec.getTuningConfig();
      final GranularitySpec granularitySpec = schema.getGranularitySpec();

      IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withDimensionsSpec(parser.getDimensionsSpec())
          .withMetrics(schema.getAggregators())
          .withQueryGranularity(granularitySpec.getQueryGranularity())
          .withSegmentGranularity(granularitySpec.getSegmentGranularity())
          .withRollup(rollup == null ? granularitySpec.isRollup() : rollup)
          .withFixedSchema(true)
          .build();

      Map<String, Object> forwardContext = Maps.newHashMap(loadSpec.getProperties());
      forwardContext.put("format", "index");
      forwardContext.put("schema", jsonMapper.convertValue(indexSchema, new TypeReference<Map<String, Object>>() { } ));
      forwardContext.put("tuningConfig", jsonMapper.convertValue(tuningConfig, new TypeReference<Map<String, Object>>() { } ));
      forwardContext.put("timestampColumn", Row.TIME_COLUMN_NAME);
      forwardContext.put("dataSource", schema.getDataSource());
      forwardContext.put("registerTable", true);
      forwardContext.put("temporary", temporary == null || temporary);
      forwardContext.put("segmentGranularity", granularitySpec.getSegmentGranularity());

      final DummyQuery<Row> query = new DummyQuery<Row>().withOverriddenContext(
          ImmutableMap.<String, Object>of(
              BaseQuery.QUERYID, UUID.randomUUID().toString(),
              Query.FORWARD_URL, ForwardConstants.LOCAL_TEMP_URL,
              Query.FORWARD_CONTEXT, forwardContext,
              QueryContextKeys.POST_PROCESSING, ImmutableMap.of("type", "rowToMap") // dummy to skip tabulating
          )
      );
      log.info("Start loading.. %s into index", locations);

      Map<String, Object> loadContext = Maps.newHashMap(forwardContext);
      loadContext.put("skipFirstN", loadSpec.getSkipFirstN());
      loadContext.put("ignoreInvalidRows", tuningConfig != null && tuningConfig.isIgnoreInvalidRows());

      final Sequence<Row> sequence = writer.read(locations, parser, loadContext);
      final QueryRunner runner = ((ForwardingSegmentWalker) segmentWalker).wrap(
          query, new QueryRunner()
          {
            @Override
            public Sequence<Map<String, Object>> run(Query query, Map responseContext)
            {
              return Sequences.map(sequence, Rows.rowToMap(Row.TIME_COLUMN_NAME));
            }
          }
      );
      if (async) {
        queryManager.registerQuery(
            query, new ProgressingFuture(
                exec.submit(
                    new PrioritizedCallable.Background<Sequence>()
                    {
                      @Override
                      public Sequence call() throws Exception
                      {
                        return runner.run(query, Maps.newHashMap());
                      }
                    }
                ), sequence
            )
        );
        return context.ok(ImmutableMap.of("queryId", query.getId(), "broker", node.getHostAndPort()));
      } else {
        List result = Sequences.toList(runner.run(query, Maps.newHashMap()), Lists.newArrayList());
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
