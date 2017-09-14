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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.BrokerServerView;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.Progressing;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.output.Formatters;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DummyQuery;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
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
import io.druid.segment.IndexMergerV9;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.Events;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthConfig;
import io.druid.timeline.DataSegment;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
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
  private final DataSegmentPusher pusher;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  private final ListeningExecutorService exec;

  @Inject
  public BrokerQueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      @Events Emitter eventEmitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      AuthConfig authConfig,
      CoordinatorClient coordinator,
      TimelineServerView brokerServerView,
      DataSegmentPusher pusher,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @Self DruidNode node,
      QueryToolChestWarehouse warehouse,
      IndexMergerV9 merger,
      Map<String, ResultWriter> writerMap,
      @Processing ExecutorService exec
  )
  {
    super(
        config,
        jsonMapper,
        smileMapper,
        texasRanger,
        emitter,
        eventEmitter,
        requestLogger,
        queryManager,
        authConfig,
        node,
        warehouse,
        merger,
        writerMap
    );
    this.coordinator = coordinator;
    this.brokerServerView = brokerServerView;
    this.pusher = pusher;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
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

  @Override
  @SuppressWarnings("unchecked")
  protected Sequence wrapForwardResult(Query query, Map<String, Object> forwardContext, Map<String, Object> result)
      throws IOException
  {
    if (Formatters.isIndexFormat(forwardContext) && PropUtils.parseBoolean(forwardContext, "registerTable", false)) {
      result = Maps.newLinkedHashMap(result);
      result.put("broker", node.getHostAndPort());
      result.put("queryId", query.getId());
      Map<String, Object> dataMeta = (Map<String, Object>) result.get("data");
      URI location = (URI) dataMeta.get("location");
      DataSegment segment = (DataSegment) dataMeta.get("segment");
      ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      builder.put("feed", "BrokerQueryResource");
      builder.put("broker", node.getHostAndPort());
      builder.put("payload", segment);
      if (PropUtils.parseBoolean(forwardContext, "temporary", true)) {
        log.info("Publishing index to temporary table..");
        BrokerServerView serverView = (BrokerServerView) brokerServerView;
        serverView.addedLocalSegment(segment, merger.getIndexIO().loadIndex(new File(location.getPath())), result);
        builder.put("type", "localPublish");
      } else {
        log.info("Publishing index to table..");
        pusher.push(new File(location.getPath()), segment);
        indexerMetadataStorageCoordinator.announceHistoricalSegments(Sets.newHashSet(segment));
        builder.put("type", "publish");
      }
      eventEmitter.emit(new Events.SimpleEvent(builder.put("createTime", System.currentTimeMillis()).build()));
    }
    return Sequences.simple(Arrays.asList(result));
  }

  @POST
  @Path("/load")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  @SuppressWarnings("unchecked")
  public Response loadToIndex(
      BrokerLoadSpec loadSpec,
      @QueryParam("rollup") boolean rollup,
      @QueryParam("maxOccupation") long maxOccupation,
      @QueryParam("maxRowCount") long maxRowCount,
      @QueryParam("temporary") Boolean temporary,
      @QueryParam("async") Boolean async,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req
  ) throws Exception
  {
    final DataSchema schema = loadSpec.getSchema();
    final InputRowParser parser = schema.getParser();

    final RequestContext context = new RequestContext(req, pretty != null);
    try {
      if (!(parser instanceof StringInputRowParser)) {
        throw new IllegalArgumentException("Currently supports StringInputRowParser only");
      }
      ParseSpec parseSpec = parser.getParseSpec();
      final DimensionsSpec dimensionsSpec = parseSpec.getDimensionsSpec();
      final String timestampColumn = parseSpec.getTimestampSpec().getTimestampColumn();
      if (!dimensionsSpec.hasCustomDimensions()) {
        throw new IllegalArgumentException("Need to specify dimension specs, for now");
      }
      final List<URI> locations = loadSpec.getURIs();
      final String scheme = locations.get(0).getScheme();
      final ResultWriter writer = writerMap.get(scheme);
      if (writer == null) {
        throw new IAE("Unsupported scheme '%s'", scheme);
      }
      IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withDimensionsSpec(dimensionsSpec)
          .withMetrics(schema.getAggregators())
          .withFixedSchema(true)
          .withRollup(rollup)
          .build();

      Map<String, Object> forwardContext = Maps.newHashMap();
      forwardContext.put("format", "index");
      forwardContext.put("schema", jsonMapper.convertValue(indexSchema, new TypeReference<Map<String, Object>>() { } ));
      forwardContext.put("timestampColumn", timestampColumn);
      forwardContext.put("dataSource", schema.getDataSource());
      forwardContext.put("registerTable", true);
      forwardContext.put("temporary", temporary == null || temporary);
      forwardContext.put("maxOccupation", Math.max(maxOccupation, 256 << 20));
      forwardContext.put("maxRowCount", Math.max(maxRowCount, 500000));

      File output = File.createTempFile("__druid_broker-", "-file_loader");
      output.delete();
      output.mkdirs();

      final DummyQuery<Row> query = new DummyQuery<Row>().withOverriddenContext(
          ImmutableMap.<String, Object>of(
              BaseQuery.QUERYID, UUID.randomUUID().toString(),
              Query.FORWARD_URL, "file://" + output.getAbsolutePath(),
              Query.FORWARD_CONTEXT, forwardContext,
              QueryContextKeys.POST_PROCESSING, ImmutableMap.of("type", "rowToMap") // dummy to skip tabulating
          )
      );
      log.info("Start loading.. %s into index", locations);
      final Sequence<Row> sequence = writer.read(locations, parser);
      final QueryRunner runner = wrapForward(
          query, new QueryRunner()
          {
            @Override
            public Sequence run(Query query, Map responseContext)
            {
              return Sequences.map(sequence, Rows.rowToMap(timestampColumn));
            }
          }
      );
      if (async) {
        queryManager.registerQuery(
            query, new ProgressingFuture(
                exec.submit(
                    new AbstractPrioritizedCallable<Sequence>(0)
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
    public float progress() throws IOException, InterruptedException
    {
      return progressing instanceof Progressing ? ((Progressing)progressing).progress() : -1;
    }
  }
}
