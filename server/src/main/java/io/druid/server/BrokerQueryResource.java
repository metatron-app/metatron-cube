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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.selector.ServerSelector;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.guice.LocalDataStorageDruidModule;
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
import io.druid.query.TabularFormat;
import io.druid.query.UnionDataSource;
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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@Path("/druid/v2/")
public class BrokerQueryResource extends QueryResource
{
  private final DruidNode node;
  private final CoordinatorClient coordinator;
  private final TimelineServerView brokerServerView;
  private final QueryToolChestWarehouse warehouse;
  private final Map<String, ResultWriter> writerMap;

  @Inject
  public BrokerQueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @Self DruidNode node,
      QuerySegmentWalker texasRanger,
      QueryToolChestWarehouse warehouse,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      AuthConfig authConfig,
      Map<String, ResultWriter> writerMap,
      CoordinatorClient coordinator,
      TimelineServerView brokerServerView
  )
  {
    super(config, jsonMapper, smileMapper, texasRanger, emitter, requestLogger, queryManager, authConfig);
    this.node = node;
    this.coordinator = coordinator;
    this.brokerServerView = brokerServerView;
    this.warehouse = warehouse;
    this.writerMap = writerMap;
    log.info("Supporting writer schemes.. " + writerMap.keySet());
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

  @Override @SuppressWarnings("unchecked")
  protected Sequence toDispatchSequence(Query query, Sequence res) throws IOException
  {
    String forwardURL = BaseQuery.getResultForwardURL(query);
    if (forwardURL != null && !StringUtils.isNullOrEmpty(forwardURL)) {
      URI uri;
      try {
        uri = new URI(forwardURL);
        String scheme = uri.getScheme() == null ? ResultWriter.FILE_SCHEME : uri.getScheme();
        if (scheme.equals(ResultWriter.FILE_SCHEME) || scheme.equals(LocalDataStorageDruidModule.SCHEME)) {
          uri = rewriteURI(uri, scheme, node);
        }
      }
      catch (URISyntaxException e) {
        log.warn("Invalid uri `" + forwardURL + "`", e);
        return Sequences.empty();
      }

      ResultWriter writer = writerMap.get(uri.getScheme());
      if (writer == null) {
        log.warn("Unsupported scheme `" + uri.getScheme() + "`");
        return Sequences.empty();
      }
      TabularFormat result = warehouse.getToolChest(query).toTabularFormat(res);
      Map<String, Object> info = writer.write(uri, result, BaseQuery.getResultForwardContext(query));

      return Sequences.simple(Arrays.asList(info));
    }

    return super.toDispatchSequence(query, res);
  }

  private URI rewriteURI(URI uri, String scheme, DruidNode node) throws URISyntaxException
  {
    return new URI(
        scheme,
        uri.getUserInfo(),
        node.getHost(),
        node.getPort(),
        uri.getPath(),
        uri.getQuery(),
        uri.getFragment()
    );
  }

  @Override @SuppressWarnings("unchecked")
  protected Query prepareQuery(Query query)
  {
    query = rewriteDataSources(query);
    if (BaseQuery.rewriteQuery(query, false)) {
      query = warehouse.getToolChest(query).rewriteQuery(query, texasRanger);
    }
    return query;
  }

  private Query rewriteDataSources(Query query)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof RegexDataSource) {
      List<String> exploded = coordinator.findDatasources(dataSource.getNames());
      if (exploded.size() == 1) {
        query = query.withDataSource(new TableDataSource(exploded.get(0)));
      } else {
        query = query.withDataSource(new UnionDataSource(TableDataSource.of(exploded)));
      }
    } else if (dataSource instanceof QueryDataSource) {
      Query subQuery = rewriteDataSources(((QueryDataSource) query).getQuery());
      query = query.withDataSource(new QueryDataSource(subQuery));
    }
    return query;
  }
}
