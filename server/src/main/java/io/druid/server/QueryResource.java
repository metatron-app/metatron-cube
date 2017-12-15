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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.PropUtils;
import io.druid.data.output.Formatters;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DruidMetrics;
import io.druid.query.JoinQuery;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.ResultWriter;
import io.druid.query.TabularFormat;
import io.druid.query.UnionAllQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.Events;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  protected static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  protected static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;

  protected final ServerConfig config;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final QuerySegmentWalker.Wrapper texasRanger;
  protected final ServiceEmitter emitter;
  protected final RequestLogger requestLogger;
  protected final Emitter eventEmitter;
  protected final QueryManager queryManager;
  protected final AuthConfig authConfig;

  protected final DruidNode node;
  protected final QueryToolChestWarehouse warehouse;
  protected final Map<String, ResultWriter> writerMap;
  protected final IndexMergerV9 merger;

  @Inject
  public QueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      final QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      @Events Emitter eventEmitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      AuthConfig authConfig,
      @Self DruidNode node,
      QueryToolChestWarehouse warehouse,
      IndexMergerV9 merger,
      Map<String, ResultWriter> writerMap
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.emitter = emitter;
    this.eventEmitter = eventEmitter;
    this.requestLogger = requestLogger;
    this.queryManager = queryManager;
    this.authConfig = authConfig;
    this.node = node;
    this.warehouse = warehouse;
    this.merger = merger;
    this.writerMap = writerMap;
    this.texasRanger = new QuerySegmentWalker.Wrapper(texasRanger)
    {
      @Override
      @SuppressWarnings("unchecked")
      protected <T> QueryRunner<T> wrap(Query<T> query, QueryRunner<T> runner) throws Exception
      {
        return wrapForward(query, runner);
      }
    };
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelQuery(@PathParam("id") String queryId, @Context final HttpServletRequest req)
  {
    log.info("Received cancel request for query [%s]", queryId);
    Set<String> dataSources = queryManager.getQueryDatasources(queryId);
    if (dataSources != null && !dataSources.isEmpty()) {
      Access access = authorizeDS(dataSources, Action.WRITE, req);
      if (access != null) {
        return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", access).build();
      }
    }
    queryManager.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used to get request content-type, remote address and AuthorizationInfo
  ) throws IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    String queryId = null;

    final RequestContext context = new RequestContext(req, pretty != null);
    final String contentType = context.getContentType();
    final ObjectWriter jsonWriter = context.getOutputWriter();

    final Thread currentThread = Thread.currentThread();
    final String currThreadName = currentThread.getName();
    try {
      query = context.getInputMapper().readValue(in, Query.class);
      queryId = query.getId();
      if (queryId == null) {
        queryId = UUID.randomUUID().toString();
        query = query.withId(queryId);
      }
      if (query.getContextValue(QueryContextKeys.TIMEOUT) == null) {
        query = query.withOverriddenContext(
            ImmutableMap.of(
                QueryContextKeys.TIMEOUT,
                config.getMaxIdleTime().toStandardDuration().getMillis()
            )
        );
      }

      log.info("Got query [%s]", log.isDebugEnabled() ? query : queryId + ":" + query.getType());
      currentThread.setName(String.format("%s[%s_%s]", currThreadName, query.getType(), queryId));

      final Query prepared = prepareQuery(query);

      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      Access access = authorize(prepared, req);
      if (access != null) {
        return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", access).build();
      }

      // concurrent hashmap
      final Map<String, Object> responseContext = new MapMaker().makeMap();
      final Sequence results = prepared.run(texasRanger, responseContext);

      final Yielder yielder = results.toYielder(
          null,
          new YieldingAccumulator()
          {
            @Override
            public Object accumulate(Object accumulated, Object in)
            {
              yield();
              return in;
            }
          }
      );

      try {
        final Query theQuery = query;
        Response.ResponseBuilder builder = Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream outputStream) throws IOException, WebApplicationException
                  {
                    // json serializer will always close the yielder
                    CountingOutputStream os = new CountingOutputStream(outputStream);
                    jsonWriter.writeValue(os, yielder);

                    os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                    os.close();

                    final long queryTime = System.currentTimeMillis() - start;
                    emitter.emit(
                        DruidMetrics.makeQueryTimeMetric(jsonMapper, theQuery, req.getRemoteAddr())
                                    .setDimension("success", "true")
                                    .build("query/time", queryTime)
                    );
                    emitter.emit(
                        DruidMetrics.makeQueryTimeMetric(jsonMapper, theQuery, req.getRemoteAddr())
                                    .build("query/bytes", os.getCount())
                    );

                    requestLogger.log(
                        new RequestLogLine(
                            new DateTime(start),
                            req.getRemoteAddr(),
                            toLoggingQuery(theQuery),
                            new QueryStats(
                                ImmutableMap.<String, Object>of(
                                    "query/time", queryTime,
                                    "query/bytes", os.getCount(),
                                    "success", true
                                )
                            )
                        )
                    );
                  }
                },
                contentType
            )
            .header("X-Druid-Query-Id", queryId);

        //Limit the response-context header, see https://github.com/druid-io/druid/issues/2331
        //Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
        //and encodes the string using ASCII, so 1 char is = 1 byte
        String responseCtxString = jsonMapper.writeValueAsString(responseContext);
        if (responseCtxString.length() > RESPONSE_CTX_HEADER_LEN_LIMIT) {
          log.warn("Response Context truncated for id [%s] . Full context is [%s].", queryId, responseCtxString);
          responseCtxString = responseCtxString.substring(0, RESPONSE_CTX_HEADER_LEN_LIMIT);
        }

        return builder
            .header("X-Druid-Response-Context", responseCtxString)
            .build();
      }
      catch (Exception e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder.close();
        throw Throwables.propagate(e);
      }
      finally {
        // do not close yielder here, since we do not want to close the yielder prior to
        // StreamingOutput having iterated over all the results
      }
    }
    catch (QueryInterruptedException e) {
      try {
        log.info("%s [%s]", e.getMessage(), queryId);
        final long queryTime = System.currentTimeMillis() - start;
        emitter.emit(
            DruidMetrics.makeQueryTimeMetric(jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(start),
                req.getRemoteAddr(),
                toLoggingQuery(query),
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "query/time",
                        queryTime,
                        "success",
                        false,
                        "interrupted",
                        true,
                        "reason",
                        e.toString()
                    )
                )
            )
        );
      }
      catch (Exception e2) {
        log.error(e2, "Unable to log query [%s]!", query);
      }
      return context.gotError(e);
    }
    catch (Throwable e) {
      // Input stream has already been consumed by the json object mapper if query == null
      final String queryString =
          query == null
          ? "unparsable query"
          : query.toString();

      log.warn(e, "Exception occurred on request [%s]", queryString);

      try {
        final long queryTime = System.currentTimeMillis() - start;
        emitter.emit(
            DruidMetrics.makeQueryTimeMetric(jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(start),
                req.getRemoteAddr(),
                toLoggingQuery(query),
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "query/time",
                        queryTime,
                        "success",
                        false,
                        "exception",
                        e.toString()
                    )
                )
            )
        );
      }
      catch (Exception e2) {
        log.error(e2, "Unable to log query [%s]!", queryString);
      }

      log.makeAlert(e, "Exception handling request")
         .addData("exception", e.toString())
         .addData("query", queryString)
         .addData("peer", req.getRemoteAddr())
         .emit();

      return context.gotError(e);
    }
    finally {
      currentThread.setName(currThreadName);
    }
  }

  // suppress excessive interval logging in historical node
  protected Query toLoggingQuery(Query<?> query)
  {
    return query.withQuerySegmentSpec(
        new MultipleIntervalSegmentSpec(Arrays.asList(JodaUtils.umbrellaInterval(query.getIntervals())))
    );
  }

  protected Access authorize(Query query, HttpServletRequest req)
  {
    return authorizeDS(query.getDataSource().getNames(), Action.READ, req);
  }

  protected Access authorizeDS(Iterable<String> dataSources, Action action, HttpServletRequest req)
  {
    if (authConfig.isEnabled()) {
      AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      if (authorizationInfo == null) {
        throw new ISE("Security is enabled but no authorization info found in the request");
      }
      for (String dataSource : dataSources) {
        Resource resource = new Resource(dataSource, ResourceType.DATASOURCE);
        Access authResult = authorizationInfo.isAuthorized(resource, action);
        if (!authResult.isAllowed()) {
          return authResult;
        }
      }
    }
    return null;
  }

  protected Query prepareQuery(Query query) throws Exception
  {
    Query rewritten = rewriteQuery(query);
    if (query != rewritten) {
      log.info("Base query is rewritten to %s", rewritten);
    }
    return rewritten;
  }

  private Query rewriteQuery(final Query query)
  {
    Query rewritten = query;
    if (query.getDataSource() instanceof QueryDataSource) {
      Query source = ((QueryDataSource) query.getDataSource()).getQuery();
      if (source instanceof Query.RewritingQuery) {
        if (source.getId() == null) {
          source = source.withId(query.getId());
        }
        Query element = rewriteQuery(source);
        rewritten = rewritten.withDataSource(new QueryDataSource(element));
      }
    }
    if (query instanceof JoinQuery) {
      JoinQuery<?> joinQuery = (JoinQuery)query;
      for (Map.Entry<String, DataSource> entry : joinQuery.getDataSources().entrySet()) {
        if (entry.getValue() instanceof QueryDataSource) {
          Query source = ((QueryDataSource) entry.getValue()).getQuery();
          if (source.getId() == null) {
            source = source.withId(query.getId());
          }
          Query element = rewriteQuery(source);
          entry.setValue(new QueryDataSource(element));
        }
      }
    }
    if (rewritten instanceof UnionAllQuery) {
      UnionAllQuery<?> union = (UnionAllQuery) rewritten;
      if (union.getQuery() != null) {
        Query source = union.getQuery();
        if (source.getId() == null) {
          source = source.withId(query.getId());
        }
        Query element = rewriteQuery(source);
        rewritten = union.withQuery(element);
      } else {
        List<Query> queries = Lists.newArrayList();
        for (Query source : union.getQueries()) {
          if (source.getId() == null) {
            source = source.withId(query.getId());
          }
          Query element = rewriteQuery(source);
          queries.add(element);
        }
        rewritten = union.withQueries(queries);
      }
    }
    if (query instanceof Query.RewritingQuery) {
      rewritten = ((Query.RewritingQuery) query).rewriteQuery(texasRanger, jsonMapper);
    }
    return rewritten;
  }

  protected class RequestContext
  {
    final boolean isSmile;
    final boolean isPretty;
    final String contentType;

    RequestContext(HttpServletRequest request, boolean pretty)
    {
      String requestType = request.getContentType();
      isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType)
                || APPLICATION_SMILE.equals(requestType);
      isPretty = pretty;
      contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    }

    String getContentType()
    {
      return contentType;
    }

    ObjectMapper getInputMapper()
    {
      return isSmile ? smileMapper : jsonMapper;
    }

    ObjectWriter getOutputWriter()
    {
      ObjectMapper mapper = getInputMapper();
      return isPretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer();
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(getOutputWriter().writeValueAsString(object), contentType).build();
    }

    Response gotError(Throwable e) throws IOException
    {
      return Response.serverError()
                     .type(contentType)
                     .entity(
                         getOutputWriter().writeValueAsBytes(
                             QueryInterruptedException.wrapIfNeeded(e, node.getHostAndPort())
                         )
                     )
                     .build();
    }
  }

  @SuppressWarnings("unchecked")
  protected <T> QueryRunner wrapForward(final Query<T> query, final QueryRunner<T> baseRunner) throws Exception
  {
    final URI uri = getForwardURI(query);
    if (uri == null) {
      return baseRunner;
    }
    final String scheme = uri.getScheme() == null ? ResultWriter.FILE_SCHEME : uri.getScheme();

    final ResultWriter writer = scheme.equals("null") ? ResultWriter.NULL : writerMap.get(uri.getScheme());
    if (writer == null) {
      log.warn("Unsupported scheme '" + uri.getScheme() + "'");
      throw new IAE("Unsupported scheme '%s'", uri.getScheme());
    }
    final Map<String, Object> forwardContext = BaseQuery.getResultForwardContext(query);

    if (Formatters.isIndexFormat(forwardContext)) {
      String indexSchema = Objects.toString(forwardContext.get("schema"), null);
      String indexInterval = Objects.toString(forwardContext.get("interval"), null);
      if (Strings.isNullOrEmpty(indexSchema)) {
        IncrementalIndexSchema schema = Queries.relaySchema(query, texasRanger.getDelegate());
        log.info(
            "Resolved index schema.. dimensions: %s, metrics: %s",
            schema.getDimensionsSpec().getDimensionNames(),
            Arrays.toString(schema.getMetrics())
        );
        forwardContext.put(
            "schema",
            jsonMapper.convertValue(schema, new TypeReference<Map<String, Object>>() { })
        );
        if (indexInterval == null || jsonMapper.readValue(indexInterval, Interval.class) == null) {
          Interval dataInterval = JodaUtils.umbrellaInterval(query.getQuerySegmentSpec().getIntervals());
          forwardContext.put("interval", dataInterval.toString());
        }
      }
    }

    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        URI rewritten = uri;
        try {
          if (PropUtils.parseBoolean(forwardContext, Query.FORWARD_PREFIX_LOCATION)) {
            rewritten = rewriteURI(uri, scheme, null, uri.getPath() + "/" + node.toPathName());
          }
          if (scheme.equals(ResultWriter.FILE_SCHEME) || scheme.equals(LocalDataStorageDruidModule.SCHEME)) {
            rewritten = rewriteURI(uri, scheme, node, null);
          }
          TabularFormat result = toTabularFormat(removeForwardContext(query), responseContext);
          return wrapForwardResult(query, forwardContext, writer.write(rewritten, result, forwardContext));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      private TabularFormat toTabularFormat(final Query query, final Map responseContext)
      {
        // union-all does not have toolchest. delegate it to inner query
        Query representative = BaseQuery.getRepresentative(query);
        if (PostProcessingOperators.isTabularOutput(query, jsonMapper)) {
          // already converted to tabular format
          return new TabularFormat()
          {
            @Override
            public Sequence getSequence() { return baseRunner.run(query, responseContext); }

            @Override
            public Map<String, Object> getMetaData() { return null; }
          };
        }
        String timestampColumn = PropUtils.parseString(forwardContext, Query.FORWARD_TIMESTAMP_COLUMN);
        return warehouse.getToolChest(representative).toTabularFormat(
            baseRunner.run(query, responseContext),
            timestampColumn
        );
      }
    };
  }

  // remove forward context (except select forward query) for historical, etc.
  private Query removeForwardContext(Query query)
  {
    if (query instanceof UnionAllQuery) {
      UnionAllQuery<?> union = (UnionAllQuery)query;
      if (union.getQueries() != null) {
        List<Query> rewritten = Lists.newArrayList();
        for (Query element : union.getQueries()) {
          rewritten.add(removeForwardContext(element));
        }
        return union.withQueries(rewritten);
      }
      return union.withQuery(removeForwardContext(union.getQuery()));
    }
    Map<String, String> override = ImmutableMap.of(Query.FORWARD_URL, "", Query.FORWARD_CONTEXT, "");
    if (query.getDataSource() instanceof QueryDataSource) {
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
      query = query.withDataSource(new QueryDataSource(dataSource.getQuery().withOverriddenContext(override)));
    }
    return query.withOverriddenContext(override);
  }

  private static URI getForwardURI(Query query) throws URISyntaxException
  {
    String forwardURL = BaseQuery.getResultForwardURL(query);
    if (!Strings.isNullOrEmpty(forwardURL)) {
      return new URI(forwardURL);
    }
    return null;
  }

  private static URI rewriteURI(URI uri, String scheme, DruidNode node, String path) throws URISyntaxException
  {
    return new URI(
        scheme,
        uri.getUserInfo(),
        node == null ? uri.getHost() : node.getHost(),
        node == null ? uri.getPort() : node.getPort(),
        path == null ? uri.getPath() : path,
        uri.getQuery(),
        uri.getFragment()
    );
  }

  protected Sequence wrapForwardResult(Query query, Map<String, Object> forwardContext, Map<String, Object> result)
      throws IOException
  {
    return Sequences.simple(Arrays.asList(result));
  }
}
