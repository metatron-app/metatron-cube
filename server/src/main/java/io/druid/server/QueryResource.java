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
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.common.utils.JodaUtils;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Self;
import io.druid.jackson.JodaStuff;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.filter.DimFilters;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang.mutable.MutableInt;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  protected static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  protected static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;

  protected final QueryLifecycleFactory queryLifecycleFactory;
  protected final DruidNode node;
  protected final ServerConfig config;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final ObjectMapper jsonCustomMapper;
  protected final ObjectMapper smileCustomMapper;

  protected final QueryManager queryManager;
  protected final QuerySegmentWalker segmentWalker;
  protected final QueryToolChestWarehouse warehouse;

  protected final AuthConfig authConfig;

  @Inject
  public QueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Self DruidNode node,
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryManager queryManager,
      QuerySegmentWalker segmentWalker,
      QueryToolChestWarehouse warehouse,
      AuthConfig authConfig
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.node = node;
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.queryManager = queryManager;
    this.segmentWalker = segmentWalker;
    this.warehouse = warehouse;
    this.authConfig = authConfig;
    this.jsonCustomMapper = JodaStuff.overrideForInternal(jsonMapper);
    this.smileCustomMapper = JodaStuff.overrideForInternal(smileMapper);
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

  @GET
  @Path("/dump")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dumpQueries(@Context final HttpServletRequest req)
  {
    queryManager.dumpAll();
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getJMX(
      @QueryParam("expression") String expression,
      @QueryParam("dumpLongestStack") boolean dumpLongestStack,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    String query = "{ \"queryType\": \"jmx\"";
    if (!Strings.isNullOrEmpty(expression)) {
      query += ", \"expression\": " + expression;
    }
    if (dumpLongestStack) {
      query += ", \"dumpLongestStack\": true";
    }
    query += "}";
    return doPost(new ReaderInputStream(new StringReader(query)), "pretty", null, req);
  }

  @GET
  @Path("/config")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfig(
      @QueryParam("keys") String keys,
      @QueryParam("expression") String expression,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    StringBuilder query = new StringBuilder("{ \"queryType\": \"config\"");
    if (!Strings.isNullOrEmpty(expression)) {
      query.append(", \"expression\": ").append(expression);
    }
    if (!Strings.isNullOrEmpty(keys)) {
      query.append(", \"config\": {");
      final String[] split = keys.split(",");
      for (int i = 0; i < split.length; i++) {
        if (i > 0) {
          query.append(", ");
        }
        query.append("\"").append(split[i]).append("\": null");
      }
      query.append("}");
    }
    query.append("}");
    return doPost(new ReaderInputStream(new StringReader(query.toString())), "pretty", null, req);
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @QueryParam("smile") String smile,
      @Context final HttpServletRequest req // used to get request content-type, remote address and AuthorizationInfo
  ) throws IOException
  {
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    final String remote = req.getRemoteAddr();
    final RequestContext context = new RequestContext(req, pretty != null, Boolean.valueOf(smile));
    final String contentType = context.getContentType();

    final Thread currThread = Thread.currentThread();
    final String currThreadName = resetThreadName(currThread);

    Query query = readQuery(in, context);
    try {
      if (log.isDebugEnabled()) {
        log.info("Got query [%s]", query);
      } else {
        log.info("Got query [%s:%s]", query.getType(), query.getId());
      }
      currThread.setName(String.format("%s[%s_%s]", currThreadName, query.getType(), query.getId()));

      queryLifecycle.initialize(prepareQuery(query));
      final Query prepared = queryLifecycle.getQuery();

      final Access access = queryLifecycle.authorize((AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN));
      if (access != null && !access.isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", access).build();
      }

      final AtomicReference<Thread> writer = new AtomicReference<>();
      final Execs.SettableFuture future = new Execs.SettableFuture<Object>()
      {
        @Override
        protected void interruptTask()
        {
          final Thread thread = writer.get();
          if (thread != null) {
            thread.interrupt();
          }
        }
      };
      queryManager.registerQuery(query, future);

      final QueryToolChest toolChest = warehouse.getToolChest(prepared);
      final QueryLifecycle.QueryResponse queryResponse = queryLifecycle.execute();
      Sequence<?> sequence = queryResponse.getResults();
      final Map<String, Object> responseContext = queryResponse.getResponseContext();

      if (toolChest != null) {
        sequence = toolChest.serializeSequence(prepared, sequence, segmentWalker);
      }

      final MutableInt counter = new MutableInt();
      final Yielder yielder = Sequences.withBaggage(sequence, future).toYielder(
          null,
          new YieldingAccumulator()
          {
            private final ToIntFunction numRows = QueryToolChest.numRows(prepared, toolChest);

            @Override
            public Object accumulate(Object accumulated, Object in)
            {
              counter.add(numRows.applyAsInt(in));
              yield();
              return in;
            }
          }
      );

      final ObjectWriter jsonWriter = context.getOutputWriter(
          prepared.getContextBoolean(Query.DATETIME_CUSTOM_SERDE, false)
      );

      final StreamingOutput output = new StreamingOutput()
      {
        @Override
        public void write(OutputStream outputStream) throws IOException, WebApplicationException
        {
          // json serializer will always close the yielder
          CountingOutputStream os = new CountingOutputStream(outputStream);
          if (future.isCancelled()) {
            throw new QueryInterruptedException(new InterruptedException());
          }
          writer.set(Thread.currentThread());
          try {
            // it'll block for ServerConfig.maxIdleTime * 1.5 and throw exception, killing the thread
            jsonWriter.writeValue(os, yielder);
            os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
            os.close();
          }
          catch (Throwable t) {
            // it's not propagated to handlings below. so do it here
            handleException(queryLifecycle, remote, counter.intValue(), os.getCount(), t);
            currThread.setName(currThreadName);
            if (t instanceof IOException) {
              throw (IOException) t;
            }
            if (t instanceof WebApplicationException) {
              throw (WebApplicationException) t;
            }
            throw Throwables.propagate(t);
          } finally {
            writer.set(null);
          }

          queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), os.getCount(), counter.intValue());
          currThread.setName(currThreadName);
        }
      };
      //Limit the response-context header, see https://github.com/druid-io/druid/issues/2331
      //Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
      //and encodes the string using ASCII, so 1 char is = 1 byte
      String responseCtxString = jsonMapper.writeValueAsString(responseContext);
      if (responseCtxString.length() > RESPONSE_CTX_HEADER_LEN_LIMIT) {
        log.warn("Response Context truncated for id [%s]. Full context is [%s].", prepared.getId(), responseCtxString);
        responseCtxString = responseCtxString.substring(0, RESPONSE_CTX_HEADER_LEN_LIMIT);
      }

      return Response.ok(output, contentType)
                     .header("X-Druid-Query-Id", prepared.getId())
                     .header("X-Druid-Response-Context", responseCtxString)
                     .build();
    }
    catch (Throwable e) {
      // Input stream has already been consumed by the json object mapper if query == null
      handleException(queryLifecycle, remote, 0, 0, e);
      currThread.setName(currThreadName);
      return context.gotError(e);
    }
  }

  private Query readQuery(InputStream in, RequestContext context) throws IOException
  {
    Query query = context.getInputMapper(false).readValue(in, Query.class);
    Map<String, Object> adding = Maps.newHashMap();
    if (query.getId() == null) {
      adding.put(Query.QUERYID, UUID.randomUUID().toString());
    }
    adding.put(
        Query.TIMEOUT,
        warehouse.getQueryConfig().getMaxQueryTimeout(query.getContextInt(QueryContextKeys.TIMEOUT, -1))
    );
    return query.withOverriddenContext(adding);
  }

  private void handleException(QueryLifecycle queryLifecycle, String remote, int rows, long bytes, Throwable e) {
    queryLifecycle.emitLogsAndMetrics(e, remote, bytes, rows);
  }

  // clear previous query name if exists (should not)
  private String resetThreadName(Thread thread)
  {
    String currThreadName = thread.getName();
    int index = currThreadName.indexOf('[');
    if (index > 0 && currThreadName.endsWith("]")) {
      thread.setName(currThreadName = currThreadName.substring(0, index));
    }
    return currThreadName;
  }

  // suppress excessive interval logging in historical node
  protected Query toLoggingQuery(Query<?> query)
  {
    query = query.withQuerySegmentSpec(
        new MultipleIntervalSegmentSpec(Arrays.asList(JodaUtils.umbrellaInterval(query.getIntervals())))
    );
    if (query instanceof Query.LogProvider) {
      query = ((Query.LogProvider) query).forLog();
    }
    return query;
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
    String queryId = query.getId();
    if (queryId != null) {
      // test queries don't have ids
      query = QueryUtils.setQueryId(query, queryId);
    }
    if (ServiceTypes.PEON.equals(node.getType()) ||
        ServiceTypes.REALTIME.equals(node.getType())) {
      query = DimFilters.rewriteLuceneFilter(query);
    }
    return query;
  }

  protected class RequestContext
  {
    final boolean isSmile;
    final boolean isPretty;
    final boolean isSmileOut;
    final String contentType;

    RequestContext(HttpServletRequest request, boolean pretty)
    {
      this(request, pretty, false);
    }

    RequestContext(HttpServletRequest request, boolean pretty, boolean smileOut)
    {
      String requestType = request.getContentType();
      isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType)
                || APPLICATION_SMILE.equals(requestType);
      isPretty = pretty;
      isSmileOut = smileOut;
      contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    }

    String getContentType()
    {
      return contentType;
    }

    ObjectMapper getInputMapper(boolean useCustomSerdeForDateTime)
    {
      return useCustomSerdeForDateTime ?
             isSmile ? smileCustomMapper : jsonCustomMapper :
             isSmile ? smileMapper : jsonMapper;
    }

    ObjectWriter getOutputWriter(boolean useCustomSerdeForDateTime)
    {
      ObjectMapper mapper;
      if (isSmileOut) {
        mapper = useCustomSerdeForDateTime ? smileCustomMapper : smileMapper;
      } else {
        mapper = getInputMapper(useCustomSerdeForDateTime);
      }
      return isPretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer();
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(getOutputWriter(false).writeValueAsString(object), contentType).build();
    }

    Response gotError(Throwable e) throws IOException
    {
      return Response.serverError()
                     .type(contentType)
                     .entity(
                         getOutputWriter(false).writeValueAsBytes(
                             QueryInterruptedException.wrapIfNeeded(e, node.getHostAndPort(), node.getServiceName())
                         )
                     )
                     .build();
    }
  }
}
