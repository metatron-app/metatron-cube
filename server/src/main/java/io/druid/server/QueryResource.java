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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.guava.YieldingAccumulator;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.output.OutputDecorator;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.jackson.JodaStuff;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.security.Access;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang.mutable.MutableInt;
import org.eclipse.jetty.io.EofException;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  public static final String GET_FEATURE = "GetFeature";

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

  protected final AuthorizerMapper authorizerMapper;

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
      AuthorizerMapper authorizerMapper
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
    this.authorizerMapper = authorizerMapper;
    this.jsonCustomMapper = JodaStuff.overrideForInternal(jsonMapper);
    this.smileCustomMapper = JodaStuff.overrideForInternal(smileMapper);
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelQuery(@PathParam("id") String queryId, @Context final HttpServletRequest req)
  {
    log.info("Received cancel request for query [%s]", queryId);
    List<String> dataSources = queryManager.getQueryDatasources(queryId);
    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(dataSources, AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
        authorizerMapper
    );
    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
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
      @QueryParam("pretty") String pretty,
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
    return doPost(new ReaderInputStream(new StringReader(query)), "pretty", pretty, req);
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
    final String remote = req.getRemoteAddr();
    final RequestContext context = new RequestContext(req, pretty != null, Boolean.valueOf(smile));

    final Thread currThread = Thread.currentThread();
    final String currThreadName = resetThreadName(currThread);

    final Query query = readQuery(in, context);
    final QueryLifecycle lifecycle = queryLifecycleFactory.factorize(query);

    if (log.isDebugEnabled()) {
      log.info("Got query [%s]", query);
    } else {
      log.info("Got query [%s:%s]", query.getType(), query.getId());
    }
    try {
      currThread.setName(String.format("%s[%s_%s]", currThreadName, query.getType(), query.getId()));

      final Query prepared = prepareQuery(query, context);
      final Access access = lifecycle.authorize(prepared, req);
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
      final Map<String, Object> responseContext = new ConcurrentHashMap<>();

      Sequence<?> sequence = lifecycle.execute(prepared, responseContext);
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

      OutputDecorator decorator = null;
      if (context.request.getAttribute(GET_FEATURE) != null) {
        decorator = jsonMapper.convertValue(BaseQuery.removeDecoratorContext(prepared), OutputDecorator.class);
      }
      final ObjectWriter jsonWriter = context.getOutputWriter(
          prepared.getContextBoolean(Query.DATETIME_CUSTOM_SERDE, false), decorator
      );

      final StreamingOutput output = new StreamingOutput()
      {
        @Override
        public void write(OutputStream outputStream) throws IOException, WebApplicationException
        {
          // json serializer will always close the yielder
          CountingOutputStream os = new CountingOutputStream(outputStream);
          if (future.isCancelled()) {
            throw QueryInterruptedException.wrapIfNeeded(new InterruptedException(), node);
          }
          writer.set(Thread.currentThread());
          try {
            // it'll block for ServerConfig.maxIdleTime * 1.5 and throw exception, killing the thread
            jsonWriter.writeValue(os, yielder);
            os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
            os.close();
          }
          catch (Throwable t) {
            IOUtils.closeQuietly(outputStream);
            if (QueryLifecycle.isEOF(t)) {
              // ignore.. todo: let's find better way to handle intentional disconnect
              lifecycle.emitLogsAndMetrics(toLoggingQuery(query), null, remote, os.getCount(), counter.intValue());
              currThread.setName(currThreadName);
              return;
            }
            // it's not propagated to handlings below. so do it here
            lifecycle.emitLogsAndMetrics(toLoggingQuery(query), t, remote, os.getCount(), counter.intValue());
            currThread.setName(currThreadName);
            if (QueryLifecycle.isInterrupted(t)) {
              throw new EofException(t.getCause()); // suppress logging in servlet cotainer (see jetty.ServletHandler)
            }
            if (t instanceof IOException) {
              throw (IOException) t;
            }
            if (t instanceof WebApplicationException) {
              throw (WebApplicationException) t;
            }
            throw Throwables.propagate(t);
          }
          finally {
            writer.set(null);
          }

          lifecycle.emitLogsAndMetrics(toLoggingQuery(query), null, remote, os.getCount(), counter.intValue());
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

      return Response.ok(output, context.getContentType())
                     .header("X-Druid-Query-Id", prepared.getId())
                     .header("X-Druid-Response-Context", responseCtxString)
                     .build();
    }
    catch (Throwable e) {
      // Input stream has already been consumed by the json object mapper if query == null
      lifecycle.emitLogsAndMetrics(toLoggingQuery(query), e, remote, -1, -1);
      currThread.setName(currThreadName);
      return context.gotError(e);
    }
  }

  private Query readQuery(InputStream in, RequestContext context) throws IOException
  {
    ObjectMapper mapper = context.getInputMapper(false);
    Query query = QueryUtils.readPostProcessors(mapper.readValue(in, Query.class), mapper);
    if (query.getId() == null) {
      query = QueryUtils.setQueryId(query, UUID.randomUUID().toString());
    }
    return query;
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
        MultipleIntervalSegmentSpec.of(JodaUtils.umbrellaInterval(query.getIntervals()))
    );
    return QueryUtils.forLog(query);
  }

  protected Query prepareQuery(Query query, RequestContext context) throws Exception
  {
    return BaseQuery.enforceTimeout(query, warehouse.getQueryConfig().getMaxQueryTimeout());
  }

  protected class RequestContext
  {
    final boolean isSmile;
    final boolean isPretty;
    final boolean isSmileOut;
    final String contentType;
    final HttpServletRequest request;

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
      this.request = request;
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

    ObjectWriter getOutputWriter(final boolean useCustomSerdeForDateTime, final OutputDecorator decorator)
    {
      ObjectMapper mapper;
      if (isSmileOut) {
        mapper = useCustomSerdeForDateTime ? smileCustomMapper : smileMapper;
      } else {
        mapper = getInputMapper(useCustomSerdeForDateTime);
      }
      if (decorator != null) {
        ObjectMapper copy = mapper.copy();
        SimpleSerializers serializers = new SimpleSerializers();
        serializers.addSerializer(Yielder.class, new JsonSerializer<Yielder>()
        {
          @Override
          public void serialize(Yielder yielder, JsonGenerator jgen, SerializerProvider provider)
              throws IOException, JsonProcessingException
          {
            try {
              decorator.start(jgen, provider);
              for (;!yielder.isDone(); yielder = yielder.next(null)) {
                decorator.serialize(jgen, provider, yielder.get());
              }
              decorator.end(jgen, provider);
            }
            finally {
              yielder.close();
            }
          }
        });
        mapper = copy.setSerializerFactory(mapper.getSerializerFactory().withAdditionalSerializers(serializers));
      }
      return isPretty ? mapper.writerWithDefaultPrettyPrinter() : mapper.writer();
    }

    ObjectWriter getOutputWriter()
    {
      return getOutputWriter(false, null);
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(getOutputWriter().writeValueAsString(object), contentType).build();
    }

    Response gotError(Throwable e) throws IOException
    {
      return Response.serverError()
                     .type(contentType)
                     .entity(getOutputWriter().writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(e, node)))
                     .build();
    }
  }
}
