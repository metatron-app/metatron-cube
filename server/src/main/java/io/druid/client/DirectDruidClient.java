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

package io.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Pair;
import com.metamx.common.RE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.Execs;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.jackson.JodaStuff;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulatorFns;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(DirectDruidClient.class);

  private static final Map<Class<? extends Query>, Pair<JavaType, JavaType>> typesMap = Maps.newConcurrentMap();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String host;
  private final ExecutorService backgroundExecutorService;

  private final AtomicInteger openConnections;
  private final String contentType;

  private final StreamHandlerFactory.WithEmitter handlerFactory;

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String host,
      ServiceEmitter emitter,
      ExecutorService backgroundExecutorService
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.host = host;
    this.backgroundExecutorService = backgroundExecutorService;
    this.contentType = objectMapper.getFactory() instanceof SmileFactory
                       ? SmileMediaTypes.APPLICATION_JACKSON_SMILE
                       : MediaType.APPLICATION_JSON;
    this.openConnections = new AtomicInteger();
    this.handlerFactory = new StreamHandlerFactory.WithEmitter(log, emitter, objectMapper);
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> context)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    Pair<JavaType, JavaType> types = typesMap.get(query.getClass());
    if (types == null) {
      final TypeFactory typeFactory = objectMapper.getTypeFactory();
      JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference());
      JavaType bySegmentType = typeFactory.constructParametricType(
          Result.class, typeFactory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
      types = Pair.of(baseType, bySegmentType);
      typesMap.put(query.getClass(), types);
    }

    final URL url;
    final URL cancelUrl;
    final ListenableFuture<InputStream> future;

    final byte[] bytes;
    try {
      url = new URL(String.format("http://%s/druid/v2/", host));
      cancelUrl = new URL(String.format("http://%s/druid/v2/%s", host, query.getId()));

      log.debug("Querying queryId[%s] url[%s]", query.getId(), url);

      final ServiceMetricEvent.Builder builder = toolChest.makeMetricBuilder(query);
      builder.setDimension("server", host);
      builder.setDimension(DruidMetrics.ID, Strings.nullToEmpty(query.getId()));

      bytes = objectMapper.writeValueAsBytes(query);
      future = httpClient.go(
          new Request(HttpMethod.POST, url)
              .setContent(bytes)
              .setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType),
          handlerFactory.create(query, url, builder, context)
      );

      queryWatcher.registerQuery(query, Execs.tag(future, host));

      openConnections.getAndIncrement();
      Futures.addCallback(
          future, new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
              if (future.isCancelled()) {
                // forward the cancellation to underlying queryable node
                try {
                  StatusResponseHolder res = httpClient.go(
                      new Request(HttpMethod.DELETE, cancelUrl)
                          .setContent(bytes)
                          .setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType),
                      new StatusResponseHandler(Charsets.UTF_8)
                  ).get();
                  if (res.getStatus().getCode() >= 500) {
                    throw new RE(
                        "Error cancelling query[%s]: queryable node returned status[%d] [%s].",
                        res.getStatus().getCode(),
                        res.getStatus().getReasonPhrase()
                    );
                  }
                }
                catch (ExecutionException | InterruptedException e) {
                  Throwables.propagate(e);
                }
              }
            }
          }
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final ObjectMapper mapper = query.getContextBoolean(Query.DATETIME_CUSTOM_SERDE, false)
                                ? JodaStuff.overrideForInternal(objectMapper)
                                : objectMapper;

    final boolean isBySegment = BaseQuery.getContextBySegment(query, false);
    final JavaType typeRef = isBySegment ? types.rhs : types.lhs;

    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<T>(mapper, typeRef, future, url);
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            if (!iterFromMake.close()) {
              backgroundExecutorService.submit(
                  new PrioritizedCallable.Background<StatusResponseHolder>()
                  {
                    @Override
                    public StatusResponseHolder call() throws Exception
                    {
                      return httpClient.go(
                          new Request(HttpMethod.DELETE, cancelUrl)
                              .setContent(bytes)
                              .setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType),
                          new StatusResponseHandler(Charsets.UTF_8)
                      ).get();
                    }
                  }
              );
            }
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makePreComputeManipulatorFn(
              query,
              MetricManipulatorFns.deserializing()
          )
      );
    }

    return retVal;
  }
}
