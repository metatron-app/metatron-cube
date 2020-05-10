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

package io.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(DirectDruidClient.class);
  private static final long WRITE_DELAY_LOG_THRESHOLD = 100;

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper customMapper;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String host;
  private final String type;
  private final BrokerIOConfig ioConfig;
  private final ExecutorService backgroundExecutorService;

  private final AtomicInteger openConnections;
  private final String contentType;

  private final StreamHandlerFactory.WithEmitter handlerFactory;

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      ObjectMapper customMapper,
      HttpClient httpClient,
      String host,
      String type,
      ServiceEmitter emitter,
      BrokerIOConfig ioConfig,
      ExecutorService backgroundExecutorService
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.customMapper = customMapper;
    this.httpClient = httpClient;
    this.host = host;
    this.type = type;
    this.ioConfig = ioConfig;
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

    final URL url;
    final URL cancelUrl;
    final ListenableFuture<InputStream> future;
    final StreamHandler handler;
    try {
      url = new URL(String.format("http://%s/druid/v2/", host));
      cancelUrl = new URL(String.format("http://%s/druid/v2/%s", host, query.getId()));

      if (!query.getContextBoolean(Query.DISABLE_LOG, false)) {
        log.debug("Querying [%s][%s:%s] to url[%s]", query.getId(), query.getType(), query.getDataSource(), url);
      }

      final QueryMetrics<? super Query<T>> queryMetrics = toolChest.makeMetrics(query);
      queryMetrics.server(host);

      final long start = System.currentTimeMillis();
      handler = handlerFactory.create(query, url, ioConfig.getQueueSize(), queryMetrics, context);
      future = httpClient.go(
          new Request(HttpMethod.POST, url)
              .setContent(objectMapper.writeValueAsBytes(query))
              .setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType),
          handler
      );
      final long elapsed = System.currentTimeMillis() - start;
      if (elapsed > WRITE_DELAY_LOG_THRESHOLD) {
        log.info("Took %,d msec to write query[%s:%s] to url[%s]", elapsed, query.getType(), query.getId(), url);
      }

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
              IOUtils.closeQuietly(future.isCancelled() ? cancelAsResource(query, cancelUrl, handler) : handler);
            }
          }
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final ObjectMapper mapper = query.getContextBoolean(Query.DATETIME_CUSTOM_SERDE, false)
                                ? customMapper
                                : objectMapper;

    final boolean isBySegment = BaseQuery.isBySegment(query);
    final TypeFactory typeFactory = objectMapper.getTypeFactory();
    final JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference(query));

    final JavaType typeRef;
    if (isBySegment) {
      typeRef = typeFactory.constructParametricType(
          Result.class, typeFactory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
    } else {
      typeRef = baseType;
    }

    Sequence<T> sequence = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            long remaining = queryWatcher.remainingTime(query.getId());
            return new JsonParserIterator.FromFutureStream<T>(mapper, typeRef, url, type, future, remaining);
          }

          @Override
          public void cleanup(JsonParserIterator<T> parser)
          {
            IOUtils.closeQuietly(parser.close() ? handler : cancelAsResource(query, cancelUrl, handler));
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      sequence = toolChest.deserializeSequence(query, sequence);
    }

    return sequence;
  }

  private Closeable cancelAsResource(Query query, URL cancelUrl, StreamHandler handler)
  {
    return new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        backgroundExecutorService.submit(
            () -> {
              try {
                StatusResponseHolder response = httpClient.go(
                    new Request(HttpMethod.DELETE, cancelUrl),
                    new StatusResponseHandler(Charsets.UTF_8)
                ).get();
                HttpResponseStatus status = response.getStatus();
                if (status.getCode() >= 500) {
                  log.info(
                      "Error cancelling query[%s]: [%s] returned status[%d] [%s].",
                      query.getId(),
                      cancelUrl.getHost(),
                      status.getCode(),
                      status.getReasonPhrase()
                  );
                }
                return response;
              }
              finally {
                IOUtils.closeQuietly(handler);
              }
            }
        );
      }
    };
  }
}
