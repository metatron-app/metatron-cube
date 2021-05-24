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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.ChannelResource;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.utils.StopWatch;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  private static final Logger LOG = new Logger(DirectDruidClient.class);
  private static final long CONNECT_DELAY_LOG_THRESHOLD = 200;

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper customMapper;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String host;
  private final String type;
  private final URL hostURL;
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
    this.hostURL = StringUtils.toURL(String.format("http://%s/druid/v2/", host));
    this.backgroundExecutorService = backgroundExecutorService;
    this.contentType = objectMapper.getFactory() instanceof SmileFactory
                       ? SmileMediaTypes.APPLICATION_JACKSON_SMILE
                       : MediaType.APPLICATION_JSON;
    this.openConnections = new AtomicInteger();
    this.handlerFactory = new StreamHandlerFactory.WithEmitter(host, ioConfig, emitter, objectMapper);
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> context)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final ObjectMapper mapper = query.getContextBoolean(Query.DATETIME_CUSTOM_SERDE, false)
                                ? customMapper
                                : objectMapper;

    final JavaType typeRef = toolChest.getResultTypeReference(query, objectMapper.getTypeFactory());

    final QueryMetrics<?> queryMetrics = toolChest.makeMetrics(query);
    queryMetrics.server(host);

    if (!query.getContextBoolean(Query.DISABLE_LOG, false)) {
      LOG.debug("Querying [%s][%s:%s] to url[%s]", query.getId(), query.getType(), query.getDataSource(), hostURL);
    }

    final long remain = queryWatcher.remainingTime(query.getId());
    final byte[] content = serializeQuery(query, remain);

    final StopWatch watch = new StopWatch(remain);
    final StreamHandler handler = handlerFactory.create(query, content.length, watch, queryMetrics, context);

    final long start = System.currentTimeMillis();
    final ChannelResource<InputStream> channel = httpClient.go(
        new Request(HttpMethod.POST, hostURL, true)
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, contentType)
            .setContent(content),
        handler
    );
    final long elapsed = System.currentTimeMillis() - start;
    if (elapsed > CONNECT_DELAY_LOG_THRESHOLD) {
      LOG.info("Took %,d msec connecting to url[%s]", elapsed, hostURL);
    }

    openConnections.getAndIncrement();

    final Callable<InputStream> waiter = () -> watch.wainOn(channel);
    final JsonParserIterator<T> iterator = new JsonParserIterator<T>(mapper, typeRef, host, type, waiter);
    final Closeable resource = new Closeable()
    {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public void close() throws IOException
      {
        if (closed.compareAndSet(false, true)) {
          openConnections.getAndDecrement();
          queryWatcher.unregister(query, handler);
          if (!iterator.close()) {
            if (watch.isExpired()) {
              DirectDruidClient.this.cancelRemote(query);
            }
            IOUtils.closeQuietly(channel);
          }
          IOUtils.closeQuietly(handler);
        }
      }
    };
    queryWatcher.register(query, Execs.tag(channel, host), resource);

    Sequence<T> sequence = Sequences.once(query.estimatedOutputColumns(), GuavaUtils.withResource(iterator, resource));
    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!BaseQuery.isBySegment(query)) {
      sequence = toolChest.deserializeSequence(query, sequence);
    }

    return sequence;
  }

  private byte[] serializeQuery(Query<T> query, long remain)
  {
    if (remain <= 0) {
      throw new QueryInterruptedException(new TimeoutException());
    }
    try {
      return objectMapper.writeValueAsBytes(query.withOverriddenContext(Query.TIMEOUT, remain));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void cancelRemote(Query<T> query)
  {
    backgroundExecutorService.submit(
        () -> {
          URL cancelUrl = StringUtils.toURL(String.format("http://%s/druid/v2/%s", host, query.getId()));
          StatusResponseHolder response = httpClient.go(
              new Request(HttpMethod.DELETE, cancelUrl),
              new StatusResponseHandler(Charsets.UTF_8)
          ).get();
          HttpResponseStatus status = response.getStatus();
          if (status.getCode() >= 500) {
            LOG.info(
                "Error cancelling query[%s]: [%s] returned status[%d] [%s].",
                query.getId(),
                cancelUrl.getHost(),
                status.getCode(),
                status.getReasonPhrase()
            );
          }
          return response;
        }
    );
  }
}
