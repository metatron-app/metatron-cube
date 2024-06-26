/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import io.druid.client.selector.Server;
import io.druid.common.utils.StringUtils;
import io.druid.curator.discovery.ServerDiscoverySelector;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class facilitates interaction with Coordinator/Overlord leader nodes. Instance of this class is injected
 * via Guice with annotations @Coordinator or @IndexingService .
 * Usage:
 * Request request = druidLeaderClient.makeRequest(HttpMethod, requestPath)
 * request.setXXX(..)
 * FullResponseHolder responseHolder = druidLeaderClient.go(request)
 */
public class DruidLeaderClient
{
  private final Logger log = new Logger(DruidLeaderClient.class);

  private static final int MAX_RETRIES = 5;

  private final HttpClient httpClient;
  private final String nodeTypeToWatch;

  private final String leaderRequestPath;

  //Note: This is kept for back compatibility with pre 0.11.0 releases and should be removed in future.
  private final ServerDiscoverySelector serverDiscoverySelector;

  private AtomicReference<String> currentKnownLeader = new AtomicReference<>();

  public DruidLeaderClient(
      HttpClient httpClient,
      String nodeTypeToWatch,
      String leaderRequestPath,
      ServerDiscoverySelector serverDiscoverySelector
  )
  {
    this.httpClient = httpClient;
    this.nodeTypeToWatch = nodeTypeToWatch;
    this.leaderRequestPath = leaderRequestPath;
    this.serverDiscoverySelector = serverDiscoverySelector;
  }

  @LifecycleStart
  public void start()
  {

    log.info("Started.");
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopped.");
  }

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
  {
    return new Request(httpMethod, new URL(StringUtils.format("%s%s", getCurrentKnownLeader(true), urlPath)));
  }

  /**
   * Executes the request object aimed at the leader and process the response with given handler
   * Note: this method doesn't do retrying on errors or handle leader changes occurred during communication
   */
  public <Intermediate, Final> ListenableFuture<Final> goAsync(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return httpClient.go(request, handler);
  }

  /**
   * Executes a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public FullResponseHolder go(
      Request request,
      HttpResponseHandler<FullResponseHolder, FullResponseHolder> responseHandler
  ) throws IOException, InterruptedException
  {
    for (int counter = 0; counter < MAX_RETRIES; counter++) {

      final FullResponseHolder fullResponseHolder;

      try {
        try {
          fullResponseHolder = httpClient.go(request, responseHandler).get();
        }
        catch (ExecutionException e) {
          // Unwrap IOExceptions and ChannelExceptions, re-throw others
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          Throwables.propagateIfInstanceOf(e.getCause(), ChannelException.class);
          throw new RE(e, "HTTP request to[%s] failed", request.getUrl());
        }
      }
      catch (IOException | ChannelException ex) {
        // can happen if the node is stopped.
        log.warn(ex, "Request[%s] failed.", request.getUrl());

        try {
          if (request.getUrl().getQuery() == null) {
            request = withUrl(
                request,
                new URL(StringUtils.format("%s%s", getCurrentKnownLeader(false), request.getUrl().getPath()))
            );
          } else {
            request = withUrl(
                request,
                new URL(StringUtils.format(
                    "%s%s?%s",
                    getCurrentKnownLeader(false),
                    request.getUrl().getPath(),
                    request.getUrl().getQuery()
                ))
            );
          }
          continue;
        }
        catch (MalformedURLException e) {
          // Not an IOException; this is our own fault.
          throw new ISE(
              e,
              "failed to build url with path[%] and query string [%s].",
              request.getUrl().getPath(),
              request.getUrl().getQuery()
          );
        }
      }

      if (HttpResponseStatus.TEMPORARY_REDIRECT.equals(fullResponseHolder.getResponse().getStatus())) {
        String redirectUrlStr = fullResponseHolder.getResponse().headers().get("Location");
        if (redirectUrlStr == null) {
          throw new IOException("No redirect location is found in response from url[" + request.getUrl() + "]");
        }

        log.info("Request[%s] received redirect response to location [%s].", request.getUrl(), redirectUrlStr);

        final URL redirectUrl;
        try {
          redirectUrl = new URL(redirectUrlStr);
        }
        catch (MalformedURLException ex) {
          throw new IOException(
              String.format("Malformed redirect location is found in response from url[%s], new location[%s].",
                            request.getUrl(),
                            redirectUrlStr),
              ex
          );
        }

        //update known leader location
        currentKnownLeader.set(StringUtils.format(
            "%s://%s:%s",
            redirectUrl.getProtocol(),
            redirectUrl.getHost(),
            redirectUrl.getPort()
        ));

        request = withUrl(request, redirectUrl);
      } else {
        return fullResponseHolder;
      }
    }

    throw new IOException("Retries exhausted, couldn't fulfill request to [" + request.getUrl() + "].");
  }

  private String getCurrentKnownLeader(final boolean cached) throws IOException
  {
    final String leader = pickOneHost();

    if (leader == null) {
      throw new IOException("No known server");
    } else {
      return leader;
    }
  }

  @Nullable
  private String pickOneHost()
  {
    Server server = serverDiscoverySelector.pick();
    if (server != null) {
      return StringUtils.format(
          "%s://%s:%s",
          server.getScheme(),
          server.getAddress(),
          server.getPort()
      );
    }

    return null;
  }

  private Request withUrl(Request old, URL url)
  {
    Request req = new Request(old.getMethod(), url);
    req.addHeaderValues(old.getHeaders());
    if (old.hasContent()) {
      req.setContent(old.getContent());
    }
    return req;
  }
}
