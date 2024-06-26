/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
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

package io.druid.java.util.http.client;

import com.google.common.base.Charsets;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.TimeoutException;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests with servers that are at least moderately well-behaving.
 */
public class FriendlyServersTest
{
  @Test
  public void testFriendlyHttpServer() throws Exception
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  BufferedReader in = new BufferedReader(
                      new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                  );
                  OutputStream out = clientSocket.getOutputStream()
              ) {
                while (!in.readLine().equals("")) {
                  // skip lines
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(Charsets.UTF_8));
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          ).get();

      Assert.assertEquals(200, response.getStatus().getCode());
      Assert.assertEquals("hello!", response.getContent());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testCompressionCodecConfig() throws Exception
  {
    final ExecutorService exec = Executors.newSingleThreadExecutor();
    final ServerSocket serverSocket = new ServerSocket(0);
    final AtomicBoolean foundAcceptEncoding = new AtomicBoolean();
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (!Thread.currentThread().isInterrupted()) {
              try (
                  Socket clientSocket = serverSocket.accept();
                  BufferedReader in = new BufferedReader(
                      new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
                  );
                  OutputStream out = clientSocket.getOutputStream()
              ) {
                // Read headers
                String header;
                while (!(header = in.readLine()).equals("")) {
                  if (header.equals("Accept-Encoding: identity")) {
                    foundAcceptEncoding.set(true);
                  }
                }
                out.write("HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nhello!".getBytes(Charsets.UTF_8));
              }
              catch (Exception e) {
                // Suppress
              }
            }
          }
        }
    );

    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder()
                                                      .withCompressionCodec(HttpClientConfig.CompressionCodec.IDENTITY)
                                                      .build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);
      final StatusResponseHolder response = client
          .go(
              new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d/", serverSocket.getLocalPort()))),
              new StatusResponseHandler(Charsets.UTF_8)
          ).get();

      Assert.assertEquals(200, response.getStatus().getCode());
      Assert.assertEquals("hello!", response.getContent());
      Assert.assertTrue(foundAcceptEncoding.get());
    }
    finally {
      exec.shutdownNow();
      serverSocket.close();
      lifecycle.stop();
    }
  }

  @Test
  public void testFriendlySelfSignedHttpsServer() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    final String keyStorePath = getClass().getClassLoader().getResource("keystore.jks").getFile();
    Server server = new Server();

    HttpConfiguration https = new HttpConfiguration();
    https.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keyStorePath);
    sslContextFactory.setKeyStorePassword("abc123");
    sslContextFactory.setKeyManagerPassword("abc123");

    ServerConnector sslConnector = new ServerConnector(
        server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https)
    );

    sslConnector.setPort(0);
    server.setConnectors(new Connector[]{sslConnector});
    server.start();

    try {
      final SSLContext mySsl = HttpClientInit.sslContextWithTrustedKeyStore(keyStorePath, "abc123");
      final HttpClientConfig trustingConfig = HttpClientConfig.builder()
                                                              .withSslContext(mySsl)
                                                              .withGetConnectionTimeoutDuration(Duration.millis(5000))
                                                              .build();
      final HttpClient trustingClient = HttpClientInit.createClient(trustingConfig, lifecycle);

      final HttpClientConfig skepticalConfig = HttpClientConfig.builder()
                                                               .withSslContext(SSLContext.getDefault())
                                                               .withGetConnectionTimeoutDuration(Duration.millis(5000))
                                                               .build();
      final HttpClient skepticalClient = HttpClientInit.createClient(skepticalConfig, lifecycle);

      // Correct name ("localhost")
      {
        final HttpResponseStatus status = trustingClient
            .go(
                new Request(
                    HttpMethod.GET,
                    new URL(StringUtils.format("https://localhost:%d/", sslConnector.getLocalPort()))
                ),
                new StatusResponseHandler(Charsets.UTF_8)
            ).get().getStatus();
        Assert.assertEquals(404, status.getCode());
      }

      // Incorrect name ("127.0.0.1")
      {
        String message = null;
        try {
          trustingClient.go(
              new Request(
                  HttpMethod.GET,
                  new URL(StringUtils.format("https://127.0.0.1:%d/", sslConnector.getLocalPort()))
              ),
              new StatusResponseHandler(Charsets.UTF_8)
          );
        }
        catch (TimeoutException e) {
          message = e.getMessage();
        }

        Assert.assertTrue(message, message.startsWith("Timeout getting connection"));
      }

      // Untrusting client
      {
        String message = null;
        try {
          skepticalClient.go(
              new Request(
                  HttpMethod.GET,
                  new URL(StringUtils.format("https://127.0.0.1:%d/", sslConnector.getLocalPort()))
              ),
              new StatusResponseHandler(Charsets.UTF_8)
          );
        }
        catch (TimeoutException e) {
          message = e.getMessage();
        }

        Assert.assertTrue(message, message.startsWith("Timeout getting connection"));
      }
    }
    finally {
      lifecycle.stop();
      server.stop();
    }
  }

  @Test
  @Ignore
  public void testHttpBin() throws Throwable
  {
    final Lifecycle lifecycle = new Lifecycle();
    try {
      final HttpClientConfig config = HttpClientConfig.builder().withSslContext(SSLContext.getDefault()).build();
      final HttpClient client = HttpClientInit.createClient(config, lifecycle);

      {
        final HttpResponseStatus status = client
            .go(
                new Request(HttpMethod.GET, new URL("https://httpbin.org/get")),
                new StatusResponseHandler(Charsets.UTF_8)
            ).get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }

      {
        final HttpResponseStatus status = client
            .go(
                new Request(HttpMethod.POST, new URL("https://httpbin.org/post"))
                    .setContent(new byte[]{'a', 'b', 'c', 1, 2, 3}),
                new StatusResponseHandler(Charsets.UTF_8)
            ).get().getStatus();

        Assert.assertEquals(200, status.getCode());
      }
    }
    finally {
      lifecycle.stop();
    }
  }
}
