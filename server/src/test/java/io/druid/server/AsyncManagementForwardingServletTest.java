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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.java.util.http.client.Request;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.utils.SocketUtil;
import io.druid.common.utils.StringUtils;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.BaseJettyTest;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class AsyncManagementForwardingServletTest extends BaseJettyTest
{
  private static final ExpectedRequest coordinatorExpectedRequest = new ExpectedRequest();
  private static final ExpectedRequest overlordExpectedRequest = new ExpectedRequest();

  private static int coordinatorPort;
  private static int overlordPort;

  private Server coordinator;
  private Server overlord;

  private static class ExpectedRequest
  {
    private boolean called = false;
    private String path;
    private String query;
    private String method;
    private Map<String, String> headers;
    private String body;

    private void reset()
    {
      called = false;
      path = null;
      query = null;
      method = null;
      headers = null;
      body = null;
    }
  }

  @Override
  @Before
  public void setup() throws Exception
  {
    super.setup();

    coordinatorPort = SocketUtil.findOpenPortFrom(port + 1);
    overlordPort = SocketUtil.findOpenPortFrom(coordinatorPort + 1);

    coordinator = makeTestServer(coordinatorPort, coordinatorExpectedRequest);
    overlord = makeTestServer(overlordPort, overlordExpectedRequest);

    coordinator.start();
    overlord.start();
  }

  @After
  public void tearDown() throws Exception
  {
    coordinator.stop();
    overlord.stop();

    coordinatorExpectedRequest.reset();
    overlordExpectedRequest.reset();
  }

  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), ImmutableList.of(new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        JsonConfigProvider.bindInstance(
            binder,
            Key.get(DruidNode.class, Self.class),
            new DruidNode("test", "localhost", null)
        );
        binder.bind(JettyServerInitializer.class).to(ProxyJettyServerInit.class).in(LazySingleton.class);
        LifecycleModule.register(binder, Server.class);
      }
    }));
  }

  @Test
  public void testCoordinatorDatasources() throws Exception
  {
    coordinatorExpectedRequest.path = "/druid/coordinator/v1/datasources";
    coordinatorExpectedRequest.method = "GET";
    coordinatorExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, coordinatorExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    for (Map.Entry<String, String> entry : coordinatorExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testCoordinatorLoadStatus() throws Exception
  {
    coordinatorExpectedRequest.path = "/druid/coordinator/v1/loadstatus";
    coordinatorExpectedRequest.query = "full";
    coordinatorExpectedRequest.method = "GET";
    coordinatorExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format(
            "http://localhost:%d%s?%s", port, coordinatorExpectedRequest.path, coordinatorExpectedRequest.query
        )).openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    for (Map.Entry<String, String> entry : coordinatorExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testCoordinatorEnable() throws Exception
  {
    coordinatorExpectedRequest.path = "/druid/coordinator/v1/datasources/myDatasource";
    coordinatorExpectedRequest.method = "POST";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, coordinatorExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testCoordinatorDisable() throws Exception
  {
    coordinatorExpectedRequest.path = "/druid/coordinator/v1/datasources/myDatasource/intervals/2016-06-27_2016-06-28";
    coordinatorExpectedRequest.method = "DELETE";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, coordinatorExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testCoordinatorProxyStatus() throws Exception
  {
    coordinatorExpectedRequest.path = "/status";
    coordinatorExpectedRequest.method = "GET";
    coordinatorExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/coordinator%s", port, coordinatorExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    for (Map.Entry<String, String> entry : coordinatorExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testCoordinatorProxySegments() throws Exception
  {
    coordinatorExpectedRequest.path = "/druid/coordinator/v1/metadata/datasources/myDatasource/segments";
    coordinatorExpectedRequest.method = "POST";
    coordinatorExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");
    coordinatorExpectedRequest.body = "[\"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\", \"2012-01-05T00:00:00.000/2012-01-07T00:00:00.000\"]";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/coordinator%s", port, coordinatorExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(coordinatorExpectedRequest.method);

    for (Map.Entry<String, String> entry : coordinatorExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    connection.setDoOutput(true);
    OutputStream os = connection.getOutputStream();
    os.write(coordinatorExpectedRequest.body.getBytes(Charsets.UTF_8));
    os.close();

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testOverlordPostTask() throws Exception
  {
    overlordExpectedRequest.path = "/druid/indexer/v1/task";
    overlordExpectedRequest.method = "POST";
    overlordExpectedRequest.headers = ImmutableMap.of(
        "Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=",
        "Content-Type", "application/json"
    );
    overlordExpectedRequest.body = "{\"type\": \"index\", \"spec\": \"stuffGoesHere\"}";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, overlordExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(overlordExpectedRequest.method);

    for (Map.Entry<String, String> entry : overlordExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    connection.setDoOutput(true);
    OutputStream os = connection.getOutputStream();
    os.write(overlordExpectedRequest.body.getBytes(Charsets.UTF_8));
    os.close();

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertTrue("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testOverlordTaskStatus() throws Exception
  {
    overlordExpectedRequest.path = "/druid/indexer/v1/task/myTaskId/status";
    overlordExpectedRequest.method = "GET";
    overlordExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, overlordExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(overlordExpectedRequest.method);

    for (Map.Entry<String, String> entry : overlordExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertTrue("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testOverlordProxyLeader() throws Exception
  {
    overlordExpectedRequest.path = "/druid/indexer/v1/leader";
    overlordExpectedRequest.method = "GET";
    overlordExpectedRequest.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/overlord/%s", port, overlordExpectedRequest.path))
            .openConnection());
    connection.setRequestMethod(overlordExpectedRequest.method);

    for (Map.Entry<String, String> entry : overlordExpectedRequest.headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      connection.setRequestProperty(key, value);
    }

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertTrue("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testBadProxyDestination() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/other/status", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(400, connection.getResponseCode());
    Assert.assertFalse("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  @Test
  public void testLocalRequest() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/status", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(404, connection.getResponseCode());
    Assert.assertFalse("coordinator called", coordinatorExpectedRequest.called);
    Assert.assertFalse("overlord called", overlordExpectedRequest.called);
  }

  private static Server makeTestServer(int port, final ExpectedRequest expectedRequest)
  {
    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(new ServletHolder(new HttpServlet()
    {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
      {
        handle(req, resp);
      }

      private void handle(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        boolean passed = expectedRequest.path.equals(req.getRequestURI());
        passed &= expectedRequest.query == null || expectedRequest.query.equals(req.getQueryString());
        passed &= expectedRequest.method.equals(req.getMethod());

        if (expectedRequest.headers != null) {
          for (Map.Entry<String, String> header : expectedRequest.headers.entrySet()) {
            passed &= header.getValue().equals(req.getHeader(header.getKey()));
          }
        }

        passed &= expectedRequest.body == null || expectedRequest.body.equals(IOUtils.toString(req.getReader()));

        expectedRequest.called = true;
        resp.setStatus(passed ? 200 : 400);
      }
    }), "/*");

    server.setHandler(handler);
    return server;
  }

  public static class ProxyJettyServerInit implements JettyServerInitializer
  {
    private final DruidNode druidNode;

    @Inject
    public ProxyJettyServerInit(@Self DruidNode druidNode)
    {
      this.druidNode = druidNode;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

      final TestCoordinatorClient coordinatorLeaderSelector = new TestCoordinatorClient(druidNode, null, null, null)
      {
        @Override
        public Request makeRequest(HttpMethod httpMethod, String resourcePath)
        {
          try {
            return new Request(HttpMethod.GET,
                               new URL(StringUtils.format("http://localhost:%d", coordinatorPort)));
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
          //return StringUtils.format("http://localhost:%d", coordinatorPort);
        }
      };

      final TestIndexingServiceClient overlordLeaderSelector = new TestIndexingServiceClient()
      {
        @Override
        public Request makeRequest(HttpMethod httpMethod, String resourcePath)
        {
          try {
            return new Request(HttpMethod.GET, new URL(StringUtils.format("http://localhost:%d", overlordPort)));
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
          //return StringUtils.format("http://localhost:%d", overlordPort);
        }
      };

      ServletHolder holder = new ServletHolder(
          new AsyncManagementForwardingServlet(
              injector.getInstance(ObjectMapper.class),
              injector.getProvider(HttpClient.class),
              injector.getInstance(DruidHttpClientConfig.class),
              coordinatorLeaderSelector,
              overlordLeaderSelector
          )
      );

      //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
      holder.setInitParameter("maxThreads", "256");

      root.addServlet(holder, "/druid/coordinator/*");
      root.addServlet(holder, "/druid/indexer/*");
      root.addServlet(holder, "/proxy/*");

      JettyServerInitUtils.addExtensionFilters(root, injector);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{JettyServerInitUtils.wrapWithDefaultGzipHandler(root)});
      server.setHandler(handlerList);
    }
  }

  private static class TestCoordinatorClient extends CoordinatorClient
  {

    public TestCoordinatorClient(
        DruidNode server,
        io.druid.java.util.http.client.HttpClient client,
        ObjectMapper jsonMapper,
        ServerDiscoverySelector selector
    )
    {
      super(server, client, jsonMapper, selector);
    }

    public TestCoordinatorClient()
    {
      super(null, null, null, null);
    }

    public Request makeRequest(HttpMethod httpMethod, String resourcePath)
    {
      return null;
    }
  }

  private static class TestIndexingServiceClient extends IndexingServiceClient
  {

    public TestIndexingServiceClient(
        io.druid.java.util.http.client.HttpClient client,
        ObjectMapper jsonMapper,
        ServerDiscoverySelector selector
    )
    {
      super(client, jsonMapper, selector);
    }

    public TestIndexingServiceClient()
    {
      super(null, null, null);
    }

    public Request makeRequest(HttpMethod httpMethod, String resourcePath)
    {
      return null;
    }
  }

}
