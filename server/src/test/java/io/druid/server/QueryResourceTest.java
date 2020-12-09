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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class QueryResourceTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT = new AuthenticationResult("druid", "druid", null, null);

  private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  public static final ServerConfig serverConfig = new ServerConfig()
  {
    @Override
    public int getNumThreads()
    {
      return 1;
    }

    @Override
    public Period getMaxIdleTime()
    {
      return Period.seconds(1);
    }
  };
  private final HttpServletRequest testServletRequest = EasyMock.createMock(HttpServletRequest.class);
  public static final QuerySegmentWalker testSegmentWalker = new QuerySegmentWalker()
  {
    @Override
    public ExecutorService getExecutor()
    {
      return executorService;
    }

    @Override
    public ObjectMapper getObjectMapper()
    {
      return jsonMapper;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(
        Query<T> query, Iterable<Interval> intervals
    )
    {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(
            Query<T> query, Map<String, Object> responseContext
        )
        {
          return Sequences.<T>empty();
        }
      };
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(
        Query<T> query, Iterable<SegmentDescriptor> specs
    )
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };

  private static final DruidNode node = new DruidNode("dummy", "dummy", 0);
  private static final QueryToolChestWarehouse warehouse = new MapQueryToolChestWarehouse(
      new QueryConfig(), Maps.<Class<? extends Query>, QueryToolChest>newHashMap()
  );
  private static final ServiceEmitter noopServiceEmitter = new NoopServiceEmitter();

  private QueryResource queryResource;
  private QueryManager queryManager;

  @BeforeClass
  public static void staticSetup()
  {
    io.druid.java.util.emitter.EmittingLogger.registerEmitter(noopServiceEmitter);
  }

  @Before
  public void setup()
  {
    EasyMock.expect(testServletRequest.getContentType()).andReturn(MediaType.APPLICATION_JSON).anyTimes();
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();
    queryManager = new QueryManager();
    QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        queryManager,
        warehouse,
        testSegmentWalker,
        new DefaultGenericQueryMetricsFactory(jsonMapper),
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        jsonMapper,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
    queryResource = new QueryResource(
        queryLifecycleFactory,
        node,
        serverConfig,
        jsonMapper,
        jsonMapper,
        queryManager,
        testSegmentWalker,
        warehouse,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
  }

  private static final String simpleTimeSeriesQuery = "{\n"
                                                      + "    \"queryType\": \"timeseries\",\n"
                                                      + "    \"dataSource\": \"mmx_metrics\",\n"
                                                      + "    \"granularity\": \"hour\",\n"
                                                      + "    \"intervals\": [\n"
                                                      + "      \"2014-12-17/2015-12-30\"\n"
                                                      + "    ],\n"
                                                      + "    \"aggregations\": [\n"
                                                      + "      {\n"
                                                      + "        \"type\": \"count\",\n"
                                                      + "        \"name\": \"rows\"\n"
                                                      + "      }\n"
                                                      + "    ]\n"
                                                      + "}";

  @Test
  public void testGoodQuery() throws IOException
  {
    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream(simpleTimeSeriesQuery.getBytes("UTF-8")),
        null /*pretty*/,
        null,
        testServletRequest
    );
    Assert.assertNotNull(response);
  }

  @Test(expected = JsonParseException.class)
  public void testBadQuery() throws IOException
  {
    EasyMock.replay(testServletRequest);
    Response response = queryResource.doPost(
        new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes("UTF-8")),
        null /*pretty*/,
        null,
        testServletRequest
    );
    Assert.assertNotNull(response);
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  public void testSecuredQuery() throws Exception
  {
    EasyMock.expect(testServletRequest.getAttribute(QueryResource.GET_FEATURE))
            .andReturn(null)
            .anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().times(1);

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            return new Access(resource.getName().equals("allow"));
          }
        };
      }
    };

    QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        queryManager,
        warehouse,
        testSegmentWalker,
        new DefaultGenericQueryMetricsFactory(jsonMapper),
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        jsonMapper,
        authMapper
    );
    queryResource = new QueryResource(
        queryLifecycleFactory,
        node,
        serverConfig,
        jsonMapper,
        jsonMapper,
        queryManager,
        testSegmentWalker,
        warehouse,
        authMapper
    );

    Response response = queryResource.doPost(
        new ByteArrayInputStream(simpleTimeSeriesQuery.getBytes("UTF-8")),
        null /*pretty*/,
        null,
        testServletRequest
    );
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());

    response = queryResource.doPost(
        new ByteArrayInputStream("{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\"}".getBytes("UTF-8")),
        null /*pretty*/,
        null,
        testServletRequest
    );

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

  }

  @Test(timeout = 60_000L)
  public void testSecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);
    final CountDownLatch cancelledCountDownLatch = new CountDownLatch(1);

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            // READ action corresponds to the query
            // WRITE corresponds to cancellation of query
            if (action.equals(Action.READ)) {
              try {
                // Countdown startAwaitLatch as we want query cancellation to happen
                // after we enter isAuthorized method so that we can handle the
                // InterruptedException here because of query cancellation
                startAwaitLatch.countDown();
                waitForCancellationLatch.await();
              }
              catch (InterruptedException e) {
                // When the query is cancelled the control will reach here,
                // countdown the latch and rethrow the exception so that error response is returned for the query
                cancelledCountDownLatch.countDown();
                throw new RuntimeException(e);
              }
              return new Access(true);
            } else {
              return new Access(true);
            }
          }

        };
      }
    };

    QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        queryManager,
        warehouse,
        testSegmentWalker,
        new DefaultGenericQueryMetricsFactory(jsonMapper),
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        jsonMapper,
        authMapper
    );
    queryResource = new QueryResource(
        queryLifecycleFactory,
        node,
        serverConfig,
        jsonMapper,
        jsonMapper,
        queryManager,
        testSegmentWalker,
        warehouse,
        authMapper
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query query = mapper.readValue(queryString, Query.class);

    ListenableFuture future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              Response response = queryResource.doPost(
                  new ByteArrayInputStream(queryString.getBytes("UTF-8")),
                  null,
                  null,
                  testServletRequest
              );

              Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
            waitFinishLatch.countDown();
          }
        }
    );

    queryManager.registerQuery(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        new Runnable() {
          @Override
          public void run()
          {
            Response response = queryResource.cancelQuery("id_1", testServletRequest);
            Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
            waitForCancellationLatch.countDown();
            waitFinishLatch.countDown();
          }
        }
    );
    waitFinishLatch.await();
    cancelledCountDownLatch.await();
  }

  @Test(timeout = 60_000L)
  public void testDenySecuredCancelQuery() throws Exception
  {
    final CountDownLatch waitForCancellationLatch = new CountDownLatch(1);
    final CountDownLatch waitFinishLatch = new CountDownLatch(2);
    final CountDownLatch startAwaitLatch = new CountDownLatch(1);

    EasyMock.expect(testServletRequest.getAttribute(QueryResource.GET_FEATURE))
            .andReturn(null)
            .anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();

    EasyMock.expect(testServletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(AUTHENTICATION_RESULT)
            .anyTimes();

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().times(1);

    testServletRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(testServletRequest);

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            // READ action corresponds to the query
            // WRITE corresponds to cancellation of query
            if (action.equals(Action.READ)) {
              try {
                waitForCancellationLatch.await();
              }
              catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              return new Access(true);
            } else {
              // Deny access to cancel the query
              return new Access(false);
            }
          }

        };
      }
    };

    QueryLifecycleFactory queryLifecycleFactory = new QueryLifecycleFactory(
        queryManager,
        warehouse,
        testSegmentWalker,
        new DefaultGenericQueryMetricsFactory(jsonMapper),
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        jsonMapper,
        authMapper
    );
    queryResource = new QueryResource(
        queryLifecycleFactory,
        node,
        serverConfig,
        jsonMapper,
        jsonMapper,
        queryManager,
        testSegmentWalker,
        warehouse,
        authMapper
    );

    final String queryString = "{\"queryType\":\"timeBoundary\", \"dataSource\":\"allow\","
                               + "\"context\":{\"queryId\":\"id_1\"}}";
    ObjectMapper mapper = new DefaultObjectMapper();
    Query query = mapper.readValue(queryString, Query.class);

    ListenableFuture future = MoreExecutors.listeningDecorator(
        Execs.singleThreaded("test_query_resource_%s")
    ).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              startAwaitLatch.countDown();
              Response response = queryResource.doPost(
                  new ByteArrayInputStream(queryString.getBytes("UTF-8")),
                  null,
                  null,
                  testServletRequest
              );
              Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
            waitFinishLatch.countDown();
          }
        }
    );

    queryManager.registerQuery(query, future);
    startAwaitLatch.await();

    Executors.newSingleThreadExecutor().submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              queryResource.cancelQuery("id_1", testServletRequest);
            }
            catch (ForbiddenException e) {
              waitForCancellationLatch.countDown();
              waitFinishLatch.countDown();
            }
          }
        }
    );
    waitFinishLatch.await();
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(testServletRequest);
  }
}
