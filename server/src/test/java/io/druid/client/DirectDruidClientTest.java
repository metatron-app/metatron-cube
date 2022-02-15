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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.http.client.ChannelResource;
import io.druid.java.util.http.client.ChannelResources;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.query.Druids;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class DirectDruidClientTest
{
  @Test
  public void testRun() throws Exception
  {
    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final URL url = new URL("http://foo/druid/v2/");

    SettableFuture<InputStream> future = SettableFuture.create();
    ChannelResource<InputStream> resource = ChannelResources.wrap(future);
    Capture<Request> capturedRequest = EasyMock.newCapture();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(resource)
            .times(1);

    SettableFuture futureException = SettableFuture.create();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(ChannelResources.wrap(futureException))
            .times(1);
    SettableFuture<Object> cancellationFuture = SettableFuture.create();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(ChannelResources.wrap(cancellationFuture))
            .once();

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(ChannelResources.wrap(SettableFuture.create()))
            .atLeastOnce();

    EasyMock.replay(httpClient);

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            null,
            0,
            0L
        )
    );

    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    DirectDruidClient client1 = new DirectDruidClient(
        TestIndex.segmentWalker,
        TestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        objectMapper,
        httpClient,
        "foo",
        null,
        new NoopServiceEmitter(),
        new BrokerIOConfig(),
        Execs.newDirectExecutorService()
    );
    DirectDruidClient client2 = new DirectDruidClient(
        TestIndex.segmentWalker,
        TestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        objectMapper,
        httpClient,
        "foo2",
        null,
        new NoopServiceEmitter(),
        new BrokerIOConfig(),
        Execs.newDirectExecutorService()
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer1, serverSelector.getSegment());
    QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client2
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer2, serverSelector.getSegment());

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    HashMap<String, List> context = Maps.newHashMap();
    Sequence s1 = client1.run(query, context);
    Assert.assertTrue(capturedRequest.hasCaptured());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // simulate read timeout
    Sequence s2 = client1.run(query, context);
    Assert.assertEquals(2, client1.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    cancellationFuture.set(new StatusResponseHolder(HttpResponseStatus.OK, new StringBuilder("cancelled")));
    try {
      Sequences.toList(s2, Lists.<Result>newArrayList());
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof QueryInterruptedException);
      Assert.assertTrue(e.getCause() instanceof ReadTimeoutException);
    }

    Assert.assertEquals(1, client1.getNumOpenConnections());

    // subsequent connections should work
    Sequence s3 = client1.run(query, context);
    Sequence s4 = client1.run(query, context);
    Sequence s5 = client1.run(query, context);

    Assert.assertTrue(client1.getNumOpenConnections() == 4);

    // produce result for first connection
    future.set(new ByteArrayInputStream("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]".getBytes()));
    List<Result> results = Sequences.toList(s1, Lists.<Result>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(new DateTime("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assert.assertEquals(3, client1.getNumOpenConnections());

    client2.run(query, context);
    client2.run(query, context);

    Assert.assertTrue(client2.getNumOpenConnections() == 2);

    Assert.assertNotNull(serverSelector.pick(null, new HashMap<>()));

    EasyMock.verify(httpClient);
  }

  @Test
  public void testCancel() throws Exception
  {
    HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);

    Capture<Request> capturedRequest = EasyMock.newCapture();
    ChannelResource<Object> cancelledResource = ChannelResources.wrap(Futures.immediateCancelledFuture());
    SettableFuture<Object> future = SettableFuture.create();
    ChannelResource<Object> cancellationResource = ChannelResources.wrap(future);

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andAnswer(() -> {
              Thread.sleep(1000);
              return cancelledResource;
            })
            .once();

    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(cancellationResource)
            .once();

    EasyMock.replay(httpClient);

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            null,
            0,
            0L
        )
    );

    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final QueryWatcher noopQuerywatcher = new QueryWatcher.Abstract()
    {
      @Override
      public long remainingTime(String queryId)
      {
        return 1000L;
      }
    };
    DirectDruidClient client1 = new DirectDruidClient(
        TestIndex.segmentWalker,
        noopQuerywatcher,
        objectMapper,
        objectMapper,
        httpClient,
        "foo",
        null,
        new NoopServiceEmitter(),
        new BrokerIOConfig(),
        Execs.newDirectExecutorService()
    );

    QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client1
    );
    serverSelector.addServerAndUpdateSegment(queryableDruidServer1, serverSelector.getSegment());

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    HashMap<String, List> context = Maps.newHashMap();
    future.set(new StatusResponseHolder(HttpResponseStatus.OK, new StringBuilder("cancelled")));
    Sequence results = client1.run(query, context);
    try {
      Sequences.toList(results, Lists.<Result>newArrayList());
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof QueryInterruptedException);
      Assert.assertTrue(e.getCause() instanceof TimeoutException);
    }
    Assert.assertEquals(HttpMethod.DELETE, capturedRequest.getValue().getMethod());
    Assert.assertEquals(0, client1.getNumOpenConnections());

    EasyMock.verify(httpClient);
  }

  @Test
  public void testQueryInterruptionExceptionLogMessage() throws JsonProcessingException
  {
    HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    SettableFuture<Object> future = SettableFuture.create();
    ChannelResource<Object> resource = ChannelResources.wrap(future);
    Capture<Request> capturedRequest = EasyMock.newCapture();
    String hostName = "localhost:8080";
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(resource)
            .anyTimes();

    EasyMock.replay(httpClient);

    DataSegment dataSegment = new DataSegment(
        "test",
        new Interval("2013-01-01/2013-01-02"),
        new DateTime("2013-01-01").toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        null,
        0,
        0L
    );
    final ServerSelector serverSelector = new ServerSelector(dataSegment);

    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    DirectDruidClient client1 = new DirectDruidClient(
        TestIndex.segmentWalker,
        TestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        objectMapper,
        httpClient,
        hostName,
        null,
        new NoopServiceEmitter(),
        new BrokerIOConfig(),
        Execs.newDirectExecutorService()
    );

    QueryableDruidServer queryableDruidServer = new QueryableDruidServer(
        new DruidServer("test1", hostName, 0, "historical", DruidServer.DEFAULT_TIER, 0),
        client1
    );

    serverSelector.addServerAndUpdateSegment(queryableDruidServer, dataSegment);

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").build();
    HashMap<String, List> context = Maps.newHashMap();
    future.set(new ByteArrayInputStream("{\"error\":\"testing1\",\"errorMessage\":\"testing2\"}".getBytes()));
    Sequence results = client1.run(query, context);

    QueryInterruptedException actualException = null;
    try {
      Sequences.toList(results, Lists.newArrayList());
    }
    catch (QueryInterruptedException e) {
      actualException = e;
    }
    Assert.assertNotNull(actualException);
    Assert.assertEquals("testing1", actualException.getErrorCode());
    Assert.assertEquals("testing2", actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
    EasyMock.verify(httpClient);
  }
}
