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
package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.client.BrokerIOConfig;
import io.druid.client.DirectDruidClient;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.TimelineServerView;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.Intervals;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.InputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import io.druid.java.util.http.client.response.FullResponseHolder;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.ReflectionQueryToolChestWarehouse;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.BytesAccumulatingResponseHandler;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.RowSignature;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.sql.calcite.util.TestServerInventoryView;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.timeline.DataSegment;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SystemSchemaTest extends CalciteTestBase
{
  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();

  private static final List<InputRow> ROWS1 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  private static final List<InputRow> ROWS2 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  private SystemSchema schema;
  private TestQuerySegmentWalker walker;
  private CoordinatorClient coordinatorClient;
  private IndexingServiceClient indexingServiceClient;
  private TimelineServerView serverView;
  private ObjectMapper mapper;
  private FullResponseHolder responseHolder;
  private BytesAccumulatingResponseHandler responseHandler;
  private Request request;
  private DruidSchema druidSchema;
  //private static QueryRunnerFactoryConglomerate conglomerate;
  //private static Closer resourceCloser;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass()
  {
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
  }

  @Before
  public void setUp() throws Exception
  {
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    indexingServiceClient = EasyMock.createMock(IndexingServiceClient.class);
    // TestHelper.makeJsonMapper()
    mapper = TestHelper.getTestObjectMapper();
    responseHolder = EasyMock.createMock(FullResponseHolder.class);
    responseHandler = EasyMock.createMockBuilder(BytesAccumulatingResponseHandler.class)
                              .withConstructor()
                              .addMockedMethod(
                                  "handleResponse",
                                  HttpResponse.class
                              )
                              .addMockedMethod("getStatus")
                              .createMock();
    request = EasyMock.createMock(Request.class);

    final File tmpDir = temporaryFolder.newFolder();
    final QueryableIndex index1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(
                                                          new CountAggregatorFactory("cnt"),
                                                          new DoubleSumAggregatorFactory("m1", "m1"),
                                                          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                      )
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS2)
                                              .buildMMappedIndex();

    walker = CalciteTests.newSegmentWalker()
        .add(segment1, index1)
        .add(segment2, index2)
        .add(segment2, index2)
        .add(segment3, index2);

    druidSchema = new DruidSchema(
        walker,
        new TestServerInventoryView(walker.getSegments()),
        new NoopViewManager()
    );

    schema = new SystemSchema(
        druidSchema,
        serverView,
        coordinatorClient,
        indexingServiceClient,
        CalciteTests.createOperatorTable(),
        mapper
    );
  }

  private final DataSegment segment1 = new DataSegment(
      "test1",
      Intervals.of("2010/2011"),
      "version1",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment2 = new DataSegment(
      "test2",
      Intervals.of("2011/2012"),
      "version2",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment3 = new DataSegment(
      "test3",
      Intervals.of("2012/2013"),
      "version3",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment4 = new DataSegment(
      "test4",
      Intervals.of("2017/2018"),
      "version4",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment5 = new DataSegment(
      "test5",
      Intervals.of("2017/2018"),
      "version5",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );

  private final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
  private final DirectDruidClient client1 = new DirectDruidClient(
      new ReflectionQueryToolChestWarehouse(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER,
      new DefaultObjectMapper(),
      httpClient,
      "http",
      "foo",
      new NoopServiceEmitter(),
      new BrokerIOConfig(),
      MoreExecutors.sameThreadExecutor()
  );

  private final DirectDruidClient client2 = new DirectDruidClient(
      new ReflectionQueryToolChestWarehouse(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER,
      new DefaultObjectMapper(),
      httpClient,
      "http",
      "foo2",
      new NoopServiceEmitter(),
      new BrokerIOConfig(),
      MoreExecutors.sameThreadExecutor()
  );
  private final ImmutableDruidServer druidServer1 = new ImmutableDruidServer(
      new DruidServerMetadata("server1", "localhost:0000", 5L, "realtime", DruidServer.DEFAULT_TIER, 0),
      1L,
      null,
      ImmutableMap.of("segment1", segment1, "segment2", segment2)
  );

  private final ImmutableDruidServer druidServer2 = new ImmutableDruidServer(
      new DruidServerMetadata("server2", "server2:1234", 5L, "historical", DruidServer.DEFAULT_TIER, 0),
      1L,
      null,
      ImmutableMap.of("segment2", segment2, "segment4", segment4, "segment5", segment5)
  );

  private final List<ImmutableDruidServer> immutableDruidServers = ImmutableList.of(druidServer1, druidServer2);

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(ImmutableSet.of("locks", "segments", "servers", "server_segments", "tasks", "functions"), schema.getTableNames());

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(ImmutableSet.of("locks", "segments", "servers", "server_segments", "tasks", "functions"), tableMap.keySet());
    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(13, fields.size());

    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType sysRowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> sysFields = sysRowType.getFieldList();
    Assert.assertEquals(13, sysFields.size());

    Assert.assertEquals("task_id", sysFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, sysFields.get(0).getType().getSqlTypeName());

    final SystemSchema.ServersTable serversTable = (SystemSchema.ServersTable) schema.getTableMap().get("servers");
    final RelDataType serverRowType = serversTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> serverFields = serverRowType.getFieldList();
    Assert.assertEquals(8, serverFields.size());
    Assert.assertEquals("server", serverFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, serverFields.get(0).getType().getSqlTypeName());
  }

  @Test
  @Ignore("I don't care about availability of segments")
  public void testSegmentsTable() throws Exception
  {
    // total segments = 6
    // segments 1,2,3 are published and available
    // segments 4,5,6  are published but unavailable
    // segment 3 is published but not served
    // segment 2 is served by 2 servers, so num_replicas=2

    final SystemSchema.SegmentsTable segmentsTable = EasyMock.createMockBuilder(SystemSchema.SegmentsTable.class).withConstructor(
        druidSchema, coordinatorClient, mapper, responseHandler).createMock();
    EasyMock.replay(segmentsTable);

    EasyMock.expect(coordinatorClient.makeRequest(HttpMethod.GET, "/metadata/segments")).andReturn(request).anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(coordinatorClient.goAsync(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).once();

    EasyMock.expect(request.getUrl()).andReturn(new URL("http://test-host:1234/druid/coordinator/v1/metadata/segments")).anyTimes();

    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    //published but unavailable segments
    final String json = "[{\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T23:00:00.000Z/2018-08-08T00:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T23:00:00.059Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z/2018-08-07T23:00:00.059Z/51/1578eb79-0e44-4b41-a87b-65e40c52be53/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"none\",\n"
                        + "\t\t\"partitionNum\": 51,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 47406,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z_2018-08-07T23:00:00.059Z_51\"\n"
                        + "}, {\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T18:00:00.000Z/2018-08-07T19:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T18:00:00.117Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T18:00:00.000Z_2018-08-07T19:00:00.000Z/2018-08-07T18:00:00.117Z/9/a2646827-b782-424c-9eed-e48aa448d2c5/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,metroCode,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"none\",\n"
                        + "\t\t\"partitionNum\": 9,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 83846,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T18:00:00.000Z_2018-08-07T19:00:00.000Z_2018-08-07T18:00:00.117Z_9\"\n"
                        + "}, {\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T23:00:00.000Z/2018-08-08T00:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T23:00:00.059Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z/2018-08-07T23:00:00.059Z/50/87c5457e-c39b-4c03-9df8-e2b20b210dfc/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,metroCode,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"none\",\n"
                        + "\t\t\"partitionNum\": 50,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 53527,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z_2018-08-07T23:00:00.059Z_50\"\n"
                        + "}]";
    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    in.add(bytesToWrite);
    in.done();
    future.set(in);

    EasyMock.replay(coordinatorClient, request, responseHolder, responseHandler);
    DataContext dataContext = new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String name)
      {
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    Enumerable<Object[]> rows = segmentsTable.scan(dataContext);
    Enumerator<Object[]> enumerator = rows.enumerator();

    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row2 = enumerator.current();
    //segment 2 is published and has 2 replicas
    Assert.assertEquals(1L, row2[9]);
    Assert.assertEquals(2L, row2[7]);
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row6 = enumerator.current();
    //segment 6 is published and unavailable, num_replicas is 0
    Assert.assertEquals(1L, row6[9]);
    Assert.assertEquals(0L, row6[7]);
    Assert.assertEquals(false, enumerator.moveNext());

  }

  @Test
  public void testServersTable()
  {

    SystemSchema.ServersTable serversTable = EasyMock.createMockBuilder(SystemSchema.ServersTable.class).withConstructor(serverView).createMock();
    EasyMock.replay(serversTable);

    EasyMock.expect(serverView.getDruidServers())
            .andReturn(immutableDruidServers)
            .once();
    EasyMock.replay(serverView);
    DataContext dataContext = new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String name)
      {
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    final List<Object[]> rows = serversTable.scan(dataContext).toList();
    Assert.assertEquals(2, rows.size());
    Object[] row1 = rows.get(0);
    Assert.assertEquals("localhost:0000", row1[0]);
    Assert.assertEquals("realtime", row1[4].toString());
    Object[] row2 = rows.get(1);
    Assert.assertEquals("server2:1234", row2[0]);
    Assert.assertEquals("historical", row2[4].toString());

    // Verify value types.
    verifyTypes(rows, SystemSchema.SERVERS_SIGNATURE);
  }

  @Test
  public void testTasksTable() throws Exception
  {

    SystemSchema.TasksTable tasksTable = EasyMock.createMockBuilder(SystemSchema.TasksTable.class)
                                                 .withConstructor(indexingServiceClient, mapper, responseHandler)
                                                 .createMock();
    EasyMock.replay(tasksTable);
    EasyMock.expect(indexingServiceClient.makeRequest(HttpMethod.GET, "/tasks")).andReturn(request).anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(indexingServiceClient.goAsync(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).once();
    EasyMock.expect(request.getUrl()).andReturn(new URL("http://test-host:1234/druid/indexer/v1/tasks")).anyTimes();

    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();


    String json = "[{\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-20T22:33:44.922Z\",\n"
                  + "\t\"queueInsertionTime\": \"1970-01-01T00:00:00.000Z\",\n"
                  + "\t\"statusCode\": \"FAILED\",\n"
                  + "\t\"runnerStatusCode\": \"NONE\",\n"
                  + "\t\"duration\": -1,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"testHost\",\n"
                  + "\t\t\"port\": 1234,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}, {\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-21T18:38:47.773Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-21T18:38:47.873Z\",\n"
                  + "\t\"queueInsertionTime\": \"2018-09-21T18:38:47.910Z\",\n"
                  + "\t\"statusCode\": \"RUNNING\",\n"
                  + "\t\"runnerStatusCode\": \"RUNNING\",\n"
                  + "\t\"duration\": null,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"192.168.1.6\",\n"
                  + "\t\t\"port\": 8100,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}]";
    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    in.add(bytesToWrite);
    in.done();
    future.set(in);

    EasyMock.replay(indexingServiceClient, request, responseHandler);
    DataContext dataContext = new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String name)
      {
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    final List<Object[]> rows = tasksTable.scan(dataContext).toList();

    Object[] row0 = rows.get(0);
    Assert.assertEquals("index_wikipedia_2018-09-20T22:33:44.911Z", row0[0].toString());
    Assert.assertEquals("FAILED", row0[5].toString());
    Assert.assertEquals("NONE", row0[6].toString());
    Assert.assertEquals(-1L, row0[7]);
    Assert.assertEquals("testHost:1234", row0[8]);

    Object[] row1 = rows.get(1);
    Assert.assertEquals("index_wikipedia_2018-09-21T18:38:47.773Z", row1[0].toString());
    Assert.assertEquals("RUNNING", row1[5].toString());
    Assert.assertEquals("RUNNING", row1[6].toString());
    Assert.assertEquals(0L, row1[7]);
    Assert.assertEquals("192.168.1.6:8100", row1[8]);

    // Verify value types.
    verifyTypes(rows, SystemSchema.TASKS_SIGNATURE);
  }

  private static void verifyTypes(final List<Object[]> rows, final RowSignature signature)
  {
    final RelDataType rowType = signature.getRelDataType(new JavaTypeFactoryImpl());

    for (Object[] row : rows) {
      Assert.assertEquals(row.length, signature.getRowOrder().size());

      for (int i = 0; i < row.length; i++) {
        final Class<?> expectedClass;

        final ValueDesc columnType = signature.getColumnType(signature.getRowOrder().get(i));
        final boolean nullable = rowType.getFieldList().get(i).getType().isNullable();

        switch (columnType.type()) {
          case LONG:
            expectedClass = Long.class;
            break;
          case FLOAT:
            expectedClass = Float.class;
            break;
          case DOUBLE:
            expectedClass = Double.class;
            break;
          case STRING:
            expectedClass = String.class;
            break;
          default:
            throw new IAE("Don't know what class to expect for valueType[%s]", columnType);
        }

        if (nullable) {
          Assert.assertTrue(
              StringUtils.format(
                  "Column[%s] is a [%s] or null (was %s)",
                  signature.getRowOrder().get(i),
                  expectedClass.getName(),
                  row[i] == null ? null : row[i].getClass().getName()
              ),
              row[i] == null || expectedClass.isAssignableFrom(row[i].getClass())
          );
        } else {
          Assert.assertTrue(
              StringUtils.format(
                  "Column[%s] is a [%s] (was %s)",
                  signature.getRowOrder().get(i),
                  expectedClass.getName(),
                  row[i] == null ? null : row[i].getClass().getName()
              ),
              row[i] != null && expectedClass.isAssignableFrom(row[i].getClass())
          );
        }
      }
    }
  }

}
