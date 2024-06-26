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

package io.druid.sql.calcite.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryException;
import io.druid.query.QueryException.Code;
import io.druid.query.sql.SQLFunctions;
import io.druid.server.DruidNode;
import io.druid.server.QueryManager;
import io.druid.server.QueryResource;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.ForbiddenException;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.sql.http.ResultFormat;
import io.druid.sql.http.SqlQuery;
import io.druid.sql.http.SqlResource;
import org.apache.calcite.tools.ValidationException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SqlResourceTest extends CalciteTestBase
{
  static {
    Parser.register(SQLFunctions.class);
  }

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final String DUMMY_QUERY_ID = "dummy";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private TestQuerySegmentWalker walker = null;

  private SqlResource resource;

  private HttpServletRequest req;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());

    final PlannerConfig plannerConfig = new PlannerConfig();
    final QueryConfig queryConfig = new QueryConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(walker, plannerConfig);
    final SystemSchema systemSchema =CalciteTests.createMockSystemSchema(walker);
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(req.getAttribute(QueryResource.GET_FEATURE))
            .andReturn(null)
            .anyTimes();

    EasyMock.replay(req);

    final PlannerFactory plannerFactory = new PlannerFactory(
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker),
        walker,
        new QueryManager(),
        operatorTable,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        plannerConfig,
        CalciteTests.getJsonMapper()
    );

    resource = new SqlResource(
        JSON_MAPPER,
        new SqlLifecycleFactory(
            plannerFactory,
            new NoopServiceEmitter(),
            new NoopRequestLogger(),
            null
        ),
        new DruidNode("test", "test", -1)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker = null;
  }

    @Test
  public void testUnauthorized() throws Exception
  {
    HttpServletRequest testRequest = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(testRequest.getRemoteAddr()).andReturn(null).once();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
            .andReturn(null)
            .anyTimes();
    EasyMock.expect(testRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(CalciteTests.REGULAR_USER_AUTH_RESULT)
            .anyTimes();
    testRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().once();
    EasyMock.replay(testRequest);

    try {
      resource.doPost(
          new SqlQuery("select count(*) from forbiddenDatasource", null, false, null),
          testRequest
      );
      Assert.fail("doPost did not throw ForbiddenException for an unauthorized query");
    }
    catch (ForbiddenException e) {
      // expected
    }
  }

  @Test
  public void testCountStar() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT COUNT(*) AS cnt, 'foo' AS TheFoo FROM druid.foo", null, false, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6, "TheFoo", "foo")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT.INSTANCE,
            false,
            null
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "2000-01-01T00:00:00.000Z", "t2", "2000-01-01T00:00:00.000Z")
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponseLosAngelesTimeZone() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery(
            "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1",
            ResultFormat.OBJECT.INSTANCE,
            false,
            ImmutableMap.of(PlannerContext.CTX_SQL_TIME_ZONE, "America/Los_Angeles")
        )
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("__time", "1999-12-31T16:00:00.000-08:00", "t2", "1999-12-31T00:00:00.000-08:00")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingSelect() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo LIMIT 1", ResultFormat.OBJECT.INSTANCE, false, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "a", "y", "a")
        ),
        rows
    );
  }

  @Test
  public void testFieldAliasingGroupBy() throws Exception
  {
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("SELECT dim2 \"x\", dim2 \"y\" FROM druid.foo GROUP BY dim2", ResultFormat.OBJECT.INSTANCE, false, null)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "", "y", ""),
            ImmutableMap.of("x", "a", "y", "a"),
            ImmutableMap.of("x", "abc", "y", "abc")
        ),
        rows
    );
  }

  @Test
  public void testArrayResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    //final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final String nullStr = "";

    Assert.assertEquals(
        ImmutableList.of(
            Arrays.asList("2000-01-01T00:00:00.000Z", 1, "", "a", 1.0, 1.0, "WQAAAQAAAAKTEA==", null),
            Arrays.asList("2000-01-02T00:00:00.000Z", 1, "10.1", nullStr, 2.0, 2.0, "WQAAAQAAAAFZAw==", null)
        ),
        doPost(new SqlQuery(query, ResultFormat.ARRAY.INSTANCE, false, null), new TypeReference<List<List<Object>>>() {}).rhs
    );
  }

  @Test
  public void testArrayLinesResultFormat() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.ARRAYLINES.INSTANCE, false, null)).rhs;
    //final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final String nullStr = "";

    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(4, lines.size());
    Assert.assertEquals(
        Arrays.asList("2000-01-01T00:00:00.000Z", 1, "", "a", 1.0, 1.0, "WQAAAQAAAAKTEA==", null),
        JSON_MAPPER.readValue(lines.get(0), List.class)
    );
    Assert.assertEquals(
        Arrays.asList("2000-01-02T00:00:00.000Z", 1, "10.1", nullStr, 2.0, 2.0, "WQAAAQAAAAFZAw==", null),
        JSON_MAPPER.readValue(lines.get(1), List.class)
    );
    Assert.assertEquals("", lines.get(2));
    Assert.assertEquals("", lines.get(3));
  }

  @Test
  public void testArrayLinesResultFormatWithHeader() throws Exception
  {
    final String query = "SELECT *, CASE dim2 WHEN '' THEN dim2 END FROM foo LIMIT 2";
    final String response = doPostRaw(new SqlQuery(query, ResultFormat.ARRAYLINES.INSTANCE, true, null)).rhs;
    //final String nullStr = NullHandling.replaceWithDefault() ? "" : null;
    final String nullStr = "";

    final List<String> lines = Splitter.on('\n').splitToList(response);

    Assert.assertEquals(5, lines.size());
    Assert.assertEquals(
        Arrays.asList("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1", "EXPR$7"),
        JSON_MAPPER.readValue(lines.get(0), List.class)
    );
    Assert.assertEquals(
        Arrays.asList("2000-01-01T00:00:00.000Z", 1, "", "a", 1.0, 1.0, "WQAAAQAAAAKTEA==", null),
        JSON_MAPPER.readValue(lines.get(1), List.class)
    );
    Assert.assertEquals(
        Arrays.asList("2000-01-02T00:00:00.000Z", 1, "10.1", nullStr, 2.0, 2.0, "WQAAAQAAAAFZAw==", null),
        JSON_MAPPER.readValue(lines.get(2), List.class)
    );
    Assert.assertEquals("", lines.get(3));
    Assert.assertEquals("", lines.get(4));
  }

  @Test
  public void testExplainCountStar() throws Exception
  {
    Map<String, Object> queryContext = ImmutableMap.of(Query.QUERYID, DUMMY_QUERY_ID);
    final List<Map<String, Object>> rows = doPost(
        new SqlQuery("EXPLAIN PLAN WITH TYPE FOR SELECT COUNT(*) AS cnt FROM druid.foo", ResultFormat.OBJECT.INSTANCE, false, queryContext)
    ).rhs;

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.<String, Object>of(
                "PLAN",
                "DruidQueryRel(table=[druid.foo], cnt=[COUNT()])\n"
            )
        ),
        rows
    );
  }

  @Test
  public void testCannotValidate() throws Exception
  {
    final QueryException exception = doPost(
        new SqlQuery(
            "SELECT dim3 FROM druid.foo",
            ResultFormat.OBJECT.INSTANCE,
            false,
            null
        )
    ).lhs;

    Assert.assertNotNull(exception);
    Assert.assertEquals(Code.UNKNOWN, exception.getErrorCode());
    Assert.assertEquals(ValidationException.class.getName(), exception.getErrorClass());
    Assert.assertTrue(exception.getMessage().contains("Column 'dim3' not found in any table"));
  }

  @Test
  @Ignore("we supports")
  public void testCannotConvert() throws Exception
  {
    // SELECT + ORDER unsupported
    Throwable exception = doPost(new SqlQuery("SELECT dim1 FROM druid.foo ORDER BY dim1", ResultFormat.OBJECT.INSTANCE.INSTANCE, false,null)).lhs;
    Assert.assertNotNull(exception);
  }

  // Returns either an error or a result, assuming the result is a JSON object.
  private <T> Pair<QueryException, T> doPost(
      final SqlQuery query,
      final TypeReference<T> typeReference
  ) throws Exception
  {
    final Pair<QueryException, String> pair = doPostRaw(query);
    if (pair.rhs == null) {
      //noinspection unchecked
      return (Pair<QueryException, T>) pair;
    } else {
      return Pair.of(pair.lhs, JSON_MAPPER.readValue(pair.rhs, typeReference));
    }
  }

  // Returns either an error or a result.
  private Pair<QueryException, String> doPostRaw(final SqlQuery query) throws Exception
  {
    final Response response = resource.doPost(query, req);
    if (response.getStatus() == 200) {
      final StreamingOutput output = (StreamingOutput) response.getEntity();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      output.write(baos);
      return Pair.of(
          null,
          new String(baos.toByteArray(), StandardCharsets.UTF_8)
      );
    } else {
      return Pair.of(
          JSON_MAPPER.readValue((byte[]) response.getEntity(), QueryException.class),
          null
      );
    }
  }

  private Pair<QueryException, List<Map<String, Object>>> doPost(final SqlQuery query) throws Exception
  {
    return doPost(query, new TypeReference<List<Map<String, Object>>>() {});
  }
}
