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

package io.druid.sql.calcite;

import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.Parser;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.Query;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class SimpleTest extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final TestQuerySegmentWalker walker = TestHelper.newWalker().withQueryHook(hook);

  private static final Map<String, Object> GROUP_CONTEXT = ImmutableMap.of(
      Query.GROUPED_DIMENSIONS, "bks_event_d0,bks_event_d1,bks_event_d2"
  );

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker.addCdisIndex().addIndex("part_i", "part_schema.json", "part.tbl", false);
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testGroupBy() throws Exception
  {
    testQuery(
        "WITH emp AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   ('Haki',    'R&D',      'Manager'),"
        + "   ('Dan',     'R&D',      'Developer'),"
        + "   ('Jax',     'R&D',      'Developer'),"
        + "   ('George',  'Sales',    'Manager'),"
        + "   ('Bill',    'Sales',    'Developer'),"
        + "   ('David',   'Sales',    'Developer')"
        + " ) AS T (name, department, role)"
        + ")"
        + " SELECT department, role, COUNT(*) FROM emp GROUP BY department, role",
        new Object[]{"Sales", "Developer", 2L},
        new Object[]{"R&D", "Developer", 2L},
        new Object[]{"R&D", "Manager", 1L},
        new Object[]{"Sales", "Manager", 1L}
    );
  }

  @Test
  public void testRollup() throws Exception
  {
    testQuery(
        "WITH emp AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   ('Haki',    'R&D',      'Manager'),"
        + "   ('Dan',     'R&D',      'Developer'),"
        + "   ('Jax',     'R&D',      'Developer'),"
        + "   ('George',  'Sales',    'Manager'),"
        + "   ('Bill',    'Sales',    'Developer'),"
        + "   ('David',   'Sales',    'Developer')"
        + " ) AS T (name, department, role)"
        + ")"
        + " SELECT department, role, COUNT(*) FROM emp GROUP BY ROLLUP(department, role)",
        new Object[]{"", "", 6L},
        new Object[]{"R&D", "", 3L},
        new Object[]{"R&D", "Developer", 2L},
        new Object[]{"R&D", "Manager", 1L},
        new Object[]{"Sales", "", 3L},
        new Object[]{"Sales", "Developer", 2L},
        new Object[]{"Sales", "Manager", 1L}
    );
  }

  @Test
  public void testCube() throws Exception
  {
    testQuery(
        "WITH emp AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   ('Haki',    'R&D',      'Manager'),"
        + "   ('Dan',     'R&D',      'Developer'),"
        + "   ('Jax',     'R&D',      'Developer'),"
        + "   ('George',  'Sales',    'Manager'),"
        + "   ('Bill',    'Sales',    'Developer'),"
        + "   ('David',   'Sales',    'Developer')"
        + " ) AS T (name, department, role)"
        + ")"
        + " SELECT department, role, COUNT(*) FROM emp GROUP BY CUBE(department, role)",
        new Object[]{"", "", 6L},
        new Object[]{"", "Developer", 4L},
        new Object[]{"", "Manager", 2L},
        new Object[]{"R&D", "", 3L},
        new Object[]{"R&D", "Developer", 2L},
        new Object[]{"R&D", "Manager", 1L},
        new Object[]{"Sales", "", 3L},
        new Object[]{"Sales", "Developer", 2L},
        new Object[]{"Sales", "Manager", 1L}
    );
  }

  @Test
  public void testComplex() throws Exception
  {
    testQuery(
        "WITH emp AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   ('Haki',    'R&D',      'Manager'),"
        + "   ('Dan',     'R&D',      'Developer'),"
        + "   ('Jax',     'R&D',      'Developer'),"
        + "   ('George',  'Sales',    'Manager'),"
        + "   ('Bill',    'Sales',    'Developer'),"
        + "   ('David',   'Sales',    'Developer')"
        + " ) AS T (name, department, role)"
        + ")"
        + " SELECT department, role, COUNT(*) FROM emp GROUP BY ROLLUP(department), role",
        new Object[]{"", "Developer", 4L},
        new Object[]{"", "Manager", 2L},
        new Object[]{"R&D", "Developer", 2L},
        new Object[]{"R&D", "Manager", 1L},
        new Object[]{"Sales", "Developer", 2L},
        new Object[]{"Sales", "Manager", 1L}
    );
  }

  @Test
  public void testGroupingSet() throws Exception
  {
    testQuery(
        "WITH emp AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   ('Haki',    'R&D',      'Manager'),"
        + "   ('Dan',     'R&D',      'Developer'),"
        + "   ('Jax',     'R&D',      'Developer'),"
        + "   ('George',  'Sales',    'Manager'),"
        + "   ('Bill',    'Sales',    'Developer'),"
        + "   ('David',   'Sales',    'Developer')"
        + " ) AS T (name, department, role)"
        + ")"
        + " SELECT department, role, COUNT(*) FROM emp GROUP BY GROUPING SETS ( (), (role), (department) )",
        new Object[]{"", "", 6L},
        new Object[]{"", "Developer", 4L},
        new Object[]{"", "Manager", 2L},
        new Object[]{"R&D", "", 3L},
        new Object[]{"Sales", "", 3L}
    );
  }

  @Test
  public void test3877() throws Exception
  {
    testQuery(
        "SELECT bks_event_d0, bks_event_d1, bks_event_d2 FROM cdis WHERE svc_mgmt_num = '10000497' limit 10",
        GROUP_CONTEXT,
        new Object[]{
            "[T114, APP, T114, APP]",
            "[금융, IT, 음식, 생활]",
            "[신용카드사, 티월드다이렉트(tworlddirect.com), 치킨, 도미노피자(Dominopizza)]"
        }
    );
    testQuery(
        "SELECT age_group, bks_event_d0, bks_event_d1, bks_event_d2, count(*) as cnt FROM cdis "
        + "WHERE svc_mgmt_num = '10000497' GROUP BY age_group, bks_event_d0, bks_event_d1, bks_event_d2",
        GROUP_CONTEXT,
        new Object[]{"10", "APP", "IT", "티월드다이렉트(tworlddirect.com)", 1L},
        new Object[]{"10", "APP", "생활", "도미노피자(Dominopizza)", 1L},
        new Object[]{"10", "T114", "금융", "신용카드사", 1L},
        new Object[]{"10", "T114", "음식", "치킨", 1L}
    );
  }

  @Test
  public void test3887() throws Exception
  {
    testQuery(
        "SELECT bks_event_d0 FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000498') "
        + " UNION ALL "
        + "SELECT bks_event_d1 FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000499')",
        new Object[]{"[T114, APP, T114, APP]"},
        new Object[]{"[금융, IT, 음식, 생활]"}
    );
    testQuery(
        "SELECT purpose, count(*) as cnt "
        + "FROM ( SELECT bks_event_d0 as purpose FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000498') "
        + " UNION ALL "
        + "SELECT bks_event_d1 as purpose FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000499')"
        + ") GROUP BY purpose",
        new Object[]{"T114", 2L},
        new Object[]{"APP", 2L},
        new Object[]{"금융", 1L},
        new Object[]{"IT", 1L},
        new Object[]{"음식", 1L},
        new Object[]{"생활", 1L}
    );
  }

  @Test
  public void test3891() throws Exception
  {
    testQuery(
        "SELECT purpose, occupation, count(*) as cnt FROM ("
        + "  SELECT bks_event_d0 as purpose, age_group as occupation FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000498') "
        + "    UNION ALL "
        + "  SELECT bks_event_d1 as purpose, age_group as occupation FROM cdis WHERE svc_mgmt_num IN ('10000497', '10000499')"
        + ") "
        + "GROUP BY purpose, occupation",
        GROUP_CONTEXT,
        new Object[]{"T114", "10", 2L},
        new Object[]{"APP", "10", 2L},
        new Object[]{"금융", "10", 1L},
        new Object[]{"IT", "10", 1L},
        new Object[]{"음식", "10", 1L},
        new Object[]{"생활", "10", 1L}
    );
    testQuery(
        "SELECT cdis.age_group, cdis.bks_event_d0, count(*) FROM cdis INNER JOIN cdis cdis2 ON cdis.svc_mgmt_num = cdis2.svc_mgmt_num"
        + " GROUP BY cdis.age_group, cdis.bks_event_d0",
        GROUP_CONTEXT,
        new Object[]{"10", "APP", 2L},
        new Object[]{"", "APP", 2L},
        new Object[]{"10", "T114", 2L},
        new Object[]{"", "T114", 2L}
    );
  }

  @Test
  public void test3896() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT count(*) FROM cdis INNER JOIN cdis cdis2 ON cdis.svc_mgmt_num = cdis2.svc_mgmt_num",
        new Object[]{2L}
    );
  }

  @Test
  public void testLatestEarliest() throws Exception
  {
    testQuery(
        "WITH X AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   (0, 'A', 1, null, null),"
        + "   (0, 'B', null, 2, null),"
        + "   (0, 'C', null, null, 3),"
        + "   (1, 'A', 4, null, null),"
        + "   (1, 'B', null, 5, null),"
        + "   (1, 'C', null, null, 6)"
        + " ) AS T (__time, asset, p1, p2, p3)"
        + ")"
        + " SELECT LATEST(p1), LATEST(p2), EARLIEST(p3) FROM X",
        new Object[]{4, 5, 3}
    );
  }

  @Test
  public void test3955() throws Exception
  {
    testQuery(
        "WITH X AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   (0, 'A', 1, null, null),"
        + "   (0, 'B', null, 2, null),"
        + "   (0, 'C', null, null, 3),"
        + "   (1, 'A', 4, null, null),"
        + "   (1, 'B', null, 5, null),"
        + "   (1, 'C', null, null, 6)"
        + " ) AS T (__time, asset, p1, p2, p3)"
        + ")"
        + " SELECT nvlPrev(p1) over (), nvlNext(p1) over (),"
        + "        nvlPrev(p2) over (), nvlNext(p2) over (),"
        + "        nvlPrev(p3) over (), nvlNext(p3) over () FROM X",
        new Object[]{1L, 1L, null, 2L, null, 3L},
        new Object[]{1L, 4L, 2L, 2L, null, 3L},
        new Object[]{1L, 4L, 2L, 5L, 3L, 3L},
        new Object[]{4L, 4L, 2L, 5L, 3L, 6L},
        new Object[]{4L, null, 5L, 5L, 3L, 6L},
        new Object[]{4L, null, 5L, null, 6L, 6L}
    );
  }

  @Test
  public void test3962() throws Exception
  {
    testQuery(
        "WITH X AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   (0, '{\"a\": \"[1,2]\", \"b\": [1,2], \"c\": \"hi\", \"d\": 3}')"
        + " ) AS T (__time, v)"
        + ")"
        + "SELECT cast(v as int),"
        + "       json_value(v, '$.a'),"
        + "       json_value(v, '$.b'),"
        + "       json_value(v, '$.b' DEFAULT 'x' ON EMPTY),"
        + "       json_value(v, '$.b' DEFAULT __time ON EMPTY),"
        + "       json_value(v, '$.c'),"
        + "       json_value(v, '$.d' RETURNING int) FROM X",
        new Object[]{null, "[1,2]", "", "x", "0", "hi", 3}
    );
    testQuery(
        "WITH X AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   (0, 'k1', '{\"field1\":\"value1\", \"field2\": 2}'),"
        + "   (0, 'k2', '{\"field1\":\"noval1\", \"field2\": 2}')"
        + " ) AS T (__time, assetId, ctx)"
        + ")"
        + "SELECT * FROM X WHERE json_value(ctx, '$.field1') like 'value%'",
        new Object[]{0, "k1", "{\"field1\":\"value1\", \"field2\": 2}"}
    );
  }

  @Test
  public void test3981() throws Exception
  {
    testQuery(
        "SELECT P_CONTAINER, P_SIZE FROM part_i WHERE P_SIZE > 7.0 AND P_SIZE < 8.5",
        new Object[]{"LG DRUM", 8L},
        new Object[]{"MED PACK", 8L},
        new Object[]{"MED JAR", 8L},
        new Object[]{"SM DRUM", 8L},
        new Object[]{"JUMBO BOX", 8L},
        new Object[]{"LG PKG", 8L},
        new Object[]{"LG CAN", 8L},
        new Object[]{"SM PACK", 8L},
        new Object[]{"MED PKG", 8L},
        new Object[]{"LG CASE", 8L},
        new Object[]{"JUMBO PKG", 8L},
        new Object[]{"WRAP JAR", 8L},
        new Object[]{"SM BAG", 8L},
        new Object[]{"MED BOX", 8L},
        new Object[]{"MED DRUM", 8L},
        new Object[]{"SM CAN", 8L},
        new Object[]{"MED CAN", 8L},
        new Object[]{"JUMBO BAG", 8L},
        new Object[]{"SM PKG", 8L},
        new Object[]{"LG CASE", 8L},
        new Object[]{"MED PKG", 8L},
        new Object[]{"JUMBO PKG", 8L},
        new Object[]{"LG CAN", 8L},
        new Object[]{"MED PACK", 8L},
        new Object[]{"WRAP BAG", 8L},
        new Object[]{"LG CASE", 8L},
        new Object[]{"LG PKG", 8L},
        new Object[]{"JUMBO DRUM", 8L},
        new Object[]{"LG PKG", 8L}
    );
  }

  @Test
  public void test3993() throws Exception
  {
    final Object[][] expected = {
        new Object[]{"10", "APP", "IT", "티월드다이렉트(tworlddirect.com)", 1L},
        new Object[]{"10", "APP", "생활", "도미노피자(Dominopizza)", 1L}
    };
    String query = "SELECT age_group, bks_event_d0, bks_event_d1, bks_event_d2, count(*) as cnt FROM %s "
                 + "WHERE svc_mgmt_num = '10000497' AND bks_event_d0 = 'APP' GROUP BY 1, 2, 3, 4";
    testQuery(String.format(query, "cdis"), GROUP_CONTEXT, expected);
    testQuery(String.format(query, "cdis_i"), GROUP_CONTEXT, expected);

    query = "SELECT age_group, bks_event_d0, bks_event_d1, bks_event_d2, count(*) as cnt FROM %s "
                 + "WHERE svc_mgmt_num = '10000497' AND bks_event_d0 <= 'APP' GROUP BY 1, 2, 3, 4";
    testQuery(String.format(query, "cdis"), GROUP_CONTEXT, expected);
    testQuery(String.format(query, "cdis_i"), GROUP_CONTEXT, expected);
  }

  static {
    Parser.register(ModuleBuiltinFunctions.class);
    ModuleBuiltinFunctions.jsonMapper = TestHelper.JSON_MAPPER;
  }

  @Test
  public void test3994() throws Exception
  {
    testQuery(
        "SELECT bks_event_d0, count(*) as cnt FROM cdis GROUP BY bks_event_d0",
        new Object[]{"APP", 4L},
        new Object[]{"T114", 4L}
    );
    testQuery(
        "SELECT dedup(bks_event_d0), count(*) as cnt FROM cdis GROUP BY dedup(bks_event_d0)",
        new Object[]{"APP", 2L},
        new Object[]{"T114", 2L}
    );
  }

  @Test
  public void test3995() throws Exception
  {
    String query = "SELECT age_group, bks_event_d2, count(*) as cnt FROM %s GROUP BY age_group, bks_event_d2";
    Object[][] expected = {
        new Object[]{"", "도미노피자(Dominopizza)", 1L},
        new Object[]{"", "신용카드사", 1L},
        new Object[]{"", "치킨", 1L},
        new Object[]{"", "티월드다이렉트(tworlddirect.com)", 1L},
        new Object[]{"10", "도미노피자(Dominopizza)", 1L},
        new Object[]{"10", "신용카드사", 1L},
        new Object[]{"10", "치킨", 1L},
        new Object[]{"10", "티월드다이렉트(tworlddirect.com)", 1L}
    };
    testQuery(String.format(query, "cdis"), GROUP_CONTEXT, expected);
    testQuery(String.format(query, "cdis_i"), GROUP_CONTEXT, expected);
  }

  @Test
  public void test3998() throws Exception
  {
    String query = "SELECT bks_event_d0, bks_event_d1, bks_event_d2,"
                   + "count(*) FILTER (WHERE bks_event_d0 = 'APP'),"
                   + "count(*) FILTER (WHERE bks_event_d0 = 'T114'),"
                   + "count(*) FILTER (WHERE bks_event_d1 = 'IT'),"
                   + "count(*) FILTER (WHERE bks_event_d2 = '치킨')"
                   + " FROM %s GROUP BY 1, 2, 3";
    Object[][] expected1 = {
        new Object[]{"APP", "IT", "티월드다이렉트(tworlddirect.com)", 2L, 0L, 2L, 0L},
        new Object[]{"APP", "생활", "도미노피자(Dominopizza)", 2L, 0L, 0L, 0L},
        new Object[]{"T114", "금융", "신용카드사", 0L, 2L, 0L, 0L},
        new Object[]{"T114", "음식", "치킨", 0L, 2L, 0L, 2L}
    };
    testQuery(String.format(query, "cdis"), GROUP_CONTEXT, expected1);
    testQuery(String.format(query, "cdis_i"), GROUP_CONTEXT, expected1);

    Object[][] expected2 = {
        new Object[]{"APP", "IT", "티월드다이렉트(tworlddirect.com)", 1L, 0L, 1L, 0L},
        new Object[]{"APP", "생활", "도미노피자(Dominopizza)", 1L, 0L, 0L, 0L},
        new Object[]{"T114", "금융", "신용카드사", 0L, 1L, 0L, 0L},
        new Object[]{"T114", "음식", "치킨", 0L, 1L, 0L, 1L}
    };
    testQuery(String.format(query, "cdis WHERE svc_mgmt_num = '10000497'"), GROUP_CONTEXT, expected2);
    testQuery(String.format(query, "cdis_i WHERE svc_mgmt_num = '10000497'"), GROUP_CONTEXT, expected2);
  }

  @Test
  public void test4015() throws Exception
  {
    testQuery(
        "WITH X AS ("
        + " SELECT * FROM "
        + " (VALUES"
        + "   (0, 'A', 1, null, null),"
        + "   (0, 'B', null, 2, null),"
        + "   (0, 'C', null, null, 3),"
        + "   (1, 'A', 4, null, null),"
        + "   (1, 'B', null, 5, null),"
        + "   (1, 'C', null, null, 6)"
        + " ) AS T (__time, asset, p1, p2, p3)"
        + ")"
        + " SELECT prevNotNull(p1) over (), nextNotNull(p1) over (),"
        + "        prevNotNull(p2) over (), nextNotNull(p2) over (),"
        + "        prevNotNull(p3) over (), nextNotNull(p3) over () FROM X",
        new Object[]{null, 4L, null, 2L, null, 3L},
        new Object[]{1L, 4L, null, 5L, null, 3L},
        new Object[]{1L, 4L, 2L, 5L, null, 6L},
        new Object[]{1L, null, 2L, 5L, 3L, 6L},
        new Object[]{4L, null, 2L, null, 3L, 6L},
        new Object[]{4L, null, 5L, null, 3L, null}
    );
  }

  @Test
  public void test4157() throws Exception
  {
    String query = "SELECT age_group, _d0 FROM %s, LATERAL TABLE(explode(bks_event_d0)) AS t(_d0)";
    Object[][] expected = {
        {"10", "T114"}, {"10", "APP"}, {"10", "T114"}, {"10", "APP"},
        {"", "T114"}, {"", "APP"}, {"", "T114"}, {"", "APP"}
    };
    testQuery(
        String.format(query, "cdis"),
        "DruidCorrelateRel(correlation=[$cor0], scanProject=[$1, $6])\n"
        + "  DruidQueryRel(table=[druid.cdis])\n"
        + "  DruidTableFunctionScanRel(invocation=[EXPLODE($cor0.bks_event_d0)])\n",
        expected
    );
    testQuery(String.format(query, "cdis_i"), expected);

    query = "SELECT age_group, _d0, _d1 FROM %s, LATERAL TABLE(explode(bks_event_d0, bks_event_d1)) AS t(_d0, _d1)";
    expected = new Object[][] {
        {"10", "T114", "금융"}, {"10", "APP", "IT"}, {"10", "T114", "음식"}, {"10", "APP", "생활"},
        {"", "T114", "금융"}, {"", "APP", "IT"}, {"", "T114", "음식"}, {"", "APP", "생활"}
    };
    testQuery(
        String.format(query, "cdis"),
        "DruidCorrelateRel(correlation=[$cor0], scanProject=[$1, $6, $7])\n"
        + "  DruidQueryRel(table=[druid.cdis])\n"
        + "  DruidTableFunctionScanRel(invocation=[EXPLODE($cor0.bks_event_d0, $cor0.bks_event_d1)])\n",
        expected
    );
    testQuery(String.format(query, "cdis_i"), expected);

    query = "SELECT age_group, _d0, _d1 FROM %s, LATERAL TABLE(explode(bks_event_d0, bks_event_d1)) AS t(_d0, _d1) WHERE age_group = 10";
    expected = new Object[][] {
        {"10", "T114", "금융"}, {"10", "APP", "IT"}, {"10", "T114", "음식"}, {"10", "APP", "생활"}
    };
    testQuery(
        String.format(query, "cdis"),
        "DruidCorrelateRel(correlation=[$cor0], scanProject=[$1, $6, $7])\n"
        + "  DruidQueryRel(table=[druid.cdis], scanFilter=[=($1, 10)])\n"
        + "  DruidTableFunctionScanRel(invocation=[EXPLODE($cor0.bks_event_d0, $cor0.bks_event_d1)])\n",
        expected
    );
    testQuery(String.format(query, "cdis_i"), expected);

    query = "SELECT age_group, _d0 FROM %s, LATERAL TABLE(explode(bks_event_d0, bks_event_d1)) AS t(_d0, _d1) WHERE _d0 = 'T114'";
    expected = new Object[][] {
        {"10", "T114"}, {"10", "T114"}, {"", "T114"}, {"", "T114"}
    };
    testQuery(
        String.format(query, "cdis"),
        "DruidCorrelateRel(correlation=[$cor0], scanProject=[$1, $6])\n"
        + "  DruidQueryRel(table=[druid.cdis])\n"
        + "  DruidTableFunctionScanRel(invocation=[EXPLODE($cor0.bks_event_d0, $cor0.bks_event_d1)], scanFilter=[=($0, 'T114')])\n",
        expected
    );
    testQuery(String.format(query, "cdis_i"), expected);

    query = "SELECT age_group, _d0, _d1, _d2 FROM %s, LATERAL TABLE(explode(bks_event_d0, bks_event_d1, bks_event_d2)) AS t(_d0, _d1, _d2) WHERE age_group = 10 AND _d0 = 'T114'";
    expected = new Object[][] {
        {"10", "T114", "금융", "신용카드사"}, {"10", "T114", "음식", "치킨"}
    };
    testQuery(
        String.format(query, "cdis"),
        "DruidCorrelateRel(correlation=[$cor0], scanProject=[$1, $6, $7, $8])\n"
        + "  DruidQueryRel(table=[druid.cdis], scanFilter=[=($1, 10)])\n"
        + "  DruidTableFunctionScanRel(invocation=[EXPLODE($cor0.bks_event_d0, $cor0.bks_event_d1, $cor0.bks_event_d2)], scanFilter=[=($0, 'T114')])\n",
        expected
    );
    testQuery(String.format(query, "cdis_i"), expected);
  }
}
