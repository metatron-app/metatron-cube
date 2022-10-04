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

package io.druid.sql.avatica;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.DateTimes;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryConfig;
import io.druid.server.QueryManager;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DruidStatementTest extends CalciteTestBase
{
  private final Logger LOG = new Logger(DruidStatementTest.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private TestQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  private final DruidConnection connection = new DruidConnection("test", -1, Maps.newHashMap(), null);

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final QueryConfig queryConfig = new QueryConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(
        walker,
        plannerConfig
    );
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(walker);
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
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
    this.sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory);
  }

  @After
  public void tearDown() throws Exception
  {
    walker = null;
  }

  @Test
  public void testSignature() throws Exception
  {
    final String sql = "SELECT * FROM druid.foo";
    final DruidStatement statement = new DruidStatement("", connection.createStatementHandle(), sqlLifecycleFactory.factorize(sql)).prepare();


    // Check signature.
    final Meta.Signature signature = statement.getSignature();
    Assert.assertEquals(Meta.CursorFactory.ARRAY, signature.cursorFactory);
    Assert.assertEquals(Meta.StatementType.SELECT, signature.statementType);
    Assert.assertEquals(sql, signature.sql);
    Assert.assertEquals(
        Lists.newArrayList(
            Lists.newArrayList("__time", "TIMESTAMP", "java.lang.Long"),
            Lists.newArrayList("cnt", "BIGINT", "java.lang.Long"),
            Lists.newArrayList("dim1", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim2", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("m1", "DOUBLE", "java.lang.Double"),
            Lists.newArrayList("m2", "DOUBLE", "java.lang.Double"),
            Lists.newArrayList("unique_dim1", "OTHER", "java.lang.Object")
        ),
        Lists.transform(
            signature.columns,
            new Function<ColumnMetaData, List<String>>()
            {
              @Override
              public List<String> apply(final ColumnMetaData columnMetaData)
              {
                return Lists.newArrayList(
                    columnMetaData.label,
                    columnMetaData.type.name,
                    columnMetaData.type.rep.clazz.getName()
                );
              }
            }
        )
    );
  }

  @Test
  public void testSelectAllInFirstFrame() throws Exception
  {
    final String sql = "SELECT __time, cnt, dim1, dim2, m1 FROM druid.foo";
    final DruidStatement statement = new DruidStatement(
        "", connection.createStatementHandle(), sqlLifecycleFactory.factorize(sql)).prepare();

    // First frame, ask for all rows.
    Meta.Frame frame = statement.execute(Collections.emptyList()).nextFrame(DruidStatement.START_OFFSET, 6);
    Assert.assertEquals(
        Meta.Frame.create(
            0,
            true,
            Lists.<Object>newArrayList(
                new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0d},
                new Object[]{DateTimes.of("2000-01-02").getMillis(), 1L, "10.1", "", 2.0d},
                new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0d},
                new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0d},
                new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0d},
                new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", "", 6.0d}
            )
        ),
        frame
    );
    Assert.assertTrue(statement.isDone());
  }

  @Test
  public void testSelectSplitOverTwoFrames() throws Exception
  {
    final String sql = "SELECT __time, cnt, dim1, dim2, m1 FROM druid.foo";
    final DruidStatement statement = new DruidStatement("", connection.createStatementHandle(), sqlLifecycleFactory.factorize(sql)).prepare();
    
    // First frame, ask for 2 rows.
    Meta.Frame frame = statement.execute(Collections.emptyList()).nextFrame(DruidStatement.START_OFFSET, 2);
    Assert.assertEquals(
        Meta.Frame.create(
            0,
            false,
            Lists.<Object>newArrayList(
                new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0d},
                new Object[]{DateTimes.of("2000-01-02").getMillis(), 1L, "10.1", "", 2.0d}
            )
        ),
        frame
    );
    Assert.assertFalse(statement.isDone());

    // Last frame, ask for all remaining rows.
    frame = statement.nextFrame(2, 10);
    Assert.assertEquals(
        Meta.Frame.create(
            2,
            true,
            Lists.<Object>newArrayList(
                new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0d},
                new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0d},
                new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0d},
                new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", "", 6.0d}
            )
        ),
        frame
    );
    Assert.assertTrue(statement.isDone());
  }

  @Ignore
  @Test
  public void testJDBC()
  {
    // Connect to /druid/v2/sql/avatica/ on your Broker.
    String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

    Properties connectionProperties = new Properties();

    //String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'druid-mt-metric'";
    String query = "SELECT  *\n"
                   + "FROM \"druid-metric\"\n"
                   + "WHERE \n"
                   + "\"__time\" BETWEEN TIMESTAMP '2020-01-06 01:00:00' AND TIMESTAMP '2020-01-24 01:00:00' \n"
                   + "ORDER BY \"__time\" DESC\n"
                   + "LIMIT 100\n";
    try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
      try (
          final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery(query)
      ) {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int count = metaData.getColumnCount();
        StringBuilder columns = new StringBuilder();
        for (int i = 1; i <= count; i++) {
          columns.append(metaData.getColumnName(i));
          if (i > 1) {
            columns.append(",");
          }
        }
        System.out.println(columns.toString());

        StringBuilder b = new StringBuilder();
        int x = 0;
        while (resultSet.next() && x++ < 100) {
          b.setLength(0);
          for (int i = 1; i <= count; i++) {
            if (i > 1) {
              b.append(',');
            }
            b.append(resultSet.getObject(i));
          }

          System.out.println(b.toString());
        }
        resultSet.close();
      }
    } catch (Exception e) {
      LOG.error("SQL failed.", e);
    }
  }
}
