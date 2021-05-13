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

import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleTest extends CalciteQueryTestHelper
{
  private static MiscQueryHook hook = new MiscQueryHook();
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker = TestHelper.newWalker().withQueryHook(hook);
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
        new Object[]{"R&D", "Manager", 1L},
        new Object[]{"R&D", "Developer", 2L},
        new Object[]{"Sales", "Manager", 1L},
        new Object[]{"Sales", "Developer", 2L}
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
}
