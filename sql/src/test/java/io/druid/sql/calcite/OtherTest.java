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

import java.util.Arrays;
import java.util.BitSet;

public class OtherTest extends CalciteQueryTestHelper
{
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker = TestHelper.profileWalker.duplicate();
    walker.populate("profile");
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testSelect() throws Exception
  {
    testQuery(
        "SELECT st11_cat FROM profile",
        new Object[]{newBitSet(3, 7, 18)},
        new Object[]{null},
        new Object[]{newBitSet(3, 9, 14)},
        new Object[]{newBitSet(6, 14, 15, 16, 17)},
        new Object[]{newBitSet(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT st11_cat FROM profile where st11_cat IS NOT NULL",
        new Object[]{newBitSet(3, 7, 18)},
        new Object[]{newBitSet(3, 9, 14)},
        new Object[]{newBitSet(6, 14, 15, 16, 17)},
        new Object[]{newBitSet(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT st11_cat[3], st11_cat[18] FROM profile",
        new Object[]{true, true},
        new Object[]{null, null},
        new Object[]{true, false},
        new Object[]{false, false},
        new Object[]{false, true}
    );
    testQuery(
        "SELECT svc_mgmt_num, age FROM profile where st11_cat[3]",
        new Object[]{"s:1", "23"},
        new Object[]{"s:3", "44"}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{0L, null},
        new Object[]{3L, Arrays.asList(3, 9, 14)},
        new Object[]{5L, Arrays.asList(6, 14, 15, 16, 17)},
        new Object[]{4L, Arrays.asList(1, 4, 14, 18)}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile WHERE _bs(st11_cat) = '3'",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{3L, Arrays.asList(3, 9, 14)}
    );
    testQuery(
        "SELECT cardinality(st11_cat), unwrap(st11_cat) FROM profile WHERE _bs(st11_cat) IN ('3', '6', '9')",
        new Object[]{3L, Arrays.asList(3, 7, 18)},
        new Object[]{3L, Arrays.asList(3, 9, 14)},
        new Object[]{5L, Arrays.asList(6, 14, 15, 16, 17)}
    );
  }

  @Test
  public void testGroupBy() throws Exception
  {
    testQuery(
        "SELECT _bs(st11_cat), count(*) as c FROM profile group by _bs(st11_cat) order by c desc limit 5",
        new Object[]{"14", 3L},
        new Object[]{"18", 2L},
        new Object[]{"3", 2L},
        new Object[]{"15", 1L},
        new Object[]{"9", 1L}
    );
  }

  private static BitSet newBitSet(int... ixs)
  {
    final BitSet set = new BitSet();
    for (int ix : ixs) {
      set.set(ix);
    }
    return set;
  }
}
