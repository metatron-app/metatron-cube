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

import io.druid.data.Pair;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static io.druid.segment.TestHelper.list;

public class StructTest extends CalciteQueryTestHelper
{
  private static final CalciteQueryTestHelper.MiscQueryHook hook = new CalciteQueryTestHelper.MiscQueryHook();
  private static final TestQuerySegmentWalker walker = TestHelper.newWalker().withQueryHook(hook);

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker.addUps();
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Override
  protected <T extends Throwable> Pair<String, List<Object[]>> failed(T ex) throws T
  {
    hook.printHooked();
    throw ex;
  }

  @Test
  public void tesBasic() throws Exception
  {
    testQuery(
        "SELECT user_cid,\"adot_usage.life_cycle\" from ups where \"adot_usage.quest.cone.received\" <= 100",
        new Object[][] {
            {"ilSFLwxxxxx+I8oxOPsf9l9xxxxxx==", "휴면"},
            {"ilSFLwyyyy+I8oxOPsf9l9yyyyx==", "안휴면"}
        }
    );
    hook.verifyHooked(
        "hYxEQ2AScutFVnTRb0XW2g==",
        "StreamQuery{dataSource='ups', filter=BoundDimFilter{adot_usage.quest.cone.received <= 100(numeric)}, columns=[user_cid, adot_usage.life_cycle]}"
    );
    testQuery(
        "SELECT \"adot_usage.quest.cone\" from ups where \"adot_usage.stickness.day_7\" > 0",
        new Object[]{list(10, null, 4)}
    );
    hook.verifyHooked(
        "JI1o91Y3WtAVrqqb6zDT5w==",
        "StreamQuery{dataSource='ups', filter=BoundDimFilter{0 < adot_usage.stickness.day_7(numeric)}, columns=[v0], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"adot_usage.quest.cone.received\",\"adot_usage.quest.cone.retention\",\"adot_usage.quest.cone.received_percentile\")', outputName='v0'}]}"
    );
  }
}
