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

package io.druid.query;

import io.druid.segment.Lucene9TestHelper;
import io.druid.sql.calcite.CalciteQueryTestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class KnnFilterQueryTest extends CalciteQueryTestHelper
{
  static final MiscQueryHook hook = new MiscQueryHook();
  static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker = Lucene9TestHelper.segmentWalker.duplicate().withQueryHook(hook);
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testSelect() throws Exception
  {
    Object[][] expected = new Object[][]{
        {"11272", Arrays.asList(0.5235608D, 0.44156343D)},
        {"11273", Arrays.asList(0.5235496D, 0.441608D)},
        {"11275", Arrays.asList(0.5236085D, 0.4415981D)},
        {"11276", Arrays.asList(0.5233764D, 0.44162035D)},
        {"11277", Arrays.asList(0.52329844D, 0.441492D)},
        {"11278", Arrays.asList(0.52339643D, 0.4415921D)},
        {"11281", Arrays.asList(0.5232792D, 0.4414288D)},
        {"11282", Arrays.asList(0.52322507D, 0.44143406D)},
        {"11283", Arrays.asList(0.5233347D, 0.4416579D)},
        {"11284", Arrays.asList(0.5232767D, 0.44144332D)}
    };
    testQuery(
        "SELECT NUMBER, vector FROM chameleon WHERE lucene_knn_vector('vector', array_float(0.5, 0.5), 10)",
        expected
    );

    hook.verifyHooked(
        "AUfmsJrIs1Wod8227jA6vg==",
        "StreamQuery{dataSource='chameleon', filter=KnnVectorFilter{field='vector', vector=[0.5, 0.5], count=10}, columns=[NUMBER, vector]}"
    );

    expected = new Object[][]{
        {"10741", Arrays.asList(0.5281539D, 0.41882014D)},
        {"11161", Arrays.asList(0.5633602D, 0.4436847D)},
        {"11281", Arrays.asList(0.5232792D, 0.4414288D)},
        {"13321", Arrays.asList(0.43399575D, 0.5105342D)},
        {"1341", Arrays.asList(0.533308D, 0.44191334D)},
        {"1351", Arrays.asList(0.5328579D, 0.44170472D)},
        {"1661", Arrays.asList(0.52456915D, 0.4240269D)},
        {"17631", Arrays.asList(0.5021096D, 0.43554822D)},
        {"17641", Arrays.asList(0.50252676D, 0.4358472D)},
        {"19401", Arrays.asList(0.43876216D, 0.46694124D)}
    };
    testQuery(
        "SELECT NUMBER, vector FROM chameleon WHERE NUMBER LIKE '%1' AND lucene_knn_vector(vector, array_float(0.5, 0.5), 10)",
        expected
    );
    hook.verifyHooked(
        "ReAjxpBgTCnhaKAUlPOZow==",
        "StreamQuery{dataSource='chameleon', filter=(NUMBER LIKE '%1' && KnnVectorFilter{field='vector', vector=[0.5, 0.5], count=10}), columns=[NUMBER, vector]}"
    );
  }
}