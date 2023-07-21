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
import org.junit.Before;
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

  @Before
  public void before()
  {
    hook.clear();
  }

  @Test
  public void testBasic() throws Exception
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

  @Test
  public void testDimensions() throws Exception
  {
    testQuery(
        "SELECT \"ci_profile.life_style\", \"adot_usage.life_cycle\" from ups where \"ci_profile.life_style\" = '영화관'",
        new Object[][] {
            {"[캠핑, 영화관, 청년1인가구]", "휴면"},
            {"[영화관, 청년2인가구]", "안휴면"}
        }
    );
    hook.verifyHooked(
        "Zqr9LlJ1p4Gh8gsXLleC6Q==",
        "StreamQuery{dataSource='ups', filter=ci_profile.life_style=='영화관', columns=[ci_profile.life_style, adot_usage.life_cycle]}"
    );
    testQuery(
        "SELECT \"ci_profile.life_style\", \"adot_usage.life_cycle\" from ups where \"ci_profile.life_style\" = '캠핑'",
        new Object[][] {
            {"[캠핑, 영화관, 청년1인가구]", "휴면"}
        }
    );
    hook.verifyHooked(
        "AKo3jcUAHWRdMefjvxXyew==",
        "StreamQuery{dataSource='ups', filter=ci_profile.life_style=='캠핑', columns=[ci_profile.life_style, adot_usage.life_cycle]}"
    );

    // map
    testQuery(
        "SELECT \"ci_profile.xdr_category\", \"adot_usage.life_cycle\" from ups where \"ci_profile.xdr_category.__key\" = 'bf_m1_app_dt_ratio_cat_03'",
        new Object[][] {
            {"{bf_m1_app_dt_ratio_cat_01=0.87, bf_m1_app_dt_ratio_cat_03=0.83, bf_m1_app_dt_ratio_cat_04=0.8, bf_m1_app_dt_ratio_cat_06=0.17, bf_m1_app_dt_ratio_cat_08=0.83}", "안휴면"}
        }
    );
    hook.verifyHooked(
        "Dby7odomifRVJGT0zz6g/Q==",
        "StreamQuery{dataSource='ups', filter=ci_profile.xdr_category.__key=='bf_m1_app_dt_ratio_cat_03', columns=[ci_profile.xdr_category, adot_usage.life_cycle]}"
    );

    // array
    testQuery(
        "SELECT onboarding.interest[0],onboarding.interest[0].name,onboarding.artist[0].name from ups where onboarding.artist[0]._id='80049126'",
        new Object[][] {
            {list("KEYWORD_001", "동네 탐방"), "동네 탐방", "아이유 (IU)"}
        }
    );
    hook.verifyHooked(
        "X0FO1AZXjpTTrqNa2rZtfg==",
        "StreamQuery{dataSource='ups', filter=MathExprFilter{expression='(\"onboarding.artist\".\"0\".\"_id\" == '80049126')'}, columns=[v0, v1, v2], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"onboarding.interest\".\"0\".\"_id\",\"onboarding.interest\".\"0\".\"name\")', outputName='v0'}, ExprVirtualColumn{expression='\"onboarding.interest\".\"0\".\"name\"', outputName='v1'}, ExprVirtualColumn{expression='\"onboarding.artist\".\"0\".\"name\"', outputName='v2'}]}"
    );
  }

  @Test
  public void testBooleans() throws Exception
  {
    testQuery(
        "SELECT \"ci_profile.base\",\"ci_profile.family.child_y\",\"ci_profile.family.adult_child_y\",\"ci_profile.family.married\" from ups where \"adot_usage.stickness.day_7\" < 1000",
        new Object[][]{
            {false, true, null, false},
            {true, true, null, null}
        }
    );
    hook.verifyHooked(
        "2ob5tJUEWj+xyzoc8dZ4Kw==",
        "StreamQuery{dataSource='ups', filter=BoundDimFilter{adot_usage.stickness.day_7 < 1000(numeric)}, columns=[ci_profile.base, ci_profile.family.child_y, ci_profile.family.adult_child_y, ci_profile.family.married]}"
    );
  }

  @Test
  public void testGroupBy() throws Exception
  {
    testQuery(
        "SELECT \"ci_profile.life_style\", sum(\"adot_usage.quest.cone.received\") from ups group by 1",
        new Object[][] {
            {"영화관", 110L},
            {"청년1인가구", 100L},
            {"청년2인가구", 10L},
            {"캠핑", 100L}
        }
    );
    hook.verifyHooked(
        "8rbDO7SXyFLkLk5C6K3HDg==",
        "GroupByQuery{dataSource='ups', dimensions=[DefaultDimensionSpec{dimension='ci_profile.life_style', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='adot_usage.quest.cone.received', inputType='long'}], outputColumns=[d0, a0]}"
    );

    // map
    testQuery(
        "SELECT \"ci_profile.xdr_category.__key\", sum(\"ci_profile.xdr_category.__value\") from ups group by 1",
        new Object[][] {
            {"bf_m1_app_dt_ratio_cat_01", 1.7400000095367432D},
            {"bf_m1_app_dt_ratio_cat_03", 0.8299999833106995D},
            {"bf_m1_app_dt_ratio_cat_04", 1.6299999952316284D},
            {"bf_m1_app_dt_ratio_cat_05", 0.800000011920929D},
            {"bf_m1_app_dt_ratio_cat_06", 0.3400000035762787D},
            {"bf_m1_app_dt_ratio_cat_08", 1.659999966621399D}
        }
    );
    hook.verifyHooked(
        "oOFwos475/lciYgcC50N0g==",
        "GroupByQuery{dataSource='ups', dimensions=[DefaultDimensionSpec{dimension='ci_profile.xdr_category.__key', outputName='d0'}], aggregatorSpecs=[GenericSumAggregatorFactory{name='a0', fieldName='ci_profile.xdr_category.__value', inputType='double'}], outputColumns=[d0, a0]}"
    );
  }
}
