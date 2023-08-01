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

import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class AdotUsageTest extends CalciteQueryTestHelper
{
  private static final TableDataSource ADOT_U = TableDataSource.of("adot_usage");
  private static final TableDataSource ADOT_U_I = TableDataSource.of("adot_usage_i");

  private static final CalciteQueryTestHelper.MiscQueryHook hook = new CalciteQueryTestHelper.MiscQueryHook()
  {
    @Override
    public void accept(Query<?> query)
    {
      super.accept(Queries.iterate(query, q -> ADOT_U_I.equals(q.getDataSource()) ? q.withDataSource(ADOT_U) : q));
    }
  };

  private static final TestQuerySegmentWalker walker = TestHelper.newWalker().addAdotUsage().withQueryHook(hook);

  @Parameterized.Parameters(name = "ds:{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Arrays.asList(new Object[]{ADOT_U.getName()}, new Object[]{ADOT_U_I.getName()});
  }

  private final Object[] params;

  public AdotUsageTest(String ds)
  {
    this.params = new Object[]{ds};
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testBasic() throws Exception
  {
    Object[][] expected = {{
        "[CONTENT_CHANNEL, " +
        "[[SBS.POWERFM, 1, 0.6179800700278069, n], [MBC.STFM, 2, 0.5830243023122413, n], " +
        "[KBS.1RADIO, 3, 0.5569049368444767, n], [KBS.COOLFM, 5, 0.528576470814762, n], " +
        "[CBS.MUSICFM, 4, 0.5342941428574567, n]]" +
        "]"
    }};
    testQueries(
        "SELECT entity.entity_preferences[0].apollo_radio[0] from %s", params, expected,
        "se1ofBQ1AHIzhHt6ZJcRAQ==",
        "StreamQuery{dataSource='adot_usage', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"entity.entity_preferences.0.apollo_radio.0.entity_role\",\"entity.entity_preferences.0.apollo_radio.0.preferences\")', outputName='v0'}]}"
    );

    expected = new Object[][]{{"[MBC.STFM, 2, 0.5830243023122413, n]"}};
    testQueries(
        "SELECT entity.entity_preferences[0].apollo_radio[0].preferences[1] from %s", params, expected,
        "GtsSM5v47dirDXVrBcFTrA==",
        "StreamQuery{dataSource='adot_usage', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"entity.entity_preferences.0.apollo_radio.0.preferences.1.name\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.ranking\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.score\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.usage_yn\")', outputName='v0'}]}"
    );

    expected = new Object[][]{{"어노인팅"}};
    testQueries(
        "SELECT music.pref_track_list[4].rp_artist_list[0].artist_name from %s", params, expected,
        "7n3wtbhl4G6Pm5QvivHRVg==",
        "StreamQuery{dataSource='adot_usage', columns=[music.pref_track_list.4.rp_artist_list.0.artist_name]}"
    );
  }
}
