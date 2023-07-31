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

public class AdotUsageTest extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static final TestQuerySegmentWalker walker = TestHelper.newWalker().withQueryHook(hook);

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker.addAdotUsage();
  }

  private final Object params = "adot_usage";

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
        String.format("SELECT entity.entity_preferences[0].apollo_radio[0] from %s", params),
        new Object[]{
            "[CONTENT_CHANNEL, " +
              "[[SBS.POWERFM, 1, 0.6179800700278069, n], [MBC.STFM, 2, 0.5830243023122413, n], " +
               "[KBS.1RADIO, 3, 0.5569049368444767, n], [KBS.COOLFM, 5, 0.528576470814762, n], " +
               "[CBS.MUSICFM, 4, 0.5342941428574567, n]]" +
            "]"
        }
    );
    hook.verifyHooked(
        "se1ofBQ1AHIzhHt6ZJcRAQ==",
        "StreamQuery{dataSource='adot_usage', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"entity.entity_preferences.0.apollo_radio.0.entity_role\",\"entity.entity_preferences.0.apollo_radio.0.preferences\")', outputName='v0'}]}"
    );

    testQuery(
        String.format("SELECT entity.entity_preferences[0].apollo_radio[0].preferences[1] from %s", params),
        new Object[]{"[MBC.STFM, 2, 0.5830243023122413, n]"}
    );
    hook.verifyHooked(
        "GtsSM5v47dirDXVrBcFTrA==",
        "StreamQuery{dataSource='adot_usage', columns=[v0], virtualColumns=[ExprVirtualColumn{expression='ARRAY(\"entity.entity_preferences.0.apollo_radio.0.preferences.1.name\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.ranking\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.score\",\"entity.entity_preferences.0.apollo_radio.0.preferences.1.usage_yn\")', outputName='v0'}]}"
    );

    testQuery(
        String.format("SELECT music.pref_track_list[4].rp_artist_list[0].artist_name from %s", params),
        new Object[]{"어노인팅"}
    );
    hook.verifyHooked(
        "7n3wtbhl4G6Pm5QvivHRVg==",
        "StreamQuery{dataSource='adot_usage', columns=[music.pref_track_list.4.rp_artist_list.0.artist_name]}"
    );
  }
}
