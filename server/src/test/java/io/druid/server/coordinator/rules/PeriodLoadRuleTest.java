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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class PeriodLoadRuleTest
{
  private final static DataSegment.Builder builder = DataSegment.builder()
                                                                .dataSource("test")
                                                                .version(new DateTime().toString());

  @Test
  public void testAppliesToAll()
  {
    DateTime now = new DateTime("2013-01-01");
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P5000Y"),
        null,
        ImmutableMap.<String, Integer>of("", 0)
    );

    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("2012-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("1000-01-01/2012-12-31")).build(), now));
    Assert.assertTrue(rule.appliesTo(builder.interval(new Interval("0500-01-01/2100-12-31")).build(), now));
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodLoadRule rule = new PeriodLoadRule(new Period("P1M"), false, null);

    Assert.assertTrue(rule.appliesTo(new Interval(now.minusWeeks(1), now), now));
    Assert.assertTrue(rule.appliesTo(new Interval(now.minusDays(1), now.plusDays(1)), now));
    Assert.assertFalse(rule.appliesTo(new Interval(now.plusDays(1), now.plusDays(2)), now));

    rule = new PeriodLoadRule(new Period("P1M"), null, null);
    Assert.assertTrue(rule.appliesTo(new Interval(now.plusDays(1), now.plusDays(2)), now));
  }

  @Test
  public void testSerdeNullTieredReplicants() throws Exception
  {
    PeriodLoadRule rule = new PeriodLoadRule(
        new Period("P1D"), null, null
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assert.assertEquals(rule.getPeriod(), ((PeriodLoadRule)reread).getPeriod());
    Assert.assertEquals(rule.getTieredReplicants(), ((PeriodLoadRule)reread).getTieredReplicants());
    Assert.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), rule.getTieredReplicants());
  }

  @Test
  public void testMappingNullTieredReplicants() throws Exception{
    String inputJson = "{\n"
                       + "      \"period\": \"P1D\",\n"
                       + "      \"type\": \"loadByPeriod\"\n"
                       + "    }";
    String expectedJson = "{\n"
                          + "      \"period\": \"P1D\",\n"
                          + "      \"tieredReplicants\": {\n"
                          + "        \""+ DruidServer.DEFAULT_TIER +"\": "+ DruidServer.DEFAULT_NUM_REPLICANTS +"\n"
                          + "      },\n"
                          + "      \"type\": \"loadByPeriod\"\n"
                          + "    }";
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    PeriodLoadRule inputPeriodLoadRule = jsonMapper.readValue(inputJson, PeriodLoadRule.class);
    PeriodLoadRule expectedPeriodLoadRule = jsonMapper.readValue(expectedJson, PeriodLoadRule.class);
    Assert.assertEquals(expectedPeriodLoadRule.getTieredReplicants(), inputPeriodLoadRule.getTieredReplicants());
    Assert.assertEquals(expectedPeriodLoadRule.getPeriod(), inputPeriodLoadRule.getPeriod());
  }

  @Test
  public void testAppliesTo()
  {
    // 2019-11-01T05:05:58 ~ 2019-11-01T07:05:58
    DateTime now = new DateTime("2019-11-01T07:05:58");

    PeriodLoadRule rule = new PeriodLoadRule(new Period("PT2H"), false, null);
    validate(now, rule, new boolean[]{false, true, true, true, false});

    rule = new PeriodLoadRule(new Period("PT2H"), true, null);
    validate(now, rule, new boolean[]{false, true, true, true, true});

    rule = new PeriodLoadRule(new Period("PT2H"), null, null);
    validate(now, rule, new boolean[]{false, true, true, true, true});
  }

  private void validate(DateTime now, PeriodLoadRule rule, boolean[] results)
  {
    int i = 0;
    for (String interval : Arrays.asList(
        "2019-11-01T04/2019-11-01T05",
        "2019-11-01T05/2019-11-01T06",
        "2019-11-01T06/2019-11-01T07",
        "2019-11-01T07/2019-11-01T08",
        "2019-11-01T08/2019-11-01T09"
    )) {
      Assert.assertEquals(interval, results[i++], rule.appliesTo(new Interval(interval), now));
    }
  }
}
