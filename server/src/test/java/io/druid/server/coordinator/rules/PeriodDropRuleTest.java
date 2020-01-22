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

import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class PeriodDropRuleTest
{
  private final static DataSegment.Builder builder = DataSegment.builder()
                                                                .dataSource("test")
                                                                .version(new DateTime("2012-12-31T01:00:00").toString());

  @Test
  public void testAppliesToAll()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(
        new Period("P5000Y"), null
    );

    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(
                new Interval(
                    now.minusDays(2),
                    now.minusDays(1)
                )
            ).build(),
            now
        )
    );
    Assert.assertTrue(
        rule.appliesTo(
            builder.interval(new Interval(now.minusYears(100), now.minusDays(1)))
                       .build(),
            now
        )
    );
  }

  @Test
  public void testAppliesToPeriod()
  {
    DateTime now = new DateTime("2012-12-31T01:00:00");
    PeriodDropRule rule = new PeriodDropRule(new Period("P1M"), false);

    Assert.assertTrue(rule.appliesTo(new Interval(now.minusWeeks(1), now.minusDays(1)), now));
    Assert.assertTrue(rule.appliesTo(new Interval(now.minusDays(1), now), now));
    Assert.assertFalse(rule.appliesTo(new Interval(now.minusYears(1), now.minusDays(1)), now));
    Assert.assertFalse(rule.appliesTo(new Interval(now.minusMonths(2), now.minusDays(1)), now));
  }

  @Test
  public void testAppliesTo()
  {
    // 2019-11-01T05:05:58 ~ 2019-11-01T07:05:58
    DateTime now = new DateTime("2019-11-01T07:05:58");

    Rule rule = new PeriodDropRule(new Period("PT2H"), false);
    validate(now, rule, new boolean[]{false, false, true, false, false});

    rule = new PeriodDropRule(new Period("PT2H"), true);
    validate(now, rule, new boolean[]{false, false, true, true, true});

    rule = new PeriodDropRule(new Period("PT2H"), null);
    validate(now, rule, new boolean[]{false, false, true, true, true});
  }

  private void validate(DateTime now, Rule rule, boolean[] results)
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
