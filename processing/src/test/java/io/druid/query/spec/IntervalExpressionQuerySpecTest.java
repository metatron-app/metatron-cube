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

package io.druid.query.spec;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.DateTimes;
import io.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class IntervalExpressionQuerySpecTest
{
  static {
    TestHelper.JSON_MAPPER.registerModule(new SimpleModule().registerSubtypes(IntervalExpressionQuerySpec.class));
  }

  private static final String MIN = DateTimes.MIN.toString();
  private static final String MAX = DateTimes.MAX.toString();

  @Test
  public void test() throws Exception
  {
    validate("interval('2011-01-01', '2012-01-01')", new Interval("2011-01-01/2012-01-01"));
    validate("interval('2011-01-01', 'P4Y')", new Interval("2011-01-01/2015-01-01"));
    validate("interval('2011-01-01', 'P1M')", new Interval("2011-01-01/2011-02-01"));
    validate("interval(NULL, '2012-01-01')", new Interval(String.format("%s/2012-01-01", MIN)));
    validate("interval('2011-01-01', NULL)", new Interval(String.format("2011-01-01/%s", MAX)));
    validate("interval(bucketStart(current_time(), 'YEAR'), NULL)", null);
  }

  private void validate(String expression, Interval expected) throws Exception
  {
    QuerySegmentSpec expectedSpec = IntervalExpressionQuerySpec.of(expression);
    QuerySegmentSpec convertedSpec = TestHelper.JSON_MAPPER.readValue(
        String.format("{\"type\": \"expression\", \"expressions\": [\"%s\"]}", expression),
        QuerySegmentSpec.class
    );
    Assert.assertEquals(expression, expectedSpec, convertedSpec);
    if (expected != null) {
      Assert.assertEquals(expression, expected, Iterables.getOnlyElement(expectedSpec.getIntervals()));
    } else {
      System.out.println(convertedSpec.getIntervals());
    }
  }
}
