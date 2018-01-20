/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.timeseries;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.query.Result;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesQueryRunnerTestHelper
{
  public static List<Result<TimeseriesResultValue>> createExpected(String[] columnNames, Object[]... values)
  {
    List<Result<TimeseriesResultValue>> events = Lists.newArrayList();
    for (Object[] value : values) {
      DateTime timestamp = value[0] instanceof DateTime ? (DateTime) value[0] : new DateTime(value[0]);
      Preconditions.checkArgument(value.length == columnNames.length + 1);
      Map<String, Object> event = Maps.newHashMapWithExpectedSize(value.length);
      for (int i = 0; i < columnNames.length; i++) {
        if (value[i + 1] != null) {
          event.put(columnNames[i], value[i + 1]);
        }
      }
      events.add(new Result<TimeseriesResultValue>(timestamp, new TimeseriesResultValue(event)));
    }
    return events;
  }

  public static void validate(
      String[] columnNames,
      List<Result<TimeseriesResultValue>> expected,
      List<Result<TimeseriesResultValue>> result
  )
  {
    int max1 = Math.min(expected.size(), result.size());
    for (int i = 0; i < max1; i++) {
      Result<TimeseriesResultValue> e = expected.get(i);
      Result<TimeseriesResultValue> r = result.get(i);
      Assert.assertEquals(i + " th result", e.getTimestamp(), r.getTimestamp());
      Map<String, Object> em = e.getValue().getBaseObject();
      Map<String, Object> rm = r.getValue().getBaseObject();
      Assert.assertEquals(i + " th result", em.size(), rm.size());
      for (String columnName : columnNames) {
        Assert.assertEquals(i + " th result", em.get(columnName), rm.get(columnName));
      }
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more results");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less results");
    }
  }
}
