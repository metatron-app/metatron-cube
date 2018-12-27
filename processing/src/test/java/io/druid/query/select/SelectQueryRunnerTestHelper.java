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

package io.druid.query.select;

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
public class SelectQueryRunnerTestHelper
{
  public static Builder builder(String[] columnNames)
  {
    return new Builder(columnNames);
  }

  public static class Builder
  {
    private final String[] columnNames;
    private final List<Result<SelectResultValue>> results = Lists.newArrayList();

    public Builder(String[] columnNames)
    {
      this.columnNames = columnNames;
    }

    public Builder add(DateTime timestamp, Object[]... values)
    {
      results.add(createExpected(timestamp, columnNames, values));
      return this;
    }

    public List<Result<SelectResultValue>> build()
    {
      return results;
    }
  }

  public static Result<SelectResultValue> createExpected(
      DateTime timestamp, String[] columnNames, Object[]... values
  )
  {
    List<EventHolder> events = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length + 2);
      String segmentId = (String) value[0];
      int offset = (Integer) value[1];
      Map<String, Object> event = Maps.newHashMapWithExpectedSize(value.length);
      for (int i = 0; i < columnNames.length; i++) {
        if (value[i + 2] != null) {
          event.put(columnNames[i], value[i + 2]);
        }
      }
      events.add(new EventHolder(segmentId, offset, event));
    }
    return new Result<>(timestamp, new SelectResultValue(null, events));
  }

  public static void validate(
      String[] columnNames,
      List<Result<SelectResultValue>> expected,
      List<Result<SelectResultValue>> result
  )
  {
    int max1 = Math.min(expected.size(), result.size());
    for (int i = 0; i < max1; i++) {
      Result<SelectResultValue> e = expected.get(i);
      Result<SelectResultValue> r = result.get(i);
      Assert.assertEquals(e.getTimestamp(), r.getTimestamp());
      List<EventHolder> es = e.getValue().getEvents();
      List<EventHolder> rs = r.getValue().getEvents();
      int max2 = Math.min(es.size(), rs.size());
      for (int j = 0; j < max2; j++) {
        EventHolder eh = es.get(j);
        EventHolder rh = rs.get(j);
        Assert.assertEquals(eh.getOffset(), rh.getOffset());
        Map<String, Object> em = eh.getEvent();
        Map<String, Object> rm = rh.getEvent();
        Assert.assertEquals(em + " vs " + rm, em.size(), rm.size());
        for (String columnName : columnNames) {
          Assert.assertEquals(j + " th event in " + i + " th result", em.get(columnName), rm.get(columnName));
        }
      }
      if (es.size() > rs.size()) {
        Assert.fail("need more event holders");
      }
      if (es.size() < rs.size()) {
        Assert.fail("need less event holders");
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
