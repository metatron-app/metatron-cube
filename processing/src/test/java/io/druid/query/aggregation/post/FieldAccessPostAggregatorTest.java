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

package io.druid.query.aggregation.post;

import io.druid.common.DateTimes;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class FieldAccessPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    PostAggregator.Processor fieldAccessPostAggregator;

    fieldAccessPostAggregator = new FieldAccessPostAggregator("To be, or not to be, that is the question:", "rows").createStateless();
    CountAggregator agg = new CountAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    MutableLong aggregate = null;
    metricValues.put("rows", agg.get(aggregate));

    DateTime timestamp = DateTimes.nowUtc();
    Assert.assertEquals(new Long(0L), fieldAccessPostAggregator.compute(timestamp, metricValues));

    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    metricValues.put("rows", agg.get(aggregate));
    Assert.assertEquals(new Long(3L), fieldAccessPostAggregator.compute(timestamp, metricValues));
  }
}
