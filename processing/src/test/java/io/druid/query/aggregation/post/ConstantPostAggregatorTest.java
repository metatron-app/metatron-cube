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
import io.druid.data.TypeResolver;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class ConstantPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    PostAggregator.Processor constantPostAggregator = new ConstantPostAggregator("shichi", 7).processor(TypeResolver.UNKNOWN);

    DateTime timestamp = DateTimes.nowUtc();
    Assert.assertEquals(7, constantPostAggregator.compute(timestamp, null));
    constantPostAggregator = new ConstantPostAggregator("rei", 0.0).processor(TypeResolver.UNKNOWN);
    Assert.assertEquals(0.0, constantPostAggregator.compute(timestamp, null));
    constantPostAggregator = new ConstantPostAggregator("ichi", 1.0).processor(TypeResolver.UNKNOWN);
    Assert.assertNotSame(1, constantPostAggregator.compute(timestamp, null));
  }

  @Test
  public void testComparator()
  {
    ConstantPostAggregator constantPostAggregator =
        new ConstantPostAggregator("thistestbasicallydoesnothing unhappyface", 1);
    Comparator comp = constantPostAggregator.getComparator();

    DateTime timestamp = DateTimes.nowUtc();
    Assert.assertEquals(0, comp.compare(0, constantPostAggregator.processor(TypeResolver.UNKNOWN).compute(timestamp, null)));
    Assert.assertEquals(0, comp.compare(0, 1));
    Assert.assertEquals(0, comp.compare(1, 0));
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ConstantPostAggregator aggregator = new ConstantPostAggregator("aggregator", 2);
    ConstantPostAggregator aggregator1 = mapper.readValue(
        mapper.writeValueAsString(aggregator),
        ConstantPostAggregator.class
    );
    Assert.assertEquals(aggregator, aggregator1);
  }
}
