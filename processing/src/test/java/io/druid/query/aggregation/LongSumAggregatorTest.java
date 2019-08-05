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

package io.druid.query.aggregation;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class LongSumAggregatorTest
{
  private MutableLong aggregate(TestLongColumnSelector selector, LongSumAggregator agg, MutableLong aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  @Test
  public void testAggregate()
  {
    final TestLongColumnSelector selector = new TestLongColumnSelector(new long[]{24L, 20L});
    LongSumAggregator agg = LongSumAggregator.create(selector, null);

    MutableLong aggregate = null;
    Assert.assertEquals(0L, agg.get(aggregate));
    Assert.assertEquals(0L, agg.get(aggregate));
    Assert.assertEquals(0L, agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertEquals(24L, agg.get(aggregate));
    Assert.assertEquals(24L, agg.get(aggregate));
    Assert.assertEquals(24L, agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertEquals(44L, agg.get(aggregate));
    Assert.assertEquals(44L, agg.get(aggregate));
    Assert.assertEquals(44L, agg.get(aggregate));
  }

  @Test
  public void testComparator()
  {
    final TestLongColumnSelector selector = new TestLongColumnSelector(new long[]{18293L});
    LongSumAggregator agg = LongSumAggregator.create(selector, null);

    MutableLong aggregate = null;
    Object first = agg.get(aggregate);
    aggregate = agg.aggregate(aggregate);

    Comparator comp = new LongSumAggregatorFactory("null", "null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get(aggregate)));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(aggregate), agg.get(aggregate)));
    Assert.assertEquals(1, comp.compare(agg.get(aggregate), first));
  }
}
