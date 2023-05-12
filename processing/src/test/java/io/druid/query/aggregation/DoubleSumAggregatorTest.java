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

import io.druid.query.filter.ValueMatcher;
import org.apache.commons.lang.mutable.MutableDouble;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregatorTest
{
  private MutableDouble aggregate(TestFloatColumnSelector selector, Aggregator.FromMutableDouble agg, MutableDouble aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  @Test
  public void testAggregate()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);
    Aggregator.FromMutableDouble agg = DoubleSumAggregator.create(selector, ValueMatcher.TRUE);

    Double expectedFirst = new Double(values[0]).doubleValue();
    Double expectedSecond = new Double(values[1]).doubleValue() + expectedFirst;

    MutableDouble aggregate = null;
    Assert.assertEquals(Double.valueOf(0), agg.get(aggregate));
    Assert.assertEquals(Double.valueOf(0), agg.get(aggregate));
    Assert.assertEquals(Double.valueOf(0), agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertEquals(expectedFirst, agg.get(aggregate));
    Assert.assertEquals(expectedFirst, agg.get(aggregate));
    Assert.assertEquals(expectedFirst, agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertEquals(expectedSecond, agg.get(aggregate));
    Assert.assertEquals(expectedSecond, agg.get(aggregate));
    Assert.assertEquals(expectedSecond, agg.get(aggregate));
  }

  @Test
  public void testComparator()
  {
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(new float[]{0.15f, 0.27f});
    Aggregator.FromMutableDouble agg = DoubleSumAggregator.create(selector, ValueMatcher.TRUE);

    MutableDouble aggregate = null;
    Object first = agg.get(aggregate);
    aggregate = agg.aggregate(aggregate);

    Comparator comp = new DoubleSumAggregatorFactory("null", "null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get(aggregate)));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(aggregate), agg.get(aggregate)));
    Assert.assertEquals(1, comp.compare(agg.get(aggregate), first));
  }
}
