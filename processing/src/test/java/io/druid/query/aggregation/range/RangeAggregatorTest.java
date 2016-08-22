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

package io.druid.query.aggregation.range;

import io.druid.query.aggregation.DoubleSumAggregator;
import io.druid.query.aggregation.TestDoubleColumnSelector;
import org.junit.Assert;
import org.junit.Test;

public class RangeAggregatorTest
{
  private void aggregate(TestDoubleColumnSelector selector, RangeAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testNormal()
  {
    final double[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestDoubleColumnSelector selector = new TestDoubleColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(DoubleSumAggregator.create(selector, null), 1, 3);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.2d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.5d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9d, rangeAggregator.getDouble(), 0.00001);
  }

  @Test
  public void testSmall()
  {
    final double[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestDoubleColumnSelector selector = new TestDoubleColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(DoubleSumAggregator.create(selector, null), 5, 3);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.3d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.6d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.0d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.5d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.6d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.3d, rangeAggregator.getDouble(), 0.00001);
  }

  @Test
  public void testExceed()
  {
    final double[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestDoubleColumnSelector selector = new TestDoubleColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(DoubleSumAggregator.create(selector, null), 0, 1);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1d, rangeAggregator.getDouble(), 0.00001);
  }
}
