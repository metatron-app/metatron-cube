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
import io.druid.query.aggregation.TestFloatColumnSelector;
import org.junit.Assert;
import org.junit.Test;

public class RangeAggregatorTest
{
  private void aggregate(TestFloatColumnSelector selector, RangeAggregator agg)
  {
    agg.aggregate();
    selector.increment();
  }

  @Test
  public void testNormal()
  {
    final float[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(new DoubleSumAggregator("test", selector), 1, 3);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.2f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.5f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.9f, rangeAggregator.getFloat(), 0.00001);
  }

  @Test
  public void testSmall()
  {
    final float[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(new DoubleSumAggregator("test", selector), 5, 3);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.3f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.6f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.0f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.5f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.6f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(1.3f, rangeAggregator.getFloat(), 0.00001);
  }

  @Test
  public void testExceed()
  {
    final float[] values = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    RangeAggregator rangeAggregator = new RangeAggregator(new DoubleSumAggregator("test", selector), 0, 1);

    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
    aggregate(selector, rangeAggregator);
    Assert.assertEquals(0.1f, rangeAggregator.getFloat(), 0.00001);
  }
}
