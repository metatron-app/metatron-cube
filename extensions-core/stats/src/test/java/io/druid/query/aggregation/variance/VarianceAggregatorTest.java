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

package io.druid.query.aggregation.variance;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.TestDoubleColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import io.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class VarianceAggregatorTest
{
  private VarianceAggregatorFactory aggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestDoubleColumnSelector selector;

  private final double[] values = {1.1f, 2.7f, 3.5f, 1.3f};
  private final double[] variances_pop = new double[values.length]; // calculated
  private final double[] variances_samp = new double[values.length]; // calculated

  public VarianceAggregatorTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"variance\", \"name\": \"billy\", \"fieldName\": \"nilly\", \"inputType\": \"double\"}";
    aggFactory = new DefaultObjectMapper().readValue(aggSpecJson, VarianceAggregatorFactory.class);
    double sum = 0;
    for (int i = 0; i < values.length; i++) {
      sum += values[i];
      if (i > 0) {
        double mean = sum / (i + 1);
        double temp = 0;
        for (int j = 0; j <= i; j++) {
          temp += Math.pow(values[j] - mean, 2);
        }
        variances_pop[i] = temp / (i + 1);
        variances_samp[i] = temp / i;
      }
    }
  }

  @Before
  public void setup()
  {
    selector = new TestDoubleColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeObjectColumnSelector("nilly")).andReturn(new TestObjectColumnSelector(0.0f));
    EasyMock.expect(colSelectorFactory.makeDoubleColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleVarianceAggregator()
  {
    VarianceAggregator agg = (VarianceAggregator) aggFactory.factorize(colSelectorFactory);

    VarianceAggregatorCollector aggregate = null;
    Assert.assertNull(agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    assertValues(agg.get(aggregate), 1, 1.1d, 0d);
    aggregate = aggregate(selector, agg, aggregate);
    assertValues(agg.get(aggregate), 2, 3.8d, 1.28d);
    aggregate = aggregate(selector, agg, aggregate);
    assertValues(agg.get(aggregate), 3, 7.3d, 2.9866d);
    aggregate = aggregate(selector, agg, aggregate);
    assertValues(agg.get(aggregate), 4, 8.6d, 3.95d);
  }

  private void assertValues(VarianceAggregatorCollector holder, long count, double sum, double nvariance)
  {
    Assert.assertEquals(count, holder.count);
    Assert.assertEquals(sum, holder.sum, 0.0001);
    Assert.assertEquals(nvariance, holder.nvariance, 0.0001);
    if (count == 0) {
      // changed semantic
      Assert.assertNull(holder.getVariance(false));
    } else {
      Assert.assertEquals(holder.getVariance(true), variances_pop[(int) count - 1], 0.0001);
      Assert.assertEquals(holder.getVariance(false), variances_samp[(int) count - 1], 0.0001);
    }
  }

  @Test
  public void testDoubleVarianceBufferAggregator()
  {
    VarianceBufferAggregator agg = (VarianceBufferAggregator) aggFactory.factorizeBuffered(
        colSelectorFactory
    );

    ByteBuffer buffer = ByteBuffer.wrap(new byte[aggFactory.getMaxIntermediateSize()]);
    agg.init(buffer, 0, 0);

    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0, 0), 0, 0d, 0d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0, 0), 1, 1.1d, 0d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0, 0), 2, 3.8d, 1.28d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0, 0), 3, 7.3d, 2.9866d);
    aggregate(selector, agg, buffer, 0);
    assertValues((VarianceAggregatorCollector) agg.get(buffer, 0, 0), 4, 8.6d, 3.95d);
  }

  @Test
  public void testCombine()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector().add(1.1f).add(2.7f);
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector().add(3.5f).add(1.3f);
    VarianceAggregatorCollector expected = new VarianceAggregatorCollector(4, 8.6d, 3.95d);
    Assert.assertTrue(expected.equalsWithEpsilon(
        aggFactory.combiner().apply(holder1, holder2), 0.00001)
    );
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    VarianceAggregatorFactory one = new VarianceAggregatorFactory("name1", "fieldName1");
    VarianceAggregatorFactory oneMore = new VarianceAggregatorFactory("name1", "fieldName1");
    VarianceAggregatorFactory two = new VarianceAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private VarianceAggregatorCollector aggregate(TestDoubleColumnSelector selector, VarianceAggregator agg, VarianceAggregatorCollector aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  private void aggregate(
      TestDoubleColumnSelector selector,
      VarianceBufferAggregator agg,
      ByteBuffer buff,
      int position
  )
  {
    agg.aggregate(buff, 0, position);
    selector.increment();
  }
}
