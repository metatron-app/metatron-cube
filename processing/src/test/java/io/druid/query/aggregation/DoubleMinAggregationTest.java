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

import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.lang.mutable.MutableDouble;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class DoubleMinAggregationTest
{
  private DoubleMinAggregatorFactory doubleMinAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestDoubleColumnSelector selector;

  private double[] values = {3.5f, 2.7f, 1.1f, 1.3f};

  public DoubleMinAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMin\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMinAggFactory = new DefaultObjectMapper().readValue(aggSpecJson , DoubleMinAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestDoubleColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeDoubleColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleMinAggregator()
  {
    DoubleMinAggregator agg = (DoubleMinAggregator) doubleMinAggFactory.factorize(colSelectorFactory);

    MutableDouble aggregate = null;
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);

    Assert.assertEquals(values[2], agg.get(aggregate).doubleValue(), 0.0001);
  }

  @Test
  public void testDoubleMinBufferAggregator()
  {
    DoubleMinBufferAggregator agg = (DoubleMinBufferAggregator) doubleMinAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Byte.BYTES + Double.BYTES]);
    agg.init(buffer, 0);

    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);

    Assert.assertEquals(values[2], ((Double) agg.get(buffer, 0)).doubleValue(), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(1.2d, (Double) doubleMinAggFactory.combiner().combine(1.2, 3.4), 0.0001);
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    DoubleMinAggregatorFactory one = new DoubleMinAggregatorFactory("name1", "fieldName1");
    DoubleMinAggregatorFactory oneMore = new DoubleMinAggregatorFactory("name1", "fieldName1");
    DoubleMinAggregatorFactory two = new DoubleMinAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private MutableDouble aggregate(TestDoubleColumnSelector selector, DoubleMinAggregator agg, MutableDouble aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  private void aggregate(TestDoubleColumnSelector selector, DoubleMinBufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, position);
    selector.increment();
  }
}
