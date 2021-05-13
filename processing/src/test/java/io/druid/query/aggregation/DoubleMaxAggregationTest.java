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
public class DoubleMaxAggregationTest
{
  private DoubleMaxAggregatorFactory doubleMaxAggFactory;
  private ColumnSelectorFactory colSelectorFactory;
  private TestFloatColumnSelector selector;

  private float[] values = {1.1f, 2.7f, 3.5f, 1.3f};

  public DoubleMaxAggregationTest() throws Exception
  {
    String aggSpecJson = "{\"type\": \"doubleMax\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    doubleMaxAggFactory = new DefaultObjectMapper().readValue(aggSpecJson , DoubleMaxAggregatorFactory.class);
  }

  @Before
  public void setup()
  {
    selector = new TestFloatColumnSelector(values);
    colSelectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(colSelectorFactory.makeFloatColumnSelector("nilly")).andReturn(selector);
    EasyMock.replay(colSelectorFactory);
  }

  @Test
  public void testDoubleMaxAggregator()
  {
    DoubleMaxAggregator agg = (DoubleMaxAggregator) doubleMaxAggFactory.factorize(colSelectorFactory);

    MutableDouble aggregate = null;
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);
    aggregate = aggregate(selector, agg, aggregate);

    Assert.assertEquals(values[2], agg.get(aggregate).doubleValue(), 0.0001);
  }

  @Test
  public void testDoubleMaxBufferAggregator()
  {
    DoubleMaxBufferAggregator agg = (DoubleMaxBufferAggregator) doubleMaxAggFactory.factorizeBuffered(colSelectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Byte.BYTES + Double.BYTES]);
    agg.init(buffer, 0, 0);

    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);
    aggregate(selector, agg, buffer, 0);

    Assert.assertEquals(values[2], ((Double) agg.get(buffer, 0, 0)).doubleValue(), 0.0001);
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(3.4d, doubleMaxAggFactory.combiner().combine(1.2, 3.4).doubleValue(), 0.0001);
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    DoubleMaxAggregatorFactory one = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory oneMore = new DoubleMaxAggregatorFactory("name1", "fieldName1");
    DoubleMaxAggregatorFactory two = new DoubleMaxAggregatorFactory("name2", "fieldName2");

    Assert.assertEquals(one.hashCode(), oneMore.hashCode());

    Assert.assertTrue(one.equals(oneMore));
    Assert.assertFalse(one.equals(two));
  }

  private MutableDouble aggregate(TestFloatColumnSelector selector, DoubleMaxAggregator agg, MutableDouble aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  private void aggregate(TestFloatColumnSelector selector, DoubleMaxBufferAggregator agg, ByteBuffer buff, int position)
  {
    agg.aggregate(buff, 0, position);
    selector.increment();
  }
}
