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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class HistogramAggregatorTest
{
  private Histogram aggregate(TestFloatColumnSelector selector, HistogramAggregator agg, Histogram aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  @Test
  public void testSerde() throws Exception
  {
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    String json0 = "{\"type\": \"histogram\", \"name\": \"billy\", \"fieldName\": \"nilly\"}";
    HistogramAggregatorFactory agg0 = objectMapper.readValue(json0, HistogramAggregatorFactory.class);
    Assert.assertEquals(ImmutableList.of(), agg0.getBreaks());

    String aggSpecJson = "{\"type\": \"histogram\", \"name\": \"billy\", \"fieldName\": \"nilly\", \"breaks\": [ -1, 2, 3.0 ]}";
    HistogramAggregatorFactory agg = objectMapper.readValue(aggSpecJson, HistogramAggregatorFactory.class);

    Assert.assertEquals(new HistogramAggregatorFactory("billy", "nilly", Arrays.asList(-1f, 2f, 3.0f)), agg);
    Assert.assertEquals(agg, objectMapper.readValue(objectMapper.writeValueAsBytes(agg), HistogramAggregatorFactory.class));
  }

  @Test
  public void testAggregate() throws Exception {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    HistogramAggregator agg = new HistogramAggregator(selector, breaks);

    Histogram aggregate = null;
    Assert.assertNull(agg.get(aggregate));
    Assert.assertNull(agg.get(aggregate));
    Assert.assertNull(agg.get(aggregate));
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, agg.get(aggregate).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, agg.get(aggregate).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, agg.get(aggregate).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, agg.get(aggregate).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 0, 1, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 0, 2, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 1, 2, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 2, 2, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 1, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 2, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 0}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 1}, agg.get(aggregate).bins);
    aggregate = aggregate(selector, agg, aggregate);
    Assert.assertArrayEquals(new long[]{1, 3, 2, 3, 1, 1}, agg.get(aggregate).bins);
  }

  private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position)
  {
    agg.aggregate(buf, 0, position);
    selector.increment();
  }

  @Test
  public void testBufferAggregate() throws Exception {
    final float[] values = {0.55f, 0.27f, -0.3f, -.1f, -0.8f, -.7f, -.5f, 0.25f, 0.1f, 2f, -3f};
    final float[] breaks = {-1f, -0.5f, 0.0f, 0.5f, 1f};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    ArrayList<Float> b = Lists.newArrayList();
    for (int i = 0; i < breaks.length; ++i) {
      b.add(breaks[i]);
    }
    HistogramAggregatorFactory factory = new HistogramAggregatorFactory(
        "billy",
        "billy",
        b
    );
    HistogramBufferAggregator agg = new HistogramBufferAggregator(selector, breaks);

    ByteBuffer buf = ByteBuffer.allocateDirect(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, 0, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 0, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 0, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);
    Assert.assertArrayEquals(new long[]{0, 0, 0, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 1, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 0, 2, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 1, 2, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 2, 2, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 1, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 2, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 0}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{0, 3, 2, 3, 1, 1}, ((Histogram) agg.get(buf, 0, position)).bins);

    aggregateBuffer(selector, agg, buf, position);
    Assert.assertArrayEquals(new long[]{1,3,2,3,1,1}, ((Histogram)agg.get(buf, 0, position)).bins);
  }
}
