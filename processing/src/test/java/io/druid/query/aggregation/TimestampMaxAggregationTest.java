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

import com.google.common.collect.Lists;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.AggregatorsModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.lang.mutable.MutableLong;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

public class TimestampMaxAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final TimestampMaxAggregatorFactory aggregatorFactory;
  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private final Timestamp[] values = new Timestamp[10];

  public TimestampMaxAggregationTest() throws Exception
  {
    String json = "{\"type\":\"timeMax\",\"name\":\"tmax\",\"fieldName\":\"test\"}";
    aggregatorFactory = new DefaultObjectMapper().readValue(json, TimestampMaxAggregatorFactory.class);

    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new AggregatorsModule()),
        temporaryFolder
    );
  }

  @Before
  public void setup()
  {
    Timestamp time = Timestamp.valueOf("2014-01-02 12:00:00");

    for (int idx = 0; idx < values.length; idx++) {
      values[idx] = new Timestamp(time.getTime() - 10000L * idx);
    }

    selector = new TestObjectColumnSelector((Object[]) values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeObjectColumnSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);

  }

  @Test
  public void testAggregator()
  {
    TimestampMaxAggregator aggregator = (TimestampMaxAggregator) aggregatorFactory.factorize(selectorFactory);

    MutableLong aggregate = null;
    for (Timestamp value: values) {
      aggregate = aggregate(selector, aggregator, aggregate);
    }

    Assert.assertEquals(values[0], new Timestamp(aggregator.get(aggregate)));
  }

  @Test
  public void testBufferAggregator()
  {
    TimestampMaxBufferAggregator aggregator = (TimestampMaxBufferAggregator) aggregatorFactory.factorizeBuffered(selectorFactory);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
    aggregator.init(buffer, 0, 0);

    for (Timestamp value: values) {
      aggregate(selector, aggregator, buffer, 0);
    }

    Assert.assertEquals(values[0], new Timestamp(aggregator.getLong(buffer, 0)));

    aggregator.init(buffer, 0, 0);

    Assert.assertEquals(Long.MIN_VALUE, aggregator.get(buffer, 0, 0));
  }

  @Test
  public void testSimpleDataIngestionAndGroupByTest() throws Exception
  {
    String recordParser = "{\n" +
        "  \"type\": \"string\",\n" +
        "  \"parseSpec\": {\n" +
        "    \"format\": \"tsv\",\n" +
        "    \"timestampSpec\": {\n" +
        "      \"column\": \"timestamp\",\n" +
        "      \"format\": \"auto\"\n" +
        "    },\n" +
        "    \"dimensionsSpec\": {\n" +
        "      \"dimensions\": [\n" +
        "        \"product\"\n" +
        "      ],\n" +
        "      \"dimensionExclusions\": [],\n" +
        "      \"spatialDimensions\": []\n" +
        "    },\n" +
        "    \"columns\": [\n" +
        "      \"timestamp\",\n" +
        "      \"cat\",\n" +
        "      \"product\",\n" +
        "      \"prefer\",\n" +
        "      \"prefer2\",\n" +
        "      \"pty_country\"\n" +
        "    ]\n" +
        "  }\n" +
        "}";
    String aggregator = "[\n" +
        "  {\n" +
        "    \"type\": \"timeMax\",\n" +
        "    \"name\": \"tmax\",\n" +
        "    \"fieldName\": \"__time\"\n" +
        "  }\n" +
        "]";
    String groupBy = "{\n" +
        "  \"queryType\": \"groupBy\",\n" +
        "  \"dataSource\": \"test_datasource\",\n" +
        "  \"granularity\": \"MONTH\",\n" +
        "  \"dimensions\": [\"product\"],\n" +
        "  \"aggregations\": [\n" +
        "    {\n" +
        "      \"type\": \"timeMax\",\n" +
        "      \"name\": \"time_max\",\n" +
        "      \"fieldName\": \"tmax\"\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [\n" +
        "    \"2011-01-12T00:00:00.000Z/2011-04-16T00:00:00.000Z\"\n" +
        "  ],\n" +
        "  \"context\": {" +
        "      \"groupByUseRawUTF8\": false" +
        "  }\n" +
        "}";
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("druid.sample.tsv").getFile()),
        recordParser,
        aggregator,
        0,
        QueryGranularities.NONE,
        100,
        groupBy
    );

    List<Row> results = Sequences.toList(seq);
    Assert.assertEquals(36, results.size());
  }

  private MutableLong aggregate(TestObjectColumnSelector selector, TimestampMaxAggregator agg, MutableLong aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  private void aggregate(TestObjectColumnSelector selector, TimestampMaxBufferAggregator agg, ByteBuffer buf, int pos)
  {
    agg.aggregate(buf, 0, pos);
    selector.increment();
  }
}
