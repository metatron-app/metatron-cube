/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.granularity.Granularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class MetadataTest
{
  @Test
  public void testSerde0() throws Exception
  {
    serdeTest("{}", new Metadata());
  }

  @Test
  public void testSerde1() throws Exception
  {
    Metadata metadata = new Metadata();
    metadata.put("k", "v");

    metadata.setAggregators(new AggregatorFactory[]{new LongSumAggregatorFactory("out", "in")});
    metadata.setQueryGranularity(Granularities.ALL);

    String expected = "{\"container\":{\"k\":\"v\"},"
                      + "\"aggregators\":[{\"type\":\"longSum\",\"name\":\"out\",\"fieldName\":\"in\"}],"
                      + "\"queryGranularity\":{\"type\":\"all\"}"
                      + "}";
    serdeTest(expected, metadata);
  }

  @Test
  public void testSerde2() throws Exception
  {
    Metadata metadata = new Metadata();
    metadata.put("k", "v");

    metadata.setAggregators(new AggregatorFactory[]{new LongSumAggregatorFactory("out", "in")});
    metadata.setQueryGranularity(Granularities.ALL);
    metadata.setNumRows(10);
    metadata.setIngestedNumRow(100L);
    metadata.setRollup(Boolean.FALSE);

    String expected = "{\"container\":{\"k\":\"v\"},"
                      + "\"aggregators\":[{\"type\":\"longSum\",\"name\":\"out\",\"fieldName\":\"in\"}],"
                      + "\"queryGranularity\":{\"type\":\"all\"},"
                      + "\"numRows\":10,"
                      + "\"ingestedNumRows\":100,"
                      + "\"rollup\":false"
                      + "}";
    serdeTest(expected, metadata);
  }

  private void serdeTest(String expected, Metadata metadata) throws Exception
  {
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    String content = jsonMapper.writeValueAsString(metadata);
    Assert.assertEquals(expected, content);
    Metadata other = jsonMapper.readValue(content, Metadata.class);

    Assert.assertEquals(metadata, other);
  }

  @Test
  public void testMerge()
  {
    Assert.assertNull(Metadata.merge(null, null));
    Assert.assertNull(Metadata.merge(ImmutableList.<Metadata>of(), null));

    List<Metadata> metadataToBeMerged = new ArrayList<>();

    metadataToBeMerged.add(null);
    Assert.assertNull(Metadata.merge(metadataToBeMerged, null));

    //sanity merge check
    AggregatorFactory[] aggs = new AggregatorFactory[]{new LongMaxAggregatorFactory("n", "f")};
    Metadata m1 = new Metadata();
    m1.put("k", "v");
    m1.setAggregators(aggs);
    m1.setQueryGranularity(Granularities.ALL);
    m1.setIngestedNumRow(100L);
    m1.setRollup(Boolean.FALSE);

    Metadata m2 = new Metadata();
    m2.put("k", "v");
    m2.setAggregators(aggs);
    m2.setQueryGranularity(Granularities.ALL);
    m2.setIngestedNumRow(200L);
    m2.setRollup(Boolean.FALSE);

    Metadata merged = new Metadata();
    merged.put("k", "v");
    merged.setAggregators(new AggregatorFactory[]{new LongMaxAggregatorFactory("n", "n")});
    merged.setRollup(Boolean.FALSE);
    merged.setQueryGranularity(Granularities.ALL);
    merged.setIngestedNumRow(300L);

    Assert.assertEquals(merged, Metadata.merge(ImmutableList.of(m1, m2), null));

    //merge check with one metadata being null
    metadataToBeMerged.clear();
    metadataToBeMerged.add(m1);
    metadataToBeMerged.add(m2);
    metadataToBeMerged.add(null);

    merged.setAggregators(null);
    merged.setQueryGranularity(null);
    merged.setRollup(null);
    Assert.assertEquals(merged, Metadata.merge(metadataToBeMerged, null));

    //merge check with client explicitly providing merged aggregators
    AggregatorFactory[] explicitAggs = new AggregatorFactory[]{new DoubleMaxAggregatorFactory("x", "y")};
    merged.setAggregators(explicitAggs);

    Assert.assertEquals(merged, Metadata.merge(metadataToBeMerged, explicitAggs));

    merged.setQueryGranularity(Granularities.ALL);
    m1.setRollup(Boolean.TRUE);
    Assert.assertEquals(merged, Metadata.merge(ImmutableList.of(m1, m2), explicitAggs));
  }

  @Test
  public void testMergeIngestedNumRows()
  {
    Metadata m1 = new Metadata();
    Metadata m2 = new Metadata();
    Assert.assertEquals(-1L, Metadata.merge(Arrays.asList(m1, m2), null).getIngestedNumRows());

    m1 = new Metadata();
    m1.setIngestedNumRow(100L);
    m2 = new Metadata();
    Assert.assertEquals(-1L, Metadata.merge(Arrays.asList(m1, m2), null).getIngestedNumRows());

    m1 = new Metadata();
    m2 = new Metadata();
    m2.setIngestedNumRow(100L);
    Assert.assertEquals(-1L, Metadata.merge(Arrays.asList(m1, m2), null).getIngestedNumRows());

    m1 = new Metadata();
    m1.setIngestedNumRow(100L);
    m2 = new Metadata();
    m2.setIngestedNumRow(200L);
    Assert.assertEquals(300L, Metadata.merge(Arrays.asList(m1, m2), null).getIngestedNumRows());
  }
}
