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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.IndexIO;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class DataSegmentTest
{
  final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testV1Serialization() throws Exception
  {

    final Interval interval = new Interval("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object>of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1,
        2
    );

    final Map<String, Object> objectMap = mapper.readValue(mapper.writeValueAsString(segment), new TypeReference<Map<String, Object>>(){});

    Assert.assertEquals(10, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assert.assertEquals(IndexIO.CURRENT_VERSION_ID, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(2, objectMap.get("numRows"));

    DataSegment deserializedSegment = mapper.readValue(mapper.writeValueAsString(segment), DataSegment.class);

    Assert.assertEquals(segment.getDataSource(), deserializedSegment.getDataSource());
    Assert.assertEquals(segment.getInterval(), deserializedSegment.getInterval());
    Assert.assertEquals(segment.getVersion(), deserializedSegment.getVersion());
    Assert.assertEquals(segment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assert.assertEquals(segment.getDimensions(), deserializedSegment.getDimensions());
    Assert.assertEquals(segment.getMetrics(), deserializedSegment.getMetrics());
    Assert.assertEquals(segment.getShardSpecWithDefault(), deserializedSegment.getShardSpecWithDefault());
    Assert.assertEquals(segment.getSize(), deserializedSegment.getSize());
    Assert.assertEquals(segment.getNumRows(), deserializedSegment.getNumRows());
    Assert.assertEquals(segment.getIdentifier(), deserializedSegment.getIdentifier());

    deserializedSegment = mapper.readValue(mapper.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(0, segment.compareTo(deserializedSegment));

    deserializedSegment = mapper.readValue(mapper.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));

    deserializedSegment = mapper.readValue(mapper.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testIdentifier()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(new Interval("2012-01-01/2012-01-02"))
                                           .version(new DateTime("2012-01-01T11:22:33.444Z").toString())
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getIdentifier()
    );
  }

  @Test
  public void testIdentifierWithZeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(new Interval("2012-01-01/2012-01-02"))
                                           .version(new DateTime("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(new SingleDimensionShardSpec("bar", null, "abc", 0))
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getIdentifier()
    );
  }

  @Test
  public void testIdentifierWithNonzeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(new Interval("2012-01-01/2012-01-02"))
                                           .version(new DateTime("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(new SingleDimensionShardSpec("bar", "abc", "def", 1))
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z_1",
        segment.getIdentifier()
    );
  }

  @Test
  public void testV1SerializationNullMetrics() throws Exception
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(new Interval("2012-01-01/2012-01-02"))
                                           .version(new DateTime("2012-01-01T11:22:33.444Z").toString())
                                           .build();

    final DataSegment segment2 = mapper.readValue(mapper.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals("empty dimensions", ImmutableList.of(), segment2.getDimensions());
    Assert.assertEquals("empty metrics", ImmutableList.of(), segment2.getMetrics());
  }
}
