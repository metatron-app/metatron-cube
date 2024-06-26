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

package io.druid.timeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.TestObjectMapper;
import io.druid.data.input.InputRow;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.ShardSpecLookup;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class DataSegmentTest
{
  private final static ObjectMapper mapper = new TestObjectMapper();
  private final static ObjectMapper skip_mapper = mapper.copy().registerModule(
      new SimpleModule().addDeserializer(DataSegment.class, DataSegment.DS_SKIP_LOADSPEC)
  );

  private final static int TEST_VERSION = 0x7;

  private static ShardSpec getShardSpec(final int partitionNum)
  {
    return new ShardSpec()
    {
      @Override
      public <T> PartitionChunk<T> createChunk(T obj)
      {
        return null;
      }

      @Override
      public boolean isInChunk(long timestamp, InputRow inputRow)
      {
        return false;
      }

      @Override
      public int getPartitionNum()
      {
        return partitionNum;
      }

      @Override
      public ShardSpecLookup getLookup(List<ShardSpec> shardSpecs)
      {
        return null;
      }
    };
  }

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
        LinearShardSpec.of(100),
        TEST_VERSION,
        1,
        2
    );

    final String content = mapper.writeValueAsString(segment);
    Map<String, Object> objectMap = mapper.readValue(
        content,
        new TypeReference<Map<String, Object>>()
        {
        }
    );

    Assert.assertEquals(10, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "linear", "partitionNum", 100), objectMap.get("shardSpec"));
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(2, objectMap.get("numRows"));

    DataSegment deserializedSegment = mapper.readValue(content, DataSegment.class);

    Assert.assertEquals(segment.getDataSource(), deserializedSegment.getDataSource());
    Assert.assertEquals(segment.getInterval(), deserializedSegment.getInterval());
    Assert.assertEquals(segment.getVersion(), deserializedSegment.getVersion());
    Assert.assertEquals(segment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assert.assertEquals(segment.getDimensions(), deserializedSegment.getDimensions());
    Assert.assertEquals(segment.getMetrics(), deserializedSegment.getMetrics());
    Assert.assertEquals(segment.getShardSpec(), deserializedSegment.getShardSpec());
    Assert.assertEquals(segment.getSize(), deserializedSegment.getSize());
    Assert.assertEquals(segment.getNumRows(), deserializedSegment.getNumRows());
    Assert.assertEquals(segment.getIdentifier(), deserializedSegment.getIdentifier());

    DataSegment deserializedSegment2 = skip_mapper.readValue(content, DataSegment.class);

    Assert.assertEquals(segment.getDataSource(), deserializedSegment2.getDataSource());
    Assert.assertEquals(segment.getInterval(), deserializedSegment2.getInterval());
    Assert.assertEquals(segment.getVersion(), deserializedSegment2.getVersion());
    Assert.assertEquals(DataSegment.MASKED, deserializedSegment2.getLoadSpec());
    Assert.assertEquals(segment.getDimensions(), deserializedSegment2.getDimensions());
    Assert.assertEquals(segment.getMetrics(), deserializedSegment2.getMetrics());
    Assert.assertEquals(segment.getShardSpec(), deserializedSegment2.getShardSpec());
    Assert.assertEquals(segment.getSize(), deserializedSegment2.getSize());
    Assert.assertEquals(segment.getNumRows(), deserializedSegment2.getNumRows());
    Assert.assertEquals(segment.getIdentifier(), deserializedSegment2.getIdentifier());

    deserializedSegment = mapper.readValue(content, DataSegment.class);
    Assert.assertEquals(0, segment.compareTo(deserializedSegment));

    deserializedSegment = mapper.readValue(content, DataSegment.class);
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));

    deserializedSegment = mapper.readValue(content, DataSegment.class);
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
                                           .shardSpec(getShardSpec(0))
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
                                           .shardSpec(getShardSpec(7))
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z_7",
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
