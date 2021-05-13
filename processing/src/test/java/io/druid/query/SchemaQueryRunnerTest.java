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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestIndex;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SchemaQueryRunnerTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public SchemaQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  private String getSegmentId(Interval interval)
  {
    return new DataSegment(dataSource, interval, "0", null, null, null, null, null, 0).getIdentifier();
  }

  // storing index value with input type 'float' and 'double' makes difference in stored size (about 40%)
  // 94.874713 vs 94.87471008300781

  @Test
  public void testSchema() throws IOException
  {
    boolean incremental = dataSource.equals(TestIndex.REALTIME) || dataSource.equals(TestIndex.REALTIME_NOROLLUP);
    List<String> dimensions = Arrays.asList("market");
    List<String> metrics = Arrays.asList("index", "indexMin", "indexMaxPlusTen", "quality_uniques", "indexDecimal");

    Interval interval = dataSource.equals(TestIndex.MMAPPED_SPLIT) ? TestIndex.INTERVAL_TOP : TestIndex.INTERVAL;
    String segmentId = getSegmentId(interval);
    SchemaQuery query = SchemaQuery.of(dataSource, MultipleIntervalSegmentSpec.of(new Interval("2011-01-12/2011-01-14")));

    List<Schema> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Schema>newArrayList()
    );

    Schema schema = Iterables.getOnlyElement(results);

    List<Pair<String, ValueDesc>> expected;
    if (incremental) {
      expected = Arrays.asList(
          Pair.of("__time", ValueDesc.LONG),
          Pair.of("market", ValueDesc.DIM_STRING),
          Pair.of("quality", ValueDesc.DIM_STRING),
          Pair.of("placement", ValueDesc.DIM_STRING),
          Pair.of("placementish", ValueDesc.DIM_STRING),
          Pair.of("partial_null_column", ValueDesc.DIM_STRING),
          Pair.of("null_column", ValueDesc.DIM_STRING),
          Pair.of("index", ValueDesc.DOUBLE),
          Pair.of("indexMin", ValueDesc.FLOAT),
          Pair.of("indexMaxPlusTen", ValueDesc.DOUBLE),
          Pair.of("quality_uniques", ValueDesc.of("hyperUnique")),
          Pair.of("indexDecimal", ValueDesc.DECIMAL)
      );
    } else {
      expected = Arrays.asList(
          Pair.of("__time", ValueDesc.LONG),
          Pair.of("market", ValueDesc.DIM_STRING),
          Pair.of("quality", ValueDesc.DIM_STRING),
          Pair.of("placement", ValueDesc.DIM_STRING),
          Pair.of("placementish", ValueDesc.DIM_STRING),
          Pair.of("partial_null_column", ValueDesc.DIM_STRING),
          Pair.of("indexMin", ValueDesc.FLOAT),
          Pair.of("quality_uniques", ValueDesc.of("hyperUnique")),
          Pair.of("index", ValueDesc.DOUBLE),
          Pair.of("indexDecimal", ValueDesc.of("decimal(18,0,DOWN)")),
          Pair.of("indexMaxPlusTen", ValueDesc.DOUBLE)
      );
    }
    System.out.println("[SchemaQueryRunnerTest/testSchema] " + schema.columnAndTypes());
    Assert.assertTrue(Iterables.elementsEqual(expected, schema.columnAndTypes()));

    // retrieves segment schema
    SchemaQuery schemaQuery = SchemaQuery.of(dataSource);
    Schema schema2 = Sequences.only(
        schemaQuery.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap())
    );
    List<Pair<String, ValueDesc>> expected2;
    if (dataSource.contains("mmapped")) {
      expected2 = Arrays.asList(
          Pair.of("__time", ValueDesc.LONG),
          Pair.of("market", ValueDesc.DIM_STRING),
          Pair.of("quality", ValueDesc.DIM_STRING),
          Pair.of("placement", ValueDesc.DIM_STRING),
          Pair.of("placementish", ValueDesc.DIM_STRING),
          Pair.of("partial_null_column", ValueDesc.DIM_STRING),
          Pair.of("indexMin", ValueDesc.FLOAT),
          Pair.of("quality_uniques", ValueDesc.of("hyperUnique")),
          Pair.of("index", ValueDesc.DOUBLE),
          Pair.of("indexDecimal", ValueDesc.of("decimal(18,0,DOWN)")),
          Pair.of("indexMaxPlusTen", ValueDesc.DOUBLE)
      );
    } else {
      expected2 = Arrays.asList(
          Pair.of("__time", ValueDesc.LONG),
          Pair.of("market", ValueDesc.DIM_STRING),
          Pair.of("quality", ValueDesc.DIM_STRING),
          Pair.of("placement", ValueDesc.DIM_STRING),
          Pair.of("placementish", ValueDesc.DIM_STRING),
          Pair.of("partial_null_column", ValueDesc.DIM_STRING),
          Pair.of("null_column", ValueDesc.DIM_STRING),
          Pair.of("index", ValueDesc.DOUBLE),
          Pair.of("indexMin", ValueDesc.FLOAT),
          Pair.of("indexMaxPlusTen", ValueDesc.DOUBLE),
          Pair.of("quality_uniques", ValueDesc.of("hyperUnique")),
          Pair.of("indexDecimal", ValueDesc.DECIMAL)
      );
    }
    Assert.assertTrue(Iterables.elementsEqual(expected2, schema2.columnAndTypes()));

    ObjectMapper mapper = TestIndex.segmentWalker.getMapper();
    CacheStrategy cacheStrategy = new SchemaQueryToolChest(DefaultGenericQueryMetricsFactory.instance()).getCacheStrategy(schemaQuery);
    Schema cached = (Schema) cacheStrategy.pullFromCache().apply(
        mapper.readValue(
            mapper.writeValueAsBytes(cacheStrategy.prepareForCache().apply(schema2)), cacheStrategy.getCacheObjectClazz()
        )
    );
    Assert.assertTrue(Iterables.elementsEqual(cached.columnAndTypes(), schema2.columnAndTypes()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getAggregators().keySet(), schema2.getAggregators().keySet()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getAggregators().values(), schema2.getAggregators().values()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getCapabilities().keySet(), schema2.getCapabilities().keySet()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getCapabilities().values(), schema2.getCapabilities().values()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getDescriptors().keySet(), schema2.getDescriptors().keySet()));
    Assert.assertTrue(Iterables.elementsEqual(cached.getDescriptors().values(), schema2.getDescriptors().values()));
  }
}
