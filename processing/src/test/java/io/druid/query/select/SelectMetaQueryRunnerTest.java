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

package io.druid.query.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.Pair;
import io.druid.query.CacheStrategy;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.Schema;
import io.druid.query.SchemaQuery;
import io.druid.query.SchemaQueryToolChest;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ExpressionDimensionSpec;
import io.druid.query.filter.InDimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
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
public class SelectMetaQueryRunnerTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public SelectMetaQueryRunnerTest(String dataSource)
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
  public void testBasic()
  {
    boolean incremental = dataSource.equals(TestIndex.REALTIME) || dataSource.equals(TestIndex.REALTIME_NOROLLUP);
    List<String> dimensions = Arrays.asList("market");
    List<String> metrics = Arrays.asList("index", "indexMin", "indexMaxPlusTen", "quality_uniques", "indexDecimal");

    Interval interval = dataSource.equals(TestIndex.MMAPPED_SPLIT) ? TestIndex.INTERVAL_TOP : TestIndex.INTERVAL;
    String segmentId = getSegmentId(interval);
    SelectMetaQuery query = new SelectMetaQuery(
        TableDataSource.of(dataSource),
        new MultipleIntervalSegmentSpec(Arrays.asList(new Interval("2011-01-12/2011-01-14"))),
        null,
        Granularities.ALL,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        null,
        null,
        null,
        Maps.<String, Object>newHashMap()
    );

    List<Result<SelectMetaResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );

    Result<SelectMetaResultValue> r = Iterables.getOnlyElement(results);
    SelectMetaResultValue value = r.getValue();
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 26), value.getPerSegmentCounts());
    Assert.assertEquals(26, value.getTotalCount());

    query = query.withFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    r = Iterables.getOnlyElement(results);
    value = r.getValue();
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 8), value.getPerSegmentCounts());
    Assert.assertEquals(8, value.getTotalCount());

    query = query.withQueryGranularity(QueryGranularities.DAY);
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(2, results.size());
    r = results.get(0);
    value = r.getValue();
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 4), value.getPerSegmentCounts());
    Assert.assertEquals(4, value.getTotalCount());

    r = results.get(1);
    Assert.assertEquals(new DateTime(2011, 1, 13, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 4), value.getPerSegmentCounts());
    Assert.assertEquals(4, value.getTotalCount());
  }

  @Test
  public void testPaging()
  {
    boolean incremental = dataSource.equals(TestIndex.REALTIME) || dataSource.equals(TestIndex.REALTIME_NOROLLUP);

    List<String> dimensions = Arrays.asList("market");
    List<String> metrics = Arrays.asList("index", "indexMin", "indexMaxPlusTen");

    Interval interval = dataSource.equals(TestIndex.MMAPPED_SPLIT) ? TestIndex.INTERVAL_TOP : TestIndex.INTERVAL;
    String segmentId = getSegmentId(interval);
    SelectMetaQuery query = new SelectMetaQuery(
        TableDataSource.of(dataSource),
        new MultipleIntervalSegmentSpec(Arrays.asList(new Interval("2011-01-12/2011-01-14"))),
        null,
        Granularities.ALL,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        metrics,
        null,
        new PagingSpec(ImmutableMap.of(segmentId, 3), -1),
        Maps.<String, Object>newHashMap()
    );

    List<Result<SelectMetaResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );

    Assert.assertEquals(1, results.size());
    Result<SelectMetaResultValue> r = results.get(0);
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 23), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(23, r.getValue().getTotalCount());

    query = query.withFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 5), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(5, r.getValue().getTotalCount());

    query = query.withQueryGranularity(QueryGranularities.DAY);
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(2, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 1), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(1, r.getValue().getTotalCount());

    r = results.get(1);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 13, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 1), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(1, r.getValue().getTotalCount());
  }

  @Test
  public void testSchema() throws IOException
  {
    SelectMetaQuery query = new SelectMetaQuery(
        TableDataSource.of(dataSource),
        MultipleIntervalSegmentSpec.of(new Interval("2011-01-12/2011-01-14")),
        null,
        Granularities.ALL,
        Arrays.asList(
            DefaultDimensionSpec.of("time"),
            DefaultDimensionSpec.of("market"),
            ExpressionDimensionSpec.of("bucketStart(__time, 'WEEK')", "week")
        ),
        Arrays.asList(
            "index", "indexMin", "indexMaxPlusTen", "quality_uniques"
        ),
        Arrays.<VirtualColumn>asList(
            new ExprVirtualColumn("cast(__time, 'datetime')", "time")
        ),
        null,
        Maps.<String, Object>newHashMap()
    );

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
          Pair.of("indexDecimal", ValueDesc.of(ValueDesc.DECIMAL_TYPE + "(18,0,DOWN)")),
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
    Schema cached = (Schema) cacheStrategy.pullFromCache(query).apply(
        mapper.readValue(
            mapper.writeValueAsBytes(cacheStrategy.prepareForCache(query).apply(schema2)), cacheStrategy.getCacheObjectClazz()
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
