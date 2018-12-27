/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularities;
import io.druid.granularity.QueryGranularities;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
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
  @Parameterized.Parameters
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
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        null,
        null,
        false,
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
    Assert.assertEquals(dimensions, value.getSchema().getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(value.getSchema().getMetricNames()));
    Assert.assertEquals(26, value.getTotalCount());
    Assert.assertEquals(incremental ? 702 : 1248, value.getEstimatedSize());

    query = query.withDimFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    r = Iterables.getOnlyElement(results);
    value = r.getValue();
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 8), value.getPerSegmentCounts());
    Assert.assertEquals(dimensions, value.getSchema().getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(value.getSchema().getMetricNames()));
    Assert.assertEquals(8, value.getTotalCount());
    Assert.assertEquals(incremental ? 216 : 384, value.getEstimatedSize());

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
    Assert.assertEquals(dimensions, value.getSchema().getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(value.getSchema().getMetricNames()));
    Assert.assertEquals(4, value.getTotalCount());
    Assert.assertEquals(incremental ? 108 : 192, value.getEstimatedSize());

    r = results.get(1);
    Assert.assertEquals(new DateTime(2011, 1, 13, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of(segmentId, 4), value.getPerSegmentCounts());
    Assert.assertEquals(dimensions, value.getSchema().getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(value.getSchema().getMetricNames()));
    Assert.assertEquals(4, value.getTotalCount());
    Assert.assertEquals(incremental ? 108 : 192, value.getEstimatedSize());
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
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        metrics,
        null,
        false,
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
    Schema schema = r.getValue().getSchema();
    Assert.assertEquals(dimensions, schema.getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(schema.getMetricNames()));
    Assert.assertEquals(23, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 621 : 483, r.getValue().getEstimatedSize());

    query = query.withDimFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 5), r.getValue().getPerSegmentCounts());
    schema = r.getValue().getSchema();
    Assert.assertEquals(dimensions, schema.getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(schema.getMetricNames()));
    Assert.assertEquals(5, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 135 : 105, r.getValue().getEstimatedSize());

    query = query.withQueryGranularity(QueryGranularities.DAY);
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(2, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 1), r.getValue().getPerSegmentCounts());
    schema = r.getValue().getSchema();
    Assert.assertEquals(dimensions, schema.getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(schema.getMetricNames()));
    Assert.assertEquals(1, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 27 : 21, r.getValue().getEstimatedSize());

    r = results.get(1);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 13, 0, 0));
    Assert.assertEquals(ImmutableMap.of(segmentId, 1), r.getValue().getPerSegmentCounts());
    schema = r.getValue().getSchema();
    Assert.assertEquals(dimensions, schema.getDimensionNames());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(schema.getMetricNames()));
    Assert.assertEquals(1, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 27 : 21, r.getValue().getEstimatedSize());
  }

  @Test
  public void testSchema()
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
        true,
        null,
        Maps.<String, Object>newHashMap()
    );

    Schema schema = Iterables.getOnlyElement(
        Sequences.toList(
            query.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
            Lists.<Result<SelectMetaResultValue>>newArrayList()
        )
    ).getValue().getSchema();

    List<Pair<String, ValueDesc>> expected = Arrays.asList(
        Pair.of("time", ValueDesc.DATETIME),
        Pair.of("market", ValueDesc.ofDimension(ValueType.STRING)),
        Pair.of("week", ValueDesc.LONG),
        Pair.of("index", ValueDesc.DOUBLE),
        Pair.of("indexMin", ValueDesc.FLOAT),
        Pair.of("indexMaxPlusTen", ValueDesc.DOUBLE),
        Pair.of("quality_uniques", ValueDesc.of("hyperUnique"))
    );
    Assert.assertTrue(Iterables.elementsEqual(expected, schema.columnAndTypes()));
  }
}
