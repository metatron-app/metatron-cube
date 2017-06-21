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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularities;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.InDimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
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
  private static final List<String> metrics = Arrays.asList("index", "indexMin", "indexMaxPlusTen", "quality_uniques");

  @Parameterized.Parameters(name = "{1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.makeQueryRunnersWithName(
        new SelectMetaQueryRunnerFactory(
            new SelectMetaQueryToolChest(),
            new SelectMetaQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
  }

  private final QueryRunner runner;
  private final String name;

  public SelectMetaQueryRunnerTest(QueryRunner runner, String name)
  {
    this.runner = runner;
    this.name = name;
  }

  // storing index value with input type 'float' and 'double' makes difference in stored size (about 40%)
  // 94.874713 vs 94.87471008300781

  @Test
  public void testBasic()
  {
    boolean incremental = name.equals("rtIndex") || name.equals("noRollupRtIndex");
    List<String> dimensions = Arrays.asList("market");

    SelectMetaQuery query = new SelectMetaQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new MultipleIntervalSegmentSpec(Arrays.asList(new Interval("2011-01-12/2011-01-14"))),
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        null,
        null,
        null,
        Maps.<String, Object>newHashMap()
    );

    List<Result<SelectMetaResultValue>> results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );

    Assert.assertEquals(1, results.size());
    Result<SelectMetaResultValue> r = results.get(0);
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of("testSegment", 26), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(26, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 598 : 988, r.getValue().getEstimatedSize());

    query = query.withDimFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 8), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(8, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 184 : 304, r.getValue().getEstimatedSize());

    query = query.withQueryGranularity(QueryGranularities.DAY);
    results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(2, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 4), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(4, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 92 : 152, r.getValue().getEstimatedSize());

    r = results.get(1);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 13, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 4), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(4, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 92 : 152, r.getValue().getEstimatedSize());
  }

  @Test
  public void testPaging()
  {
    boolean incremental = name.equals("rtIndex") || name.equals("noRollupRtIndex");

    List<String> dimensions = Arrays.asList("market");
    List<String> metrics = Arrays.asList("index", "indexMin", "indexMaxPlusTen");

    SelectMetaQuery query = new SelectMetaQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        new MultipleIntervalSegmentSpec(Arrays.asList(new Interval("2011-01-12/2011-01-14"))),
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList("market")),
        metrics,
        null,
        new PagingSpec(ImmutableMap.of("testSegment", 3), -1),
        Maps.<String, Object>newHashMap()
    );

    List<Result<SelectMetaResultValue>> results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );

    Assert.assertEquals(1, results.size());
    Result<SelectMetaResultValue> r = results.get(0);
    Assert.assertEquals(new DateTime(2011, 1, 12, 0, 0), r.getTimestamp());
    Assert.assertEquals(ImmutableMap.of("testSegment", 23), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(23, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 529 : 460, r.getValue().getEstimatedSize());

    query = query.withDimFilter(new InDimFilter("quality", Arrays.asList("mezzanine", "health"), null));
    results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 5), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(5, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 115 : 100, r.getValue().getEstimatedSize());

    query = query.withQueryGranularity(QueryGranularities.DAY);
    results = Sequences.toList(
        runner.run(query, ImmutableMap.of()),
        Lists.<Result<SelectMetaResultValue>>newArrayList()
    );
    Assert.assertEquals(2, results.size());
    r = results.get(0);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 12, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 1), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(1, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 23 : 20, r.getValue().getEstimatedSize());

    r = results.get(1);
    Assert.assertEquals(r.getTimestamp(), new DateTime(2011, 1, 13, 0, 0));
    Assert.assertEquals(ImmutableMap.of("testSegment", 1), r.getValue().getPerSegmentCounts());
    Assert.assertEquals(dimensions, r.getValue().getDimensions());
    Assert.assertEquals(Sets.newHashSet(metrics), Sets.newHashSet(r.getValue().getMetrics()));
    Assert.assertEquals(1, r.getValue().getTotalCount());
    Assert.assertEquals(incremental ? 23 : 20, r.getValue().getEstimatedSize());
  }
}
