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

package io.druid.query.groupby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.StupidPool;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.NoopQueryWatcher;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.select.StreamQueryEngine;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.segment.CloserRule;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

/**
 */
public class GroupByQueryRunnerFactoryTest
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);

  @Test
  public void testMergeRunnersEnsureGroupMerging() throws Exception
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(QueryGranularities.ALL)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("tags", "tags")))
        .setAggregatorSpecs(
            Arrays.asList(
                new AggregatorFactory[]
                    {
                        new CountAggregatorFactory("count")
                    }
            )
        )
        .build();

    QueryRunnerFactory factory = createFactory();
    QueryRunner mergedRunner = new FinalizeResultsQueryRunner(
        factory.mergeRunners(
            query,
            Executors.newSingleThreadExecutor(),
            ImmutableList.of(
                factory.createRunner(createSegment(), null),
                factory.createRunner(createSegment(), null)
            ),
            null
        ),
        factory.getToolchest()
    );

    Sequence<Row> result = mergedRunner.run(query, Maps.newHashMap());

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-01-12T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-01-12T00:00:00.000Z", "tags", "t2", "count", 4L)
    );

    TestHelper.assertExpectedObjects(expectedResults, Sequences.toList(result, new ArrayList<Row>()), "");
  }

  private Segment createSegment() throws Exception
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex(
        0,
        QueryGranularities.NONE,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count")
        },
        true,
        true,
        true,
        5000
    );

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new DefaultTimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags")), null, null),
            "\t",
            ImmutableList.of("timestamp", "product", "tags")
        ),
        "UTF-8"
    );

    String[] rows = new String[]{
        "2011-01-12T00:00:00.000Z,product_1,t1",
        "2011-01-13T00:00:00.000Z,product_2,t2",
        "2011-01-14T00:00:00.000Z,product_3,t2",
    };

    for (String row : rows) {
      incrementalIndex.add(parser.parse(row));
    }

    closerRule.closeLater(incrementalIndex);

    return new IncrementalIndexSegment(incrementalIndex, DataSegment.asKey("test"));
  }

  private GroupByQueryRunnerFactory createFactory()
  {
    QueryConfig config = new QueryConfig();
    StupidPool<ByteBuffer> pool = StupidPool.heap(1024 * 1024);

    GroupByQueryEngine engine = new GroupByQueryEngine(pool);
    GroupByQueryQueryToolChest toolchest = new GroupByQueryQueryToolChest(config, engine, pool);
    return new GroupByQueryRunnerFactory(
        engine,
        new StreamQueryEngine(),
        NoopQueryWatcher.instance(),
        config,
        toolchest,
        pool
    );
  }
}
