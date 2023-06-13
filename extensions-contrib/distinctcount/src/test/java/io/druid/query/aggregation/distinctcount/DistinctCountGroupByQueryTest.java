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

package io.druid.query.aggregation.distinctcount;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.collections.BufferPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.VectorizedGroupByQueryEngine;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.select.StreamQueryEngine;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DistinctCountGroupByQueryTest
{
  @Test
  public void testGroupByWithDistinctCountAgg() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final BufferPool pool = BufferPool.heap(1024 * 1024);

    QueryConfig config = new QueryConfig();
    config.getGroupBy().setMaxResults(10000);

    final GroupByQueryEngine engine = new GroupByQueryEngine(pool);
    final VectorizedGroupByQueryEngine batch = new VectorizedGroupByQueryEngine(pool);

    final StreamQueryEngine stream = new StreamQueryEngine();

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        batch,
        stream,
        TestHelper.NOOP_QUERYWATCHER,
        config,
        new GroupByQueryQueryToolChest(
            config, engine, pool
        )
    );

    IncrementalIndex index = new OnheapIncrementalIndex(
        0, QueryGranularities.SECOND, new AggregatorFactory[]{new CountAggregatorFactory("cnt")}, 1000
    );
    String visitor_id = "visitor_id";
    String client_type = "client_type";
    long timestamp = System.currentTimeMillis();
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "0", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 1,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "1", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "2", client_type, "android")
        )
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(Granularities.ALL)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    client_type,
                    client_type
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new LimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        client_type,
                        Direction.DESCENDING
                    )
                ), 10
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new DistinctCountAggregatorFactory("UV", visitor_id, null)
        )
        .build();
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, DataSegment.asKey("test"));

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        factory.createRunner(incrementalIndexSegment, null),
        query.resolveQuery(null, true)    // fudge timestamp
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            client_type, "iphone",
            "UV", 2L,
            "rows", 2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            client_type, "android",
            "UV", 1L,
            "rows", 1L
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "distinct-count");
  }
}
