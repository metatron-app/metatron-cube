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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecWithOrdering;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Direction;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GroupByQueryTest
{
  private static final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(new FieldAccessPostAggregator("x", "idx")))
        .setLimitSpec(
            new LimitSpec(
                ImmutableList.of(OrderByColumnSpec.asc("alias")),
                100
            )
        )
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testPreOrdering() throws IOException
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(DefaultDimensionSpec.of("quality", "q"), DefaultDimensionSpec.of("market", "m"))
        .setAggregatorSpecs(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setPostAggregatorSpecs(ImmutableList.<PostAggregator>of(new FieldAccessPostAggregator("x", "idx")))
        .setLimitSpec(
            new LimitSpec(
                ImmutableList.of(OrderByColumnSpec.desc("m")),
                100
            )
        )
        .setContext(ImmutableMap.<String, Object>of(Query.GBY_PRE_ORDERING, true))
        .build();

    GroupByQuery rewritten = (GroupByQuery) query.rewriteQuery(null, new QueryConfig(), null);

    List<DimensionSpec> dimensions = rewritten.getDimensions();
    Assert.assertEquals(2, dimensions.size());
    Assert.assertEquals(
        new DimensionSpecWithOrdering(DefaultDimensionSpec.of("market", "m"), Direction.DESCENDING, null),
        dimensions.get(0)
    );
    Assert.assertEquals(DefaultDimensionSpec.of("quality", "q"), dimensions.get(1));
    Assert.assertTrue(GuavaUtils.isNullOrEmpty(rewritten.getLimitSpec().getColumns()));
    Assert.assertEquals(100, rewritten.getLimitSpec().getLimit());

    query = query.withLimitSpec(
        new LimitSpec(
            ImmutableList.of(OrderByColumnSpec.desc("m")), 100,
            Arrays.asList(
                new WindowingSpec(
                    Arrays.asList("q"),
                    Arrays.asList(OrderByColumnSpec.asc("m", "day")),
                    "sum.x = $sum(x)"
                )
            )
        )
    );

    rewritten = (GroupByQuery) query.rewriteQuery(null, new QueryConfig(), null);

    dimensions = rewritten.getDimensions();
    Assert.assertEquals(2, dimensions.size());
    Assert.assertEquals(dimensions.get(0), DefaultDimensionSpec.of("quality", "q"));
    Assert.assertEquals(
        new DimensionSpecWithOrdering(DefaultDimensionSpec.of("market", "m"), Direction.ASCENDING, "day"),
        dimensions.get(1)
    );
    WindowingSpec windowingSpec = rewritten.getLimitSpec().getWindowingSpecs().get(0);
    Assert.assertTrue(GuavaUtils.isNullOrEmpty(windowingSpec.getSortingColumns()));
    Assert.assertEquals(Arrays.asList(OrderByColumnSpec.desc("m")), rewritten.getLimitSpec().getColumns());
    Assert.assertEquals(100, rewritten.getLimitSpec().getLimit());
  }
}
