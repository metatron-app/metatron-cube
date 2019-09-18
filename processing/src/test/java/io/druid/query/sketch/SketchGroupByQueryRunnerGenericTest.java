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

package io.druid.query.sketch;

import com.google.common.collect.ImmutableMap;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SketchGroupByQueryRunnerGenericTest extends SketchQueryRunnerTestHelper
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public SketchGroupByQueryRunnerGenericTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testGroupByLocalSplit()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            Arrays.asList(
                rowsCount,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(allGran)
        .build();

    String[] columnNames = {"__time", "alias", "rows", "idx"};
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        array("2011-04-01T00:00:00.000Z", "automotive", 2L, 282L),
        array("2011-04-01T00:00:00.000Z", "business", 2L, 230L),
        array("2011-04-01T00:00:00.000Z", "entertainment", 2L, 324L),
        array("2011-04-01T00:00:00.000Z", "health", 2L, 233L),
        array("2011-04-01T00:00:00.000Z", "mezzanine", 6L, 5317L),
        array("2011-04-01T00:00:00.000Z", "news", 2L, 235L),
        array("2011-04-01T00:00:00.000Z", "premium", 6L, 5405L),
        array("2011-04-01T00:00:00.000Z", "technology", 2L, 175L),
        array("2011-04-01T00:00:00.000Z", "travel", 2L, 245L)
    );

    List<Row> results = GroupByQueryRunnerTestHelper.runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withOverriddenContext(ImmutableMap.<String, Object>of(Query.GBY_LOCAL_SPLIT_NUM, 3));
    results = GroupByQueryRunnerTestHelper.runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
