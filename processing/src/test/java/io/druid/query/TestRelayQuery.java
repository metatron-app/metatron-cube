/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestRelayQuery extends GroupByQueryRunnerTestHelper
{
  static {
    TestIndex.addIndex(
        "relay",
        Arrays.asList("time", "dim", "m"),
        Arrays.asList("yyyy-MM-dd", "dimension", "double"),
        Granularities.DAY,
        "2019-08-01,a,4\n" +
        "2019-08-01,b,2\n" +
        "2019-08-01,b,1\n" +
        "2019-08-02,a,5\n" +
        "2019-08-02,a,1\n" +
        "2019-08-02,b,3"
    );
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("relay")
        .setDimensions(DefaultDimensionSpec.toSpec("dim"))
        .setAggregatorSpecs(
            RelayAggregatorFactory.timeMin("FIRST(m)", "m"),
            RelayAggregatorFactory.timeMax("LAST(m)", "m")
        )
        .setPostAggregatorSpecs(
            new MathPostAggregator("FIRST_TIME(m)", "datetime(\"FIRST(m)\"[0])", false, null),
            new MathPostAggregator("LAST_TIME(m)", "datetime(\"LAST(m)\"[0])", false, null)
        )
        .setGranularity(Granularities.YEAR)
        .build();
    String[] columnNames = {"__time", "dim", "FIRST_TIME(m)", "FIRST(m)", "LAST_TIME(m)", "LAST(m)"};
    Object[][] objects = {
        array("2019-01-01", "a", new DateTime("2019-08-01"), 4.0, new DateTime("2019-08-02"), 5.0),
        array("2019-01-01", "b", new DateTime("2019-08-01"), 2.0, new DateTime("2019-08-02"), 3.0)
    };
    Iterable<Row> results;
    List<Row> expectedResults;

    results = runQuery(query);
    expectedResults = createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}