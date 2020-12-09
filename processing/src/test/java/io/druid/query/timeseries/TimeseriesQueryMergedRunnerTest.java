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

package io.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.Druids;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryMergedRunnerTest
{
  public static final Map<String, Object> CONTEXT = ImmutableMap.of();

  @Parameterized.Parameters(name="{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                new QueryConfig(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private final QueryRunner<Row> runner;

  public TimeseriesQueryMergedRunnerTest(QueryRunner<Row> runner)
  {
    this.runner = runner;
  }

  @Test
  public void testTimeSeriesWithSelectionFilterLookupExtractionFn()
  {
    Map<Object, String> extractionMap = new HashMap<>();
    extractionMap.put("spot","upfront");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, true, null, true, true);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "upfront", lookupExtractionFn)
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    Iterable<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 11L,
                "index", 3783L,
                "addRowsIndexConstant", 3795.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 11L,
                "index", 3313L,
                "addRowsIndexConstant", 3325.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        runner.run(query, CONTEXT)
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
