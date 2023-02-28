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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.DateTimes;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.Druids;
import io.druid.query.PostAggregationsPostProcessor;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.model.HoltWintersPostProcessor;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerTest
{
  public static final Map<String, Object> CONTEXT = ImmutableMap.of();

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            Arrays.<Set<Object>>asList(
                Sets.<Object>newHashSet(TestIndex.DS_NAMES),
                Sets.<Object>newHashSet(false, true)
            )
        ), new Function<List<Object>, Object[]>()
        {
          @Override
          public Object[] apply(List<Object> input)
          {
            return input.toArray();
          }
        }
    );
  }

  private final String dataSource;
  private final boolean descending;

  public TimeseriesQueryRunnerTest(String dataSource, boolean descending)
  {
    this.dataSource = dataSource;
    this.descending = descending;
  }


  private <T> void assertExpectedResults(Iterable<Row> expectedResults, Iterable<Row> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testFullOnTimeseries()
  {
    Granularity gran = QueryGranularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexDoubleSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> results = Sequences.toList(query.run(TestIndex.segmentWalker, CONTEXT));

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.expectedFullOnIndexValuesDesc :
                                   QueryRunnerTestHelper.expectedFullOnIndexValues;

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    int count = 0;
    Row lastResult = null;
    for (Row result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(current) ? 0 : 13,
          result.getLongMetric("rows")
      );
      Assert.assertEquals(
          result.toString(),
          expectedIndex[count],
          String.valueOf(result.getDoubleMetric("index"))
      );
      Assert.assertEquals(
          result.toString(),
          new Double(expectedIndex[count]) +
          (QueryRunnerTestHelper.skippedDay.equals(current) ? 0 : 13) + 1,
          result.getDoubleMetric("addRowsIndexConstant"),
          0.0
      );
      Assert.assertEquals(
          result.getDoubleMetric("uniques"),
          QueryRunnerTestHelper.skippedDay.equals(current) ? 0.0d : 9.0d,
          0.02
      );

      lastResult = result;
      ++count;
    }

    Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
  }

  @Test
  public void testFullOnTimeseriesMaxMin()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryGranularities.ALL)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                      new DoubleMinAggregatorFactory("minIndex", "index")
                                  )
                                  .descending(descending)
                                  .build();

    Row result = Sequences.only(query.run(TestIndex.segmentWalker, CONTEXT));

    Assert.assertEquals(DateTimes.EPOCH, result.getTimestamp());
    Assert.assertEquals(result.toString(), 1870.06103515625, result.getDoubleMetric("maxIndex"), 0.0);
    Assert.assertEquals(result.toString(), 59.02102279663086, result.getDoubleMetric("minIndex"), 0.0);
  }

  @Test
  public void testFullOnTimeseriesWithFilter()
  {

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    Assert.assertEquals(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("upfront")
              .build(),
        query.getFilter()
    );

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    for (Row result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0 : 2,
          result.getLongMetric("rows")
      );
      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0.0d : 2.0d,
          result.getDoubleMetric("uniques"),
          0.01
      );
    }
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 6619, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testNestedTimeseries()
  {
    TimeseriesQuery subQuery = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.DAY)
        .intervals(QueryRunnerTestHelper.january_20)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(new MathPostAggregator("idx+10", "idx + 10")))
        .descending(descending)
        .build();

    List<Row> expected = GroupByQueryRunnerTest.createExpectedRows(
        new String[]{"__time", "rows", "idx", "idx+10", "uniques"},
        new Object[]{"2011-01-12", 13, 4500, 4510L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-13", 13, 6071, 6081L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-14", 13, 4916, 4926L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-15", 13, 5719, 5729L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-16", 13, 4692, 4702L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-17", 13, 4644, 4654L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-18", 13, 4392, 4402L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-19", 13, 4589, 4599L, QueryRunnerTestHelper.UNIQUES_9}
    );

    Iterable<Row> results = Sequences.toList(
        subQuery.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expected, results);

    // do post aggregation for inner query
    subQuery = subQuery.withOverriddenContext(Query.FINALIZE, null);

    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(new QueryDataSource(subQuery))
        .granularity(QueryGranularities.WEEK)
        .intervals(QueryRunnerTestHelper.january_20)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx+10")
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(new MathPostAggregator("(idx+10)+10", "\"idx+10\" + 10")))
        .descending(descending)
        .build();

    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    expected = GroupByQueryRunnerTest.createExpectedRows(
        new String[]{"__time", "rows", "idx+10", "(idx+10)+10"},
        new Object[]{"2011-01-10", 5L, 25948L, 25958L},
        new Object[]{"2011-01-17", 3L, 13655L, 13665L}
    );
    assertExpectedResults(expected, results);
  }

  @Test
  public void testHoltWinters()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(dataSource)
        .granularity(Granularities.DAY)
        .intervals(QueryRunnerTestHelper.january_20)
        .aggregators(new LongSumAggregatorFactory("idx", "index"))
        .context(
            ImmutableMap.<String, Object>of(
                Query.POST_PROCESSING, HoltWintersPostProcessor.of(3, "idx")
            )
        )
        .build();

    String[] columnNames = {Column.TIME_COLUMN_NAME, "idx", "idx.params"};
    List<Row> expected = GroupByQueryRunnerTestHelper.createExpectedRows(
        columnNames,
        new Object[]{"2011-01-12", 4500, null},
        new Object[]{"2011-01-13", 6071, null},
        new Object[]{"2011-01-14", 4916, null},
        new Object[]{"2011-01-15", 5719, null},
        new Object[]{"2011-01-16", 4692, null},
        new Object[]{"2011-01-17", 4644, null},
        new Object[]{"2011-01-18", 4392, null},
        new Object[]{"2011-01-19", 4589, null},
        new Object[]{
            "2011-01-20",
            new double[]{2719.4776163876427D, 5227.333335325231D, 7735.189054262819D},
            new double[]{1.0D, 0.13930361783807885D, 0.40495181947475867D}
        },
        new Object[]{
            "2011-01-21",
            new double[]{1636.562384787635D, 5611.4196565228085D, 9586.276928257983D},
            new double[]{1.0D, 0.13930361783807885D, 0.40495181947475867D}
        },
        new Object[]{
            "2011-01-22",
            new double[]{979.7945398452102D, 5995.505977720387D, 11011.217415595564D},
            new double[]{1.0D, 0.13930361783807885D, 0.40495181947475867D}
        }
    );

    Iterable<Row> results = Sequences.toList(
        ((Query) query).run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expected, results, "holt-winters");
  }

  @Test
  public void testTimeseriesWithTimeZone()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .intervals("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                  .virtualColumns(
                                      Arrays.<VirtualColumn>asList(
                                          new ExprVirtualColumn("index + 10", "index2")
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          new LongSumAggregatorFactory(
                                              "idx2",
                                              "index2"
                                          )
                                      )
                                  )
                                  .granularity(
                                      new PeriodGranularity(
                                          new Period("P1D"),
                                          null,
                                          DateTimeZone.forID("America/Los_Angeles")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-03-31", DateTimeZone.forID("America/Los_Angeles")),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 6619, "idx2", 6619 + 130)
        ),
        new MapBasedRow(
            new DateTime("2011-04-01T", DateTimeZone.forID("America/Los_Angeles")),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "idx2", 5827 + 130)
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVaryingGran()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Row> expectedResults1 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );

    Iterable<Row> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .granularity("DAY")
                                   .intervals(new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .build();

    List<Row> expectedResults2 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );

    Iterable<Row> results2 = Sequences.toList(
        query2.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesGranularityNotAlignedOnSegmentBoundariesWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(
                                       new PeriodGranularity(
                                           new Period("P7D"),
                                           null,
                                           DateTimeZone.forID("America/Los_Angeles")
                                       )
                                   )
                                   .intervals(
                                       new Interval("2011-01-12T00:00:00.000-08:00/2011-01-20T00:00:00.000-08:00")
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Row> expectedResults1 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-01-06T00:00:00.000-08:00", DateTimeZone.forID("America/Los_Angeles")),
                ImmutableMap.<String, Object>of("rows", 13, "idx", 6071)
        ),
        new MapBasedRow(
            new DateTime("2011-01-13T00:00:00.000-08:00", DateTimeZone.forID("America/Los_Angeles")),
                ImmutableMap.<String, Object>of("rows", 91, "idx", 33382)
        )
    );

    Iterable<Row> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryZeroFilling()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(QueryGranularities.HOUR)
                                   .intervals(
                                       new Interval("2011-04-14T00:00:00.000Z/2011-05-01T00:00:00.000Z")
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    final Iterable<Interval> iterable = QueryGranularities.HOUR.getIterable(
        new Interval(DateTimes.of("2011-04-14T01"), DateTimes.of("2011-04-15"))
    );
    List<Row> expectedResults1 = Lists.newArrayList(
        Iterables.concat(
            Arrays.<Row>asList(
                new MapBasedRow(
                    new DateTime("2011-04-14T00"),
                    ImmutableMap.<String, Object>of("rows", 13, "idx", 4907)
                )
            ),
            Arrays.<Row>asList(
                new MapBasedRow(
                    new DateTime("2011-04-15T00"),
                    ImmutableMap.<String, Object>of("rows", 13, "idx", 4717)
                )
            )
        )
    );

    Iterable<Row> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryGranularityNotAlignedWithRollupGranularity()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(
                                       new PeriodGranularity(
                                           new Period("PT1H"),
                                           new DateTime(60000),
                                           DateTimeZone.UTC
                                       )
                                   )
                                   .intervals(new Interval("2011-04-15T00:00:00.000Z/2012"))
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Row> expectedResults1 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-14T23:01Z"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 4717)
        )
    );

    Iterable<Row> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesWithVaryingGranWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Row> expectedResults1 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );
    Iterable<Row> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity("DAY")
                                   .intervals(new Interval("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .build();

    List<Row> expectedResults2 = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of("rows", 13, "idx", 5827, "uniques", QueryRunnerTestHelper.UNIQUES_9)
        )
    );

    Iterable<Row> results2 = Sequences.toList(
        query2.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesQueryBeyondTimeRangeOfData()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .intervals(new Interval("2015-01-01/2015-01-10"))
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList();

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOrFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 6619,
                "addRowsIndexConstant", 6633.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 5827,
                "addRowsIndexConstant", 5841.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithRegexFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
        .filters(new RegexDimFilter(QueryRunnerTestHelper.marketDimension, "^.p.*$", null)) // spot and upfront
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexLongSum,
                QueryRunnerTestHelper.qualityUniques
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .descending(descending)
        .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 11,
                "index", 3783,
                "addRowsIndexConstant", 3795.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 11,
                "index", 3313,
                "addRowsIndexConstant", 3325.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "spot")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 9,
                "index", 1102,
                "addRowsIndexConstant", 1112.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 9,
                "index", 1120,
                "addRowsIndexConstant", 1130.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 2681,
                "addRowsIndexConstant", 2684.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 2193,
                "addRowsIndexConstant", 2196.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter3()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 2836,
                "addRowsIndexConstant", 2839.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 2514,
                "addRowsIndexConstant", 2517.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilterAndOr()
  {
    DimFilter andDimFilter = DimFilters.and(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("spot")
              .build(),
        DimFilters.or(
            new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null),
            new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "business", null)
        )
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 254.4554443359375D,
                "addRowsIndexConstant", 257.4554443359375D,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 2,
                "index", 260.4129638671875D,
                "addRowsIndexConstant", 263.4129638671875D,
                "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilter()
  {
    DimFilter andDimFilter = DimFilters.and(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("spot")
              .build(),
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("automotive")
              .build()
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .limit(3)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(135.885094).doubleValue(),
                "addRowsIndexConstant", new Float(137.885094).doubleValue(),
                "uniques", QueryRunnerTestHelper.UNIQUES_1
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(147.425935).doubleValue(),
                "addRowsIndexConstant", new Float(149.425935).doubleValue(),
                "uniques", QueryRunnerTestHelper.UNIQUES_1
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);

    query = query.withOutputColumns(Arrays.asList("rows", "index"));
    // with projection processor

    expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(135.885094).doubleValue()
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(147.425935).doubleValue()
            )
        )
    );

    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);

    // post-aggregations post processor
    query = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(
            Query.POST_PROCESSING,
            new PostAggregationsPostProcessor(
                Arrays.<PostAggregator>asList(
                    new MathPostAggregator("daily = time_format(__time,'MMM dd','UTC','en')")
                )
            )
        )
    );

    String[] columnNames = new String[] {"__time", "rows", "index", "daily"};
    query = query.withOutputColumns(Arrays.asList("rows", "index", "daily"));
    if (descending) {
      expectedResults = GroupByQueryRunnerTest.createExpectedRows(
          columnNames,
          new Object[]{"2011-04-02", 1, 147.42593383789062d, "Apr 02"},
          new Object[]{"2011-04-01", 1, 135.88510131835938d, "Apr 01"}
      );
    } else {
      expectedResults = GroupByQueryRunnerTest.createExpectedRows(
          columnNames,
          new Object[]{"2011-04-01", 1, 135.88510131835938d, "Apr 01"},
          new Object[]{"2011-04-02", 1, 147.42593383789062d, "Apr 02"}
      );
    }

    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testTimeseriesWithOtherMultiDimFilter()
  {
    DimFilter andDimFilter = DimFilters.and(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("spot")
              .build(),
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("business")
              .build()
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(118.570340).doubleValue(),
                "addRowsIndexConstant", new Float(120.570340).doubleValue(),
                "uniques", QueryRunnerTestHelper.UNIQUES_1
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 1,
                "index", new Float(112.987027).doubleValue(),
                "addRowsIndexConstant", new Float(114.987027).doubleValue(),
                "uniques", QueryRunnerTestHelper.UNIQUES_1
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterInOr()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(
                                      QueryRunnerTestHelper.marketDimension,
                                      "spot",
                                      "upfront",
                                      "total_market",
                                      "billyblank"
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
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 6619,
                "addRowsIndexConstant", 6633.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 5827,
                "addRowsIndexConstant", 5841.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }


  @Test
  public void testTimeseriesWithInFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(
                                      new InDimFilter(
                                          QueryRunnerTestHelper.marketDimension,
                                          Arrays.asList(
                                              "spot",
                                              "upfront",
                                              "total_market",
                                              "billyblank"
                                          ),
                                          null
                                      )
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
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 6619,
                "addRowsIndexConstant", 6633.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
            ImmutableMap.<String, Object>of(
                "rows", 13,
                "index", 5827,
                "addRowsIndexConstant", 5841.0,
                "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDimAndOr()
  {
    DimFilter andDimFilter = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "spot"),
        SelectorDimFilter.or(QueryRunnerTestHelper.qualityDimension, "automotive", "business", "billyblank")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "rows", 2,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
                ImmutableMap.<String, Object>of(
                    "rows", 2,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList();

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimensionSkipBuckets()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList();

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters("bobby", null)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "rows", 13,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
                ImmutableMap.<String, Object>of(
                    "rows", 13,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithInvertedFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(new NotDimFilter(new SelectorDimFilter("bobby", "sally", null)))
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "rows", 13,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
                ImmutableMap.<String, Object>of(
                    "rows", 13,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.marketDimension, "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList();

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    DimFilter andDimFilter = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "billy"),
        SelectorDimFilter.of(QueryRunnerTestHelper.qualityDimension, "business")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList();

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          QueryRunnerTestHelper.jsIndexSumIfPlacementishA,
                                          QueryRunnerTestHelper.jsPlacementishCount
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    Iterable<Row> expectedResults = ImmutableList.<Row>of(
        new MapBasedRow(
            new DateTime(QueryRunnerTestHelper.firstToThird.getIntervals().get(0).getStart()),
            ImmutableMap.<String, Object>of(
                "index", 12459.361190795898d,
                "nindex", 283.31103515625d,
                "pishcount", 52d
            )
        )
    );

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregatorAndAlsoRegularFilters()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "a")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          QueryRunnerTestHelper.jsIndexSumIfPlacementishA,
                                          QueryRunnerTestHelper.jsPlacementishCount
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = ImmutableList.<Row>of(
        new MapBasedRow(
            new DateTime(QueryRunnerTestHelper.firstToThird.getIntervals().get(0).getStart()),
            ImmutableMap.<String, Object>of(
                "index", 283.31103515625d,
                "nindex", 283.31103515625d,
                "pishcount", 4d
            )
        )
    );

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(Granularities.DAY)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "a")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(Granularities.DAY)
              .filters(QueryRunnerTestHelper.qualityDimension, "automotive")
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr1()
  {
    DimFilter andDimFilter = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "spot"),
        SelectorDimFilter.of(QueryRunnerTestHelper.placementishDimension, "a")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    DimFilter andDimFilter2 = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "spot"),
        SelectorDimFilter.of(QueryRunnerTestHelper.qualityDimension, "automotive")
    );

    Iterable<Row> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(Granularities.DAY)
              .filters(andDimFilter2)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr2()
  {
    DimFilter andDimFilter = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "spot"),
        SelectorDimFilter.or(QueryRunnerTestHelper.placementishDimension, "a", "b")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    DimFilter andDimFilter2 = DimFilters.and(
        SelectorDimFilter.of(QueryRunnerTestHelper.marketDimension, "spot"),
        SelectorDimFilter.or(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
    );

    Iterable<Row> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(Granularities.DAY)
              .filters(andDimFilter2)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAgg()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              QueryRunnerTestHelper.commonAggregators,
                                              Lists.newArrayList(
                                                  new FilteredAggregatorFactory(
                                                      new CountAggregatorFactory("filteredAgg"),
                                                      Druids.newSelectorDimFilterBuilder()
                                                            .dimension(QueryRunnerTestHelper.marketDimension)
                                                            .value("spot")
                                                            .build()
                                                  )
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 18,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggDimensionNotPresentNotNullValue()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              QueryRunnerTestHelper.commonAggregators,
                                              Lists.newArrayList(
                                                  new FilteredAggregatorFactory(
                                                      new CountAggregatorFactory("filteredAgg"),
                                                      Druids.newSelectorDimFilterBuilder()
                                                            .dimension("abraKaDabra")
                                                            .value("Lol")
                                                            .build()
                                                  )
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 0,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggDimensionNotPresentNullValue()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              QueryRunnerTestHelper.commonAggregators,
                                              Lists.newArrayList(
                                                  new FilteredAggregatorFactory(
                                                      new CountAggregatorFactory("filteredAgg"),
                                                      Druids.newSelectorDimFilterBuilder()
                                                            .dimension("abraKaDabra")
                                                            .value(null)
                                                            .build()
                                                  )
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26
                )
            )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggValueNotPresent()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              QueryRunnerTestHelper.commonAggregators,
                                              Lists.newArrayList(
                                                  new FilteredAggregatorFactory(
                                                      new CountAggregatorFactory("filteredAgg"),
                                                      new NotDimFilter(
                                                          Druids.newSelectorDimFilterBuilder()
                                                                .dimension(QueryRunnerTestHelper.marketDimension)
                                                                .value("LolLol")
                                                                .build()
                                                      )
                                                  )
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggInvertedNullValue()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              QueryRunnerTestHelper.commonAggregators,
                                              Lists.newArrayList(
                                                  new FilteredAggregatorFactory(
                                                      new CountAggregatorFactory("filteredAgg"),
                                                      new NotDimFilter(
                                                          Druids.newSelectorDimFilterBuilder()
                                                                .dimension(QueryRunnerTestHelper.marketDimension)
                                                                .value(null)
                                                                .build()
                                                      )
                                                  )
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26
                )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithTimeColumn()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                                      QueryRunnerTestHelper.__timeLongSum
                                  )
                                  .granularity(Granularities.ALL)
                                  .descending(descending)
                                  .build();

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "rows",
                    26,
                    "ntimestamps",
                    13.0,
                    "sumtime",
                    33843139200000L
            )
        )
    );

    Iterable<Row> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithBoundFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(Granularities.DAY)
                                  .filters(
                                      new AndDimFilter(
                                          Arrays.asList(
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "spa",
                                                  "spot",
                                                  true,
                                                  false,
                                                  false,
                                                  null
                                              ),
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "spot",
                                                  "spotify",
                                                  false,
                                                  true,
                                                  false,
                                                  null
                                              ),
                                              (DimFilter) new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "SPOT",
                                                  "spot",
                                                  false,
                                                  false,
                                                  false,
                                                  null
                                              )
                                          )
                                      )
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

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime("2011-04-01"),
                ImmutableMap.<String, Object>of(
                    "rows", 9,
                    "index", 1102,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
            )
        ),
        new MapBasedRow(
            new DateTime("2011-04-02"),
                ImmutableMap.<String, Object>of(
                    "rows", 9,
                    "index", 1120,
                    "addRowsIndexConstant", 1130.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
        )
    );

    Iterable<Row> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testSimpleLimit()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryGranularities.MONTH)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                      new DoubleMinAggregatorFactory("minIndex", "index")
                                  )
                                  .descending(descending)
                                  .build();

    List<Row> expected = GroupByQueryRunnerTest.createExpectedRows(
        new String[]{"__time", "maxIndex", "minIndex"},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
    );
    if (descending) {
      Collections.reverse(expected);
    }
    for (int i = 4; i > 0; i--) {
      List<Row> results = Sequences.toList(
          query.withLimitSpec(new LimitSpec(null, i)).run(TestIndex.segmentWalker, CONTEXT),
          Lists.<Row>newArrayList()
      );
      TestHelper.assertExpectedObjects(expected.subList(0, i), results);
    }

    expected = GroupByQueryRunnerTest.createExpectedRows(
        new String[]{"__time", "maxIndex", "minIndex"},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
    );
    LimitSpec spec = LimitSpecs.of(null, OrderByColumnSpec.asc("minIndex"));
    List<Row> results = Sequences.toList(
        query.withLimitSpec(spec).run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expected, results);

    expected = GroupByQueryRunnerTest.createExpectedRows(
        new String[]{"__time", "maxIndex", "minIndex"},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
        );
    spec = LimitSpecs.of(null, OrderByColumnSpec.desc("maxIndex"));
    results = Sequences.toList(
        query.withLimitSpec(spec).run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expected, results);
  }

  @Test
  public void testWindowingSpec()
  {
    BaseAggregationQuery.Builder<TimeseriesQuery> builder = Druids.newTimeseriesQueryBuilder()
        .setDataSource(dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.january)
        .setDimFilter(new MathExprFilter("index > 100"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(QueryRunnerTestHelper.addRowsIndexConstant)
        .setGranularity(QueryGranularities.DAY);

    String[] columnNames = {"__time", "rows", "index", "addRowsIndexConstant"};
    Iterable<Row> results;

    List<Row> expectedResults = GroupByQueryRunnerTest.createExpectedRows(
        columnNames,
        new Object[]{"2011-01-12",  4, 3600.0, 3605.0},
        new Object[]{"2011-01-13", 12, 5983.074401855469, 5996.074401855469},
        new Object[]{"2011-01-14", 10, 4644.134963989258, 4655.134963989258},
        new Object[]{"2011-01-15",  9, 5356.181037902832, 5366.181037902832},
        new Object[]{"2011-01-16",  8, 4247.914710998535, 4256.914710998535},
        new Object[]{"2011-01-17", 11, 4461.939956665039, 4473.939956665039},
        new Object[]{"2011-01-18", 10, 4127.733467102051, 4138.733467102051},
        new Object[]{"2011-01-19",  9, 4243.135475158691, 4253.135475158691},
        new Object[]{"2011-01-20",  9, 4076.9225158691406, 4086.9225158691406},
        new Object[]{"2011-01-22", 10, 5874.600471496582, 5885.600471496582},
        new Object[]{"2011-01-23", 11, 5400.676559448242, 5412.676559448242},
        new Object[]{"2011-01-24", 10, 4710.972122192383, 4721.972122192383},
        new Object[]{"2011-01-25", 10, 4906.999092102051, 4917.999092102051},
        new Object[]{"2011-01-26", 10, 6022.893226623535, 6033.893226623535},
        new Object[]{"2011-01-27", 12, 5934.857353210449, 5947.857353210449},
        new Object[]{"2011-01-28", 11, 5579.304672241211, 5591.304672241211},
        new Object[]{"2011-01-29", 13, 5346.517524719238, 5360.517524719238},
        new Object[]{"2011-01-30", 12, 5400.307342529297, 5413.307342529297},
        new Object[]{"2011-01-31", 11, 5727.540992736816, 5739.540992736816}
    );

    TimeseriesQuery query = builder.build();
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = GroupByQueryRunnerTest.createExpectedRows(
        columnNames,
        new Object[]{"2011-01-29", 13, 5346.517524719238, 5360.517524719238},
        new Object[]{"2011-01-30", 12, 5400.307342529297, 5413.307342529297},
        new Object[]{"2011-01-27", 12, 5934.857353210449, 5947.857353210449},
        new Object[]{"2011-01-13", 12, 5983.074401855469, 5996.074401855469},
        new Object[]{"2011-01-17", 11, 4461.939956665039, 4473.939956665039},
        new Object[]{"2011-01-23", 11, 5400.676559448242, 5412.676559448242},
        new Object[]{"2011-01-28", 11, 5579.304672241211, 5591.304672241211},
        new Object[]{"2011-01-31", 11, 5727.540992736816, 5739.540992736816},
        new Object[]{"2011-01-18", 10, 4127.733467102051, 4138.733467102051},
        new Object[]{"2011-01-14", 10, 4644.134963989258, 4655.134963989258}
    );

    query = query.withLimitSpec(
        new LimitSpec(Arrays.asList(OrderByColumnSpec.desc("rows"), OrderByColumnSpec.asc("index")), 10)
    );
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withLimitSpec(
        new LimitSpec(
            null, null,
            Arrays.asList(
                WindowingSpec.expressions("delta = $delta(index)", "runningSum = $sum(rows)")
            )
        )
    );

    columnNames = new String[]{"__time", "rows", "index", "addRowsIndexConstant", "delta", "runningSum"};
    expectedResults = GroupByQueryRunnerTest.createExpectedRows(
        columnNames,
        new Object[]{"2011-01-12", 4, 3600.0, 3605.0, 0.0, 4L},
        new Object[]{"2011-01-13", 12, 5983.074401855469, 5996.074401855469, 2383.0744018554688, 16L},
        new Object[]{"2011-01-14", 10, 4644.134963989258, 4655.134963989258, -1338.939437866211, 26L},
        new Object[]{"2011-01-15", 9, 5356.181037902832, 5366.181037902832, 712.0460739135742, 35L},
        new Object[]{"2011-01-16", 8, 4247.914710998535, 4256.914710998535, -1108.2663269042969, 43L},
        new Object[]{"2011-01-17", 11, 4461.939956665039, 4473.939956665039, 214.0252456665039, 54L},
        new Object[]{"2011-01-18", 10, 4127.733467102051, 4138.733467102051, -334.2064895629883, 64L},
        new Object[]{"2011-01-19", 9, 4243.135475158691, 4253.135475158691, 115.40200805664062, 73L},
        new Object[]{"2011-01-20", 9, 4076.9225158691406, 4086.9225158691406, -166.21295928955078, 82L},
        new Object[]{"2011-01-22", 10, 5874.600471496582, 5885.600471496582, 1797.6779556274414, 92L},
        new Object[]{"2011-01-23", 11, 5400.676559448242, 5412.676559448242, -473.92391204833984, 103L},
        new Object[]{"2011-01-24", 10, 4710.972122192383, 4721.972122192383, -689.7044372558594, 113L},
        new Object[]{"2011-01-25", 10, 4906.999092102051, 4917.999092102051, 196.02696990966797, 123L},
        new Object[]{"2011-01-26", 10, 6022.893226623535, 6033.893226623535, 1115.8941345214844, 133L},
        new Object[]{"2011-01-27", 12, 5934.857353210449, 5947.857353210449, -88.03587341308594, 145L},
        new Object[]{"2011-01-28", 11, 5579.304672241211, 5591.304672241211, -355.5526809692383, 156L},
        new Object[]{"2011-01-29", 13, 5346.517524719238, 5360.517524719238, -232.78714752197266, 169L},
        new Object[]{"2011-01-30", 12, 5400.307342529297, 5413.307342529297, 53.789817810058594, 181L},
        new Object[]{"2011-01-31", 11, 5727.540992736816, 5739.540992736816, 327.23365020751953, 192L}
    );
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Row>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
