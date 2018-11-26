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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequences;
import io.druid.common.DateTimes;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.Druids;
import io.druid.query.PostAggregationsPostProcessor;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
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
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.LimitSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
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


  private <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedResults(expectedResults, results);
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
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.expectedFullOnIndexValuesDesc :
                                   QueryRunnerTestHelper.expectedFullOnIndexValues;

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    int count = 0;
    Result lastResult = null;
    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(current) ? 0L : 13L,
          value.getLongMetric("rows").longValue()
      );
      Assert.assertEquals(
          result.toString(),
          expectedIndex[count],
          String.valueOf(value.getDoubleMetric("index"))
      );
      Assert.assertEquals(
          result.toString(),
          new Double(expectedIndex[count]) +
          (QueryRunnerTestHelper.skippedDay.equals(current) ? 0L : 13L) + 1L,
          value.getDoubleMetric("addRowsIndexConstant"),
          0.0
      );
      Assert.assertEquals(
          value.getDoubleMetric("uniques"),
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
                                      Arrays.asList(
                                          new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          new DoubleMinAggregatorFactory("minIndex", "index")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    DateTime expectedEarliest = new DateTime("2011-01-12");
    DateTime expectedLast = new DateTime("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assert.assertEquals(expectedEarliest, result.getTimestamp());
    Assert.assertFalse(
        String.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
        result.getTimestamp().isAfter(expectedLast)
    );

    final TimeseriesResultValue value = result.getValue();

    Assert.assertEquals(result.toString(), 1870.06103515625, value.getDoubleMetric("maxIndex"), 0.0);
    Assert.assertEquals(result.toString(), 59.02102279663086, value.getDoubleMetric("minIndex"), 0.0);
  }

  @Test
  public void testFullOnTimeseriesWithFilter()
  {

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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
        query.getDimFilter()
    );

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0L : 2L,
          value.getLongMetric("rows").longValue()
      );
      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0.0d : 2.0d,
          value.getDoubleMetric(
              "uniques"
          ),
          0.01
      );
    }
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testNestedTimeseries()
  {
    TimeseriesQuery subQuery = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expected = TimeseriesQueryRunnerTestHelper.createExpected(
        new String[]{"rows", "idx", "idx+10", "uniques"},
        new Object[]{"2011-01-12", 13L, 4500L, 4510L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-13", 13L, 6071L, 6081L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-14", 13L, 4916L, 4926L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-15", 13L, 5719L, 5729L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-16", 13L, 4692L, 4702L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-17", 13L, 4644L, 4654L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-18", 13L, 4392L, 4402L, QueryRunnerTestHelper.UNIQUES_9},
        new Object[]{"2011-01-19", 13L, 4589L, 4599L, QueryRunnerTestHelper.UNIQUES_9}
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        subQuery.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expected, results);

    // do post aggregation for inner query
    subQuery = subQuery.withOverriddenContext(ImmutableMap.<String, Object>of(QueryContextKeys.FINAL_MERGE, false));

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
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    expected = TimeseriesQueryRunnerTestHelper.createExpected(
        new String[]{"rows", "idx+10", "(idx+10)+10"},
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
        .granularity(QueryRunnerTestHelper.dayGran)
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
        new Object[]{"2011-01-12", 4500L, null},
        new Object[]{"2011-01-13", 6071L, null},
        new Object[]{"2011-01-14", 4916L, null},
        new Object[]{"2011-01-15", 5719L, null},
        new Object[]{"2011-01-16", 4692L, null},
        new Object[]{"2011-01-17", 4644L, null},
        new Object[]{"2011-01-18", 4392L, null},
        new Object[]{"2011-01-19", 4589L, null},
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-03-31", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 6619L, "idx2", 6619L + 130L)
            )
        ),
        new Result<>(
            new DateTime("2011-04-01T", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "idx2", 5827L + 130L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVaryingGran()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
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
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .granularity("DAY")
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
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

    List<Result<TimeseriesResultValue>> expectedResults2 = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = Sequences.toList(
        query2.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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
                                       Arrays.asList(
                                           new Interval(
                                               "2011-01-12T00:00:00.000-08:00/2011-01-20T00:00:00.000-08:00"
                                           )
                                       )
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

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-06T00:00:00.000-08:00", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 6071L)
            )
        ),
        new Result<>(
            new DateTime("2011-01-13T00:00:00.000-08:00", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 91L, "idx", 33382L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-14T00:00:00.000Z/2011-05-01T00:00:00.000Z"
                                           )
                                       )
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

    List<Result<TimeseriesResultValue>> lotsOfZeroes = Lists.newArrayList();
    final Iterable<Interval> iterable = QueryGranularities.HOUR.getIterable(
        new Interval(DateTimes.of("2011-04-14T01"), DateTimes.of("2011-04-15"))
    );
    for (Interval interval : iterable) {
      lotsOfZeroes.add(
              new Result<>(
                      interval.getStart(),
                      new TimeseriesResultValue(
                              ImmutableMap.<String, Object>of("rows", 0L, "idx", 0L)
                      )
              )
      );
    }
    List<Result<TimeseriesResultValue>> expectedResults1 = Lists.newArrayList(
        Iterables.concat(
            Arrays.asList(
                new Result<>(
                    new DateTime("2011-04-14T00"),
                    new TimeseriesResultValue(
                        ImmutableMap.<String, Object>of("rows", 13L, "idx", 4907L)
                    )
                )
            ),
            lotsOfZeroes,
            Arrays.asList(
                new Result<>(
                    new DateTime("2011-04-15T00"),
                    new TimeseriesResultValue(
                        ImmutableMap.<String, Object>of("rows", 13L, "idx", 4717L)
                    )
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-15T00:00:00.000Z/2012"
                                           )
                                       )
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

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-14T23:01Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 4717L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
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
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        query1.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity("DAY")
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
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

    List<Result<TimeseriesResultValue>> expectedResults2 = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = Sequences.toList(
        query2.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesQueryBeyondTimeRangeOfData()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(
                                      new MultipleIntervalSegmentSpec(
                                          Arrays.asList(
                                              new Interval(
                                                  "2015-01-01/2015-01-10"
                                              )
                                          )
                                      )
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOrFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithRegexFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 11L,
                    "index", 3783L,
                    "addRowsIndexConstant", 3795.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 11L,
                    "index", 3313L,
                    "addRowsIndexConstant", 3325.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1120L,
                    "addRowsIndexConstant", 1130.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2681L,
                    "addRowsIndexConstant", 2684.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2193L,
                    "addRowsIndexConstant", 2196.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter3()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2836L,
                    "addRowsIndexConstant", 2839.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2514L,
                    "addRowsIndexConstant", 2517.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilterAndOr()
  {
    AndDimFilter andDimFilter = Druids
        .newAndDimFilterBuilder()
        .fields(
            Arrays.<DimFilter>asList(
                Druids.newSelectorDimFilterBuilder()
                      .dimension(QueryRunnerTestHelper.marketDimension)
                      .value("spot")
                      .build(),
                Druids.newOrDimFilterBuilder()
                      .fields(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
                      .build()
            )
        )
        .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilter()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("automotive")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .limit(3)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(135.885094).doubleValue(),
                    "addRowsIndexConstant", new Float(137.885094).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(147.425935).doubleValue(),
                    "addRowsIndexConstant", new Float(149.425935).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);

    query = query.withOutputColumns(Arrays.asList("rows", "index"));
    // with projection processor

    expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(135.885094).doubleValue()
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(147.425935).doubleValue()
                )
            )
        )
    );

    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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

    String[] columnNames = new String[] {"rows", "index", "daily"};
    query = query.withOutputColumns(Arrays.asList("rows", "index", "daily"));
    if (descending) {
      expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
          columnNames,
          new Object[]{"2011-04-02", 1L, 147.42593383789062d, "Apr 02"},
          new Object[]{"2011-04-01", 1L, 135.88510131835938d, "Apr 01"}
      );
    } else {
      expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
          columnNames,
          new Object[]{"2011-04-01", 1L, 135.88510131835938d, "Apr 01"},
          new Object[]{"2011-04-02", 1L, 147.42593383789062d, "Apr 02"}
      );
    }

    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testTimeseriesWithOtherMultiDimFilter()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("business")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(118.570340).doubleValue(),
                    "addRowsIndexConstant", new Float(120.570340).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(112.987027).doubleValue(),
                    "addRowsIndexConstant", new Float(114.987027).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterInOr()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }


  @Test
  public void testTimeseriesWithInFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDimAndOr()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newOrDimFilterBuilder()
                                                    .fields(
                                                        QueryRunnerTestHelper.qualityDimension,
                                                        "automotive",
                                                        "business",
                                                        "billyblank"
                                                    )
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimensionSkipBuckets()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .context(ImmutableMap.<String, Object>of("skipEmptyBuckets", "true"))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", null)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithInvertedFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(new NotDimFilter(new SelectorDimFilter("bobby", "sally", null)))
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, new HashMap<String, Object>()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("billy")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("business")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0,
                    "uniques", 0.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            new DateTime(
                QueryRunnerTestHelper.firstToThird.getIntervals()
                                                  .get(0)
                                                  .getStart()
            ),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "index", 12459.361190795898d,
                    "nindex", 283.31103515625d,
                    "pishcount", 52d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregatorAndAlsoRegularFilters()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            new DateTime(
                QueryRunnerTestHelper.firstToThird.getIntervals()
                                                  .get(0)
                                                  .getStart()
            ),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "index", 283.31103515625d,
                    "nindex", 283.31103515625d,
                    "pishcount", 4d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.dayGran)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "a")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.dayGran)
              .filters(QueryRunnerTestHelper.qualityDimension, "automotive")
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr1()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.placementishDimension)
                                                    .value("a")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    AndDimFilter andDimFilter2 = Druids.newAndDimFilterBuilder()
                                       .fields(
                                           Arrays.<DimFilter>asList(
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.marketDimension)
                                                     .value("spot")
                                                     .build(),
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.qualityDimension)
                                                     .value("automotive")
                                                     .build()
                                           )
                                       )
                                       .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.dayGran)
              .filters(andDimFilter2)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr2()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newOrDimFilterBuilder()
                                                    .fields(QueryRunnerTestHelper.placementishDimension, "a", "b")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .descending(descending)
                                  .build();

    AndDimFilter andDimFilter2 = Druids.newAndDimFilterBuilder()
                                       .fields(
                                           Arrays.<DimFilter>asList(
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.marketDimension)
                                                     .value("spot")
                                                     .build(),
                                               Druids.newOrDimFilterBuilder()
                                                     .fields(
                                                         QueryRunnerTestHelper.qualityDimension,
                                                         "automotive",
                                                         "business"
                                                     )
                                                     .build()
                                           )
                                       )
                                       .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.dayGran)
              .filters(andDimFilter2)
              .intervals(QueryRunnerTestHelper.firstToThird)
              .aggregators(QueryRunnerTestHelper.commonAggregators)
              .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
              .descending(descending)
              .build().run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAgg()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 18L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
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
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 0L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
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
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
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
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
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
                                  .granularity(QueryRunnerTestHelper.allGran)
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

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
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
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                                          QueryRunnerTestHelper.__timeLongSum
                                      )
                                  )
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows",
                    26L,
                    "ntimestamps",
                    13.0,
                    "sumtime",
                    33843139200000L
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithBoundFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1120L,
                    "addRowsIndexConstant", 1130.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testSimpleLimit()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(QueryGranularities.MONTH)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Arrays.asList(
                                          new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          new DoubleMinAggregatorFactory("minIndex", "index")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expected = TimeseriesQueryRunnerTestHelper.createExpected(
        new String[]{"maxIndex", "minIndex"},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
    );
    if (descending) {
      Collections.reverse(expected);
    }
    for (int i = 4; i > 0; i--) {
      List<Result<TimeseriesResultValue>> results = Sequences.toList(
          query.withLimitSpec(new LimitSpec(null, i)).run(TestIndex.segmentWalker, CONTEXT),
          Lists.<Result<TimeseriesResultValue>>newArrayList()
      );
      TestHelper.assertExpectedResults(expected.subList(0, i), results);
    }

    expected = TimeseriesQueryRunnerTestHelper.createExpected(
        new String[]{"maxIndex", "minIndex"},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
    );
    LimitSpec spec = LimitSpecs.of(null, OrderByColumnSpec.asc("minIndex"));
    List<Result<TimeseriesResultValue>> results = Sequences.toList(
        query.withLimitSpec(spec).run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expected, results);

    expected = TimeseriesQueryRunnerTestHelper.createExpected(
        new String[]{"maxIndex", "minIndex"},
        new Object[]{"2011-01-01", 1870.06103515625D, 71.31593322753906D},
        new Object[]{"2011-02-01", 1862.7379150390625D, 72.16365051269531D},
        new Object[]{"2011-03-01", 1734.27490234375D, 59.02102279663086D},
        new Object[]{"2011-04-01", 1522.043701171875D, 72.66842651367188D}
        );
    spec = LimitSpecs.of(null, OrderByColumnSpec.desc("maxIndex"));
    results = Sequences.toList(
        query.withLimitSpec(spec).run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expected, results);
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

    String[] columnNames = {"rows", "index", "addRowsIndexConstant"};
    Iterable<Result<TimeseriesResultValue>> results;

    List<Result<TimeseriesResultValue>> expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        new Object[]{"2011-01-12",  4L, 3600.0, 3605.0},
        new Object[]{"2011-01-13", 12L, 5983.074401855469, 5996.074401855469},
        new Object[]{"2011-01-14", 10L, 4644.134963989258, 4655.134963989258},
        new Object[]{"2011-01-15",  9L, 5356.181037902832, 5366.181037902832},
        new Object[]{"2011-01-16",  8L, 4247.914710998535, 4256.914710998535},
        new Object[]{"2011-01-17", 11L, 4461.939956665039, 4473.939956665039},
        new Object[]{"2011-01-18", 10L, 4127.733467102051, 4138.733467102051},
        new Object[]{"2011-01-19",  9L, 4243.135475158691, 4253.135475158691},
        new Object[]{"2011-01-20",  9L, 4076.9225158691406, 4086.9225158691406},
        new Object[]{"2011-01-21",  0L, 0.0, 1.0},
        new Object[]{"2011-01-22", 10L, 5874.600471496582, 5885.600471496582},
        new Object[]{"2011-01-23", 11L, 5400.676559448242, 5412.676559448242},
        new Object[]{"2011-01-24", 10L, 4710.972122192383, 4721.972122192383},
        new Object[]{"2011-01-25", 10L, 4906.999092102051, 4917.999092102051},
        new Object[]{"2011-01-26", 10L, 6022.893226623535, 6033.893226623535},
        new Object[]{"2011-01-27", 12L, 5934.857353210449, 5947.857353210449},
        new Object[]{"2011-01-28", 11L, 5579.304672241211, 5591.304672241211},
        new Object[]{"2011-01-29", 13L, 5346.517524719238, 5360.517524719238},
        new Object[]{"2011-01-30", 12L, 5400.307342529297, 5413.307342529297},
        new Object[]{"2011-01-31", 11L, 5727.540992736816, 5739.540992736816}
    );

    TimeseriesQuery query = builder.build();
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        new Object[]{"2011-01-29", 13L, 5346.517524719238, 5360.517524719238},
        new Object[]{"2011-01-30", 12L, 5400.307342529297, 5413.307342529297},
        new Object[]{"2011-01-27", 12L, 5934.857353210449, 5947.857353210449},
        new Object[]{"2011-01-13", 12L, 5983.074401855469, 5996.074401855469},
        new Object[]{"2011-01-17", 11L, 4461.939956665039, 4473.939956665039},
        new Object[]{"2011-01-23", 11L, 5400.676559448242, 5412.676559448242},
        new Object[]{"2011-01-28", 11L, 5579.304672241211, 5591.304672241211},
        new Object[]{"2011-01-31", 11L, 5727.540992736816, 5739.540992736816},
        new Object[]{"2011-01-18", 10L, 4127.733467102051, 4138.733467102051},
        new Object[]{"2011-01-14", 10L, 4644.134963989258, 4655.134963989258}
    );

    query = query.withLimitSpec(
        new LimitSpec(Arrays.asList(OrderByColumnSpec.desc("rows"), OrderByColumnSpec.asc("index")), 10)
    );
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
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

    columnNames = new String[]{"rows", "index", "addRowsIndexConstant", "delta", "runningSum"};
    expectedResults = TimeseriesQueryRunnerTestHelper.createExpected(
        columnNames,
        new Object[]{"2011-01-12", 4L, 3600.0, 3605.0, 0.0, 4L},
        new Object[]{"2011-01-13", 12L, 5983.074401855469, 5996.074401855469, 2383.0744018554688, 16L},
        new Object[]{"2011-01-14", 10L, 4644.134963989258, 4655.134963989258, -1338.939437866211, 26L},
        new Object[]{"2011-01-15", 9L, 5356.181037902832, 5366.181037902832, 712.0460739135742, 35L},
        new Object[]{"2011-01-16", 8L, 4247.914710998535, 4256.914710998535, -1108.2663269042969, 43L},
        new Object[]{"2011-01-17", 11L, 4461.939956665039, 4473.939956665039, 214.0252456665039, 54L},
        new Object[]{"2011-01-18", 10L, 4127.733467102051, 4138.733467102051, -334.2064895629883, 64L},
        new Object[]{"2011-01-19", 9L, 4243.135475158691, 4253.135475158691, 115.40200805664062, 73L},
        new Object[]{"2011-01-20", 9L, 4076.9225158691406, 4086.9225158691406, -166.21295928955078, 82L},
        new Object[]{"2011-01-21", 0L, 0.0, 1.0, -4076.9225158691406, 82L},
        new Object[]{"2011-01-22", 10L, 5874.600471496582, 5885.600471496582, 5874.600471496582, 92L},
        new Object[]{"2011-01-23", 11L, 5400.676559448242, 5412.676559448242, -473.92391204833984, 103L},
        new Object[]{"2011-01-24", 10L, 4710.972122192383, 4721.972122192383, -689.7044372558594, 113L},
        new Object[]{"2011-01-25", 10L, 4906.999092102051, 4917.999092102051, 196.02696990966797, 123L},
        new Object[]{"2011-01-26", 10L, 6022.893226623535, 6033.893226623535, 1115.8941345214844, 133L},
        new Object[]{"2011-01-27", 12L, 5934.857353210449, 5947.857353210449, -88.03587341308594, 145L},
        new Object[]{"2011-01-28", 11L, 5579.304672241211, 5591.304672241211, -355.5526809692383, 156L},
        new Object[]{"2011-01-29", 13L, 5346.517524719238, 5360.517524719238, -232.78714752197266, 169L},
        new Object[]{"2011-01-30", 12L, 5400.307342529297, 5413.307342529297, 53.789817810058594, 181L},
        new Object[]{"2011-01-31", 11L, 5727.540992736816, 5739.540992736816, 327.23365020751953, 192L}
    );
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, CONTEXT),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
