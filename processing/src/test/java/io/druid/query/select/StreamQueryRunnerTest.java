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

package io.druid.query.select;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.TestIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class StreamQueryRunnerTest extends QueryRunnerTestHelper
{
  private static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return cartesian(Arrays.asList(TestIndex.DS_NAMES), Arrays.asList(false, true));
  }

  private final String dataSource;
  private final boolean descending;

  public StreamQueryRunnerTest(String dataSource, boolean descending)
  {
    this.dataSource = dataSource;
    this.descending = descending;
  }

  private Druids.SelectQueryBuilder newTestQuery()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(TableDataSource.of(dataSource))
                 .descending(descending)
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.<String>asList()))
                 .metrics(Arrays.<String>asList())
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .granularity(QueryRunnerTestHelper.allGran);
  }

  @Test
  public void testBasic()
  {
    Druids.SelectQueryBuilder builder = testEq(newTestQuery());
    testEq(builder.columns(Arrays.asList("market", "quality")));
    testEq(builder.columns(Arrays.asList("__time", "market", "quality", "index", "indexMin")));
    testEq(builder.intervals(I_0112_0114));
    testEq(builder.limit(3));

    StreamQuery query = builder.streaming();

    String[] columnNames = {"__time", "market", "quality", "index", "indexMin"};
    List<Object[]> expected;
    if (descending) {
      expected = createExpected(
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "premium", 1564.61767578125D, 1564.6177F},
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "mezzanine", 826.0601806640625D, 826.0602F},
          new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "premium", 1689.0128173828125D, 1689.0128F}
      );
    } else {
      expected = createExpected(
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "automotive", 100D, 100F},
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "business", 100D, 100F},
          new Object[]{"2011-01-12T00:00:00.000Z", "spot", "entertainment", 100D, 100F}
      );
    }

    List<Object[]> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Object[]>newArrayList()
    );
    validate(expected, results);

    query = query.withFilter(new MathExprFilter("index > 200"));
    if (descending) {
      expected = createExpected(
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "premium", 1564.61767578125D, 1564.6177F},
          new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "mezzanine", 826.0601806640625D, 826.0602F},
          new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "premium", 1689.0128173828125D, 1689.0128F}
      );
    } else {
      expected = createExpected(
          new Object[]{"2011-01-12T00:00:00.000Z", "total_market", "mezzanine", 1000D, 1000F},
          new Object[]{"2011-01-12T00:00:00.000Z", "total_market", "premium", 1000D, 1000F},
          new Object[]{"2011-01-12T00:00:00.000Z", "upfront", "mezzanine", 800D, 800F}
      );
    }
    results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Object[]>newArrayList()
    );
    validate(expected, results);
  }

  @Test
  public void testSplit()
  {
    Druids.SelectQueryBuilder builder = testEq(newTestQuery());
    testEq(builder.columns(Arrays.asList("market", "quality")));
    testEq(builder.columns(Arrays.asList("__time", "market", "quality", "index", "indexMin")));
    testEq(builder.intervals(I_0112_0114));
    testEq(builder.limit(3));

    StreamQuery query = builder.streaming();
    query = query.withResultOrdering(OrderByColumnSpec.descending("index"));
    query = (StreamQuery) query.withOverriddenContext(Query.STREAM_RAW_LOCAL_SPLIT_NUM, 2);
    List<Object[]> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Object[]>newArrayList()
    );
    List<Object[]> expected = createExpected(
        new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "premium", 1689.0128173828125D, 1689.0128F},
        new Object[]{"2011-01-13T00:00:00.000Z", "upfront", "premium", 1564.61767578125D, 1564.6177F},
        new Object[]{"2011-01-13T00:00:00.000Z", "total_market", "mezzanine", 1040.945556640625D, 1040.9456F}
    );
    validate(expected, results);
  }

  @Test
  public void testWindowing()
  {
    Druids.SelectQueryBuilder builder = newTestQuery()
        .columns(Arrays.asList("__time", "market", "quality", "index", "indexMin"))
        .intervals(I_0112_0114)
        .limitSpec(
            LimitSpec.of(
                new WindowingSpec(
                    Arrays.asList("market"),
                    OrderByColumnSpec.descending("market", "__time", "quality"),
                    "delta_market = $delta(index)", "sum_market = $sum(index)"
                )
            )
        );
    String[] expected;
    List<Object[]> results;

    expected = new String[]{
        "[1294876800000, upfront, premium, 1564.61767578125, 1564.6177, 0.0, 1564.61767578125]",
        "[1294876800000, upfront, mezzanine, 826.0601806640625, 826.0602, -738.5574951171875, 2390.6778564453125]",
        "[1294790400000, upfront, premium, 800.0, 800.0, -26.0601806640625, 3190.6778564453125]",
        "[1294790400000, upfront, mezzanine, 800.0, 800.0, 0.0, 3990.6778564453125]",
        "[1294876800000, total_market, premium, 1689.0128173828125, 1689.0128, 0.0, 1689.0128173828125]",
        "[1294876800000, total_market, mezzanine, 1040.945556640625, 1040.9456, -648.0672607421875, 2729.9583740234375]",
        "[1294790400000, total_market, premium, 1000.0, 1000.0, -40.945556640625, 3729.9583740234375]",
        "[1294790400000, total_market, mezzanine, 1000.0, 1000.0, 0.0, 4729.9583740234375]",
        "[1294876800000, spot, travel, 106.23693084716797, 106.23693, 0.0, 106.23693084716797]",
        "[1294876800000, spot, technology, 111.35667419433594, 111.356674, 5.119743347167969, 217.5936050415039]"
    };
    results = runQuery(builder.streaming());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(i + 1 + " th", expected[i], Arrays.toString(results.get(i)));
    }

    builder.limitSpec(
        LimitSpec.of(
            new WindowingSpec(
                null, OrderByColumnSpec.descending("market", "__time", "quality"),
                "sum_all = $sum(index)", "sum_all_ratio_percent = sum_all / $last(sum_all) * 100"
            )
        )
    );
    expected = new String[]{
        "[1294876800000, upfront, premium, 1564.61767578125, 1564.6177, 1564.61767578125, 14.791314074439935]",
        "[1294876800000, upfront, mezzanine, 826.0601806640625, 826.0602, 2390.6778564453125, 22.600580047668668]",
        "[1294790400000, upfront, premium, 800.0, 800.0, 3190.6778564453125, 30.163482757203397]",
        "[1294790400000, upfront, mezzanine, 800.0, 800.0, 3990.6778564453125, 37.72638546673813]",
        "[1294876800000, total_market, premium, 1689.0128173828125, 1689.0128, 5679.690673828125, 53.693684983017334]",
        "[1294876800000, total_market, mezzanine, 1040.945556640625, 1040.9456, 6720.63623046875, 63.53439744651174]",
        "[1294790400000, total_market, premium, 1000.0, 1000.0, 7720.63623046875, 72.98802583343014]",
        "[1294790400000, total_market, mezzanine, 1000.0, 1000.0, 8720.63623046875, 82.44165422034855]",
        "[1294876800000, spot, travel, 106.23693084716797, 106.23693, 8826.873161315918, 83.44597868554443]",
        "[1294876800000, spot, technology, 111.35667419433594, 111.356674, 8938.229835510254, 84.49870330178084]",
        "[1294876800000, spot, premium, 108.8630142211914, 108.863014, 9047.092849731445, 85.52785378330779]",
        "[1294876800000, spot, news, 102.8516845703125, 102.851685, 9149.944534301758, 86.50017538820408]",
        "[1294876800000, spot, mezzanine, 104.46576690673828, 104.46577, 9254.410301208496, 87.48775592769482]",
        "[1294876800000, spot, health, 114.94740295410156, 114.9474, 9369.357704162598, 88.57442595926427]",
        "[1294876800000, spot, entertainment, 110.08729553222656, 110.087296, 9479.444999694824, 89.61515034134679]",
        "[1294876800000, spot, business, 103.62940216064453, 103.6294, 9583.074401855469, 90.59482419933205]",
        "[1294876800000, spot, automotive, 94.87471008300781, 94.87471, 9677.949111938477, 91.49173445177343]",
        "[1294790400000, spot, travel, 100.0, 100.0, 9777.949111938477, 92.43709729046526]",
        "[1294790400000, spot, technology, 100.0, 100.0, 9877.949111938477, 93.38246012915711]",
        "[1294790400000, spot, premium, 100.0, 100.0, 9977.949111938477, 94.32782296784895]",
        "[1294790400000, spot, news, 100.0, 100.0, 10077.949111938477, 95.2731858065408]",
        "[1294790400000, spot, mezzanine, 100.0, 100.0, 10177.949111938477, 96.21854864523264]",
        "[1294790400000, spot, health, 100.0, 100.0, 10277.949111938477, 97.16391148392448]",
        "[1294790400000, spot, entertainment, 100.0, 100.0, 10377.949111938477, 98.10927432261632]",
        "[1294790400000, spot, business, 100.0, 100.0, 10477.949111938477, 99.05463716130815]",
        "[1294790400000, spot, automotive, 100.0, 100.0, 10577.949111938477, 100.0]"
    };
    results = runQuery(builder.streaming());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(i + 1 + " th", expected[i], Arrays.toString(results.get(i)));
    }

    builder.limitSpec(
        new LimitSpec(
            Arrays.asList(
                new WindowingSpec(
                    null, OrderByColumnSpec.descending("market", "__time", "quality"),
                    "mean_all = $mean(index, -1, 1)", "count_all = $size()"
                )
            )
        )
    );

    expected = new String[]{
        "[1294876800000, upfront, premium, 1564.61767578125, 1564.6177, 1195.3389282226562, 26]",
        "[1294876800000, upfront, mezzanine, 826.0601806640625, 826.0602, 1063.5592854817708, 26]",
        "[1294790400000, upfront, premium, 800.0, 800.0, 808.6867268880209, 26]",
        "[1294790400000, upfront, mezzanine, 800.0, 800.0, 1096.3376057942708, 26]",
        "[1294876800000, total_market, premium, 1689.0128173828125, 1689.0128, 1176.6527913411458, 26]",
        "[1294876800000, total_market, mezzanine, 1040.945556640625, 1040.9456, 1243.3194580078125, 26]",
        "[1294790400000, total_market, premium, 1000.0, 1000.0, 1013.6485188802084, 26]",
        "[1294790400000, total_market, mezzanine, 1000.0, 1000.0, 702.078976949056, 26]",
        "[1294876800000, spot, travel, 106.23693084716797, 106.23693, 405.86453501383465, 26]",
        "[1294876800000, spot, technology, 111.35667419433594, 111.356674, 108.81887308756511, 26]",
        "[1294876800000, spot, premium, 108.8630142211914, 108.863014, 107.69045766194661, 26]",
        "[1294876800000, spot, news, 102.8516845703125, 102.851685, 105.39348856608073, 26]",
        "[1294876800000, spot, mezzanine, 104.46576690673828, 104.46577, 107.42161814371745, 26]",
        "[1294876800000, spot, health, 114.94740295410156, 114.9474, 109.83348846435547, 26]",
        "[1294876800000, spot, entertainment, 110.08729553222656, 110.087296, 109.55470021565755, 26]",
        "[1294876800000, spot, business, 103.62940216064453, 103.6294, 102.86380259195964, 26]",
        "[1294876800000, spot, automotive, 94.87471008300781, 94.87471, 99.50137074788411, 26]",
        "[1294790400000, spot, travel, 100.0, 100.0, 98.29157002766927, 26]",
        "[1294790400000, spot, technology, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, premium, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, news, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, mezzanine, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, health, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, entertainment, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, business, 100.0, 100.0, 100.0, 26]",
        "[1294790400000, spot, automotive, 100.0, 100.0, 100.0, 26]"
    };
    results = runQuery(builder.streaming());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(i + 1 + " th", expected[i], Arrays.toString(results.get(i)));
    }

    builder.limitSpec(
        LimitSpec.of(
            new WindowingSpec(
                null, OrderByColumnSpec.descending("index", "market", "quality"),
                "row_num = $row_num(index)",
                "rank = $rank(index)",
                "dense_rank = $dense_rank(index)"
            )
        )
    );
    expected = new String[]{
        "[1294876800000, total_market, premium, 1689.0128173828125, 1689.0128, 1, 1, 1]",
        "[1294876800000, upfront, premium, 1564.61767578125, 1564.6177, 2, 2, 2]",
        "[1294876800000, total_market, mezzanine, 1040.945556640625, 1040.9456, 3, 3, 3]",
        "[1294790400000, total_market, premium, 1000.0, 1000.0, 4, 4, 4]",
        "[1294790400000, total_market, mezzanine, 1000.0, 1000.0, 5, 4, 4]",
        "[1294876800000, upfront, mezzanine, 826.0601806640625, 826.0602, 6, 6, 5]",
        "[1294790400000, upfront, premium, 800.0, 800.0, 7, 7, 6]",
        "[1294790400000, upfront, mezzanine, 800.0, 800.0, 8, 7, 6]",
        "[1294876800000, spot, health, 114.94740295410156, 114.9474, 9, 9, 7]",
        "[1294876800000, spot, technology, 111.35667419433594, 111.356674, 10, 10, 8]",
        "[1294876800000, spot, entertainment, 110.08729553222656, 110.087296, 11, 11, 9]",
        "[1294876800000, spot, premium, 108.8630142211914, 108.863014, 12, 12, 10]",
        "[1294876800000, spot, travel, 106.23693084716797, 106.23693, 13, 13, 11]",
        "[1294876800000, spot, mezzanine, 104.46576690673828, 104.46577, 14, 14, 12]",
        "[1294876800000, spot, business, 103.62940216064453, 103.6294, 15, 15, 13]",
        "[1294876800000, spot, news, 102.8516845703125, 102.851685, 16, 16, 14]",
        "[1294790400000, spot, travel, 100.0, 100.0, 17, 17, 15]",
        "[1294790400000, spot, technology, 100.0, 100.0, 18, 17, 15]",
        "[1294790400000, spot, premium, 100.0, 100.0, 19, 17, 15]",
        "[1294790400000, spot, news, 100.0, 100.0, 20, 17, 15]",
        "[1294790400000, spot, mezzanine, 100.0, 100.0, 21, 17, 15]",
        "[1294790400000, spot, health, 100.0, 100.0, 22, 17, 15]",
        "[1294790400000, spot, entertainment, 100.0, 100.0, 23, 17, 15]",
        "[1294790400000, spot, business, 100.0, 100.0, 24, 17, 15]",
        "[1294790400000, spot, automotive, 100.0, 100.0, 25, 17, 15]",
        "[1294876800000, spot, automotive, 94.87471008300781, 94.87471, 26, 26, 16]"
    };
    results = runQuery(builder.streaming());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(i + 1 + " th", expected[i], Arrays.toString(results.get(i)));
    }

    builder.outputColumns("__time", "market", "row_num", "rank", "dense_rank");
    expected = new String[]{
        "[1294876800000, total_market, 1, 1, 1]",
        "[1294876800000, upfront, 2, 2, 2]",
        "[1294876800000, total_market, 3, 3, 3]",
        "[1294790400000, total_market, 4, 4, 4]",
        "[1294790400000, total_market, 5, 4, 4]",
        "[1294876800000, upfront, 6, 6, 5]",
        "[1294790400000, upfront, 7, 7, 6]",
        "[1294790400000, upfront, 8, 7, 6]",
        "[1294876800000, spot, 9, 9, 7]",
        "[1294876800000, spot, 10, 10, 8]",
        "[1294876800000, spot, 11, 11, 9]",
        "[1294876800000, spot, 12, 12, 10]",
        "[1294876800000, spot, 13, 13, 11]",
        "[1294876800000, spot, 14, 14, 12]",
        "[1294876800000, spot, 15, 15, 13]",
        "[1294876800000, spot, 16, 16, 14]",
        "[1294790400000, spot, 17, 17, 15]",
        "[1294790400000, spot, 18, 17, 15]",
        "[1294790400000, spot, 19, 17, 15]",
        "[1294790400000, spot, 20, 17, 15]",
        "[1294790400000, spot, 21, 17, 15]",
        "[1294790400000, spot, 22, 17, 15]",
        "[1294790400000, spot, 23, 17, 15]",
        "[1294790400000, spot, 24, 17, 15]",
        "[1294790400000, spot, 25, 17, 15]",
        "[1294876800000, spot, 26, 26, 16]"
    };
    results = runQuery(builder.streaming());
    for (int i = 0; i < expected.length; i++) {
      Assert.assertEquals(i + 1 + " th", expected[i], Arrays.toString(results.get(i)));
    }
  }

  private Druids.SelectQueryBuilder testEq(Druids.SelectQueryBuilder builder)
  {
    StreamQuery query1 = builder.streaming();
    StreamQuery query2 = builder.streaming();
    Map<StreamQuery, String> map = ImmutableMap.of(query1, query1.toString());
    Assert.assertEquals(query2.toString(), map.get(query2));
    return builder;
  }

  public static List<Object[]> createExpected(Object[]... values)
  {
    for (Object[] value : values) {
      value[0] = value[0] instanceof Long ? (Long) value[0] : new DateTime(value[0]).getMillis();
    }
    return Arrays.asList(values);
  }

  public static void validate(
      List<Object[]> expected,
      List<Object[]> result
  )
  {
    int max1 = Math.min(expected.size(), result.size());
    for (int i = 0; i < max1; i++) {
      Object[] e = expected.get(i);
      Object[] r = result.get(i);
      Assert.assertArrayEquals(e, r);
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more results");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less results");
    }
  }
}
