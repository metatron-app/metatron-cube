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

package io.druid.query.join;

import com.google.common.collect.Iterables;
import com.google.common.io.CharSource;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.query.Druids;
import io.druid.query.JoinElement;
import io.druid.query.JoinQuery;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.sketch.SketchQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class PartitionedJoinQueryRunnerTest extends SketchQueryRunnerTestHelper
{
  static final String JOIN_DS_P = "join_test_p";

  static final TestQuerySegmentWalker SEGMENT_WALKER = segmentWalker.duplicate();

  static {
    AggregatorFactory metric = new GenericSumAggregatorFactory("value", "value", ValueDesc.LONG);
    DimensionsSpec dimensions = new DimensionsSpec(
        StringDimensionSchema.ofNames("quality", "quality_month"), null, null
    );
    IncrementalIndexSchema schema = TestIndex.SAMPLE_SCHEMA
        .withMinTimestamp(new DateTime("2011-01-01").getMillis())
        .withDimensionsSpec(dimensions)
        .withMetrics(metric)
        .withRollup(false);

    DataSegment segment = new DataSegment(
        JOIN_DS_P,
        TestIndex.INTERVAL,
        "0",
        null,
        Arrays.asList("quality", "quality_month"),
        Arrays.asList("value"),
        null,
        null,
        0
    );
    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            dimensions,
            "\t",
            "\u0001",
            Arrays.asList("ts", "quality", "quality_month", "value")
        )
        , "utf8"
    );
    CharSource source = TestIndex.asCharSource("druid.sample.join.tsv");
    SEGMENT_WALKER.add(segment, TestIndex.makeRealtimeIndex(source, schema, parser));
//    SEGMENT_WALKER.getQueryConfig().getJoin().setHashJoinThreshold(-1);   // seemed working with hash join
    SEGMENT_WALKER.getQueryConfig().getJoin().setSemiJoinThreshold(-1);
    SEGMENT_WALKER.getQueryConfig().getJoin().setBroadcastJoinThreshold(-1);
  }

  @Override
  protected TestQuerySegmentWalker getSegmentWalker()
  {
    return SEGMENT_WALKER;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public PartitionedJoinQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testJoin()
  {
    JoinQuery joinQuery = Druids
        .newJoinQueryBuilder()
        .dataSource(dataSource, ViewDataSource.of(dataSource, "__time", "market", "quality", "index"))
        .dataSource(JOIN_DS_P, TableDataSource.of(JOIN_DS_P))
        .intervals(firstToThird)
        .element(JoinElement.inner(dataSource + ".quality = " + JOIN_DS_P + ".quality"))
        .build();

    String[] columns = new String[]{"__time", "quality", "market", "index", "quality_month", "value"};
    List<Row> expectedRows = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns,
        array("2011-04-01", "automotive", "spot", 135.88510131835938, "april_automotive", 41111L),
        array("2011-04-01", "business", "spot", 118.57034301757812, "april_business", 41112L),
        array("2011-04-01", "health", "spot", 120.13470458984375, "april_health", 41113L),
        array("2011-04-01", "mezzanine", "spot", 109.70581817626953, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "spot", 144.5073699951172, "april_premium", 41115L),
        array("2011-04-01", "technology", "spot", 78.62254333496094, "april_technology", 41116L),
        array("2011-04-01", "mezzanine", "total_market", 1314.8397216796875, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "total_market", 1522.043701171875, "april_premium", 41115L),
        array("2011-04-01", "mezzanine", "upfront", 1447.3411865234375, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "upfront", 1234.24755859375, "april_premium", 41115L),
        array("2011-04-01", "automotive", "spot", 147.42593383789062, "april_automotive", 41111L),
        array("2011-04-01", "business", "spot", 112.98703002929688, "april_business", 41112L),
        array("2011-04-01", "health", "spot", 113.44600677490234, "april_health", 41113L),
        array("2011-04-01", "mezzanine", "spot", 110.93193054199219, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "spot", 135.30149841308594, "april_premium", 41115L),
        array("2011-04-01", "technology", "spot", 97.38743591308594, "april_technology", 41116L),
        array("2011-04-01", "mezzanine", "total_market", 1193.5562744140625, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "total_market", 1321.375, "april_premium", 41115L),
        array("2011-04-01", "mezzanine", "upfront", 1144.3424072265625, "april_mezzanine", 41114L),
        array("2011-04-01", "premium", "upfront", 1049.738525390625, "april_premium", 41115L)
    );

    Iterable<Row> rows = Iterables.transform(runTabularQuery(joinQuery), Rows.mapToRow(Column.TIME_COLUMN_NAME));
    for (Object x : rows) {
      System.out.println(x);
    }
    TestHelper.assertExpectedObjects(expectedRows, rows, "");
  }
}