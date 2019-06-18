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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryRunnerTestHelper.RowBuilder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.ordering.OrderingSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SketchGroupByQueryRunnerTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return GroupByQueryRunnerTest.constructorFeeder();
  }

  public SketchGroupByQueryRunnerTest(GroupByQueryRunnerFactory factory, QueryRunner<Row> runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByQuantile()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(SketchTestHelper.indexQuantileAggr))
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(SketchTestHelper.quantilesOfIndexPostAggr))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setOutputColumns(Arrays.asList("alias", "index_quantiles"))
        .build();

    RowBuilder builder = new RowBuilder(new String[]{"alias", "index_quantiles"});

    List<Row> expectedResults = builder
        .add("1970-01-01", "automotive", new Double[]{93.00157165527344, 130.10498046875, 168.9884796142578})
        .add("1970-01-01", "business", new Double[]{98.81584930419922, 107.76509857177734, 124.52499389648438})
        .add("1970-01-01", "entertainment", new Double[]{99.0075912475586, 133.60643005371094, 158.68252563476562})
        .add("1970-01-01", "health", new Double[]{96.22660827636719, 108.82213592529297, 124.17194366455078})
        .add("1970-01-01", "mezzanine", new Double[]{100.6434326171875, 996.2053833007812, 1317.4583740234375})
        .add("1970-01-01", "news", new Double[]{100.0, 108.13581085205078, 125.04487609863281})
        .add("1970-01-01", "premium", new Double[]{106.69636535644531, 879.9880981445312, 1345.78173828125})
        .add("1970-01-01", "technology", new Double[]{72.16365051269531, 88.14277648925781, 107.62779235839844})
        .add("1970-01-01", "travel", new Double[]{107.66323852539062, 119.7391128540039, 136.1634063720703})
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withDimFilter(new MathExprFilter("index > 100"));

    expectedResults = builder
        .add("1970-01-01", "automotive", new Double[]{113.22145080566406, 134.4625244140625, 174.89051818847656})
        .add("1970-01-01", "business", new Double[]{103.64395141601562, 110.8973617553711, 125.76695251464844})
        .add("1970-01-01", "entertainment", new Double[]{109.6664047241211, 136.98341369628906, 162.81544494628906})
        .add("1970-01-01", "health", new Double[]{103.01893615722656, 113.8960189819336, 130.5989990234375})
        .add("1970-01-01", "mezzanine", new Double[]{105.4530258178711, 1017.5731811523438, 1345.96435546875})
        .add("1970-01-01", "news", new Double[]{102.48683166503906, 112.35429382324219, 125.24324798583984})
        .add("1970-01-01", "premium", new Double[]{108.8630142211914, 884.801513671875, 1345.78173828125})
        .add("1970-01-01", "technology", new Double[]{102.04454040527344, 108.4896011352539, 116.97900390625})
        .add("1970-01-01", "travel", new Double[]{107.70626068115234, 119.76852416992188, 136.1634063720703})
        .build();

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByQuantileOnVC()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setVirtualColumns(new ExprVirtualColumn("concat(market, '\\u0001', quality)", "VC"))
        .setAggregatorSpecs(
            new GenericSketchAggregatorFactory(
                "SKETCH",
                "VC",
                ValueDesc.STRING,
                SketchOp.QUANTILE,
                null,
                OrderingSpec.toOrderingSpecs("stringarray(\u0001)"),
                false
            )
        )
        .setPostAggregatorSpecs(
            SketchQuantilesPostAggregator.evenSpaced("X", "SKETCH", 3)
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setOutputColumns(Arrays.asList("X"))
        .build();

    Row result = Iterables.getOnlyElement(GroupByQueryRunnerTestHelper.runQuery(factory, runner, query));
    Assert.assertArrayEquals(
        new String[] {"spot\u0001automotive", "spot\u0001premium", "upfront\u0001premium"},
        (Object[]) result.getRaw("X")
    );
  }

  @Test
  public void testGroupByQuantileOnDimension()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(DefaultDimensionSpec.toSpec("market"))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(SketchTestHelper.qualityQuantileAggr))
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(SketchTestHelper.quantilesOfQualityPostAggr))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setOutputColumns(Arrays.asList("market", SketchTestHelper.quantilesOfQualityMetric))
        .build();

    RowBuilder builder = new RowBuilder(new String[]{"market", SketchTestHelper.quantilesOfQualityMetric});

    List<Row> expectedResults = builder
        .add("1970-01-01", "spot", new String[]{"automotive", "mezzanine", "travel"})
        .add("1970-01-01", "total_market", new String[]{"mezzanine", "premium", "premium"})
        .add("1970-01-01", "upfront", new String[]{"mezzanine", "premium", "premium"})
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withDimFilter(new MathExprFilter("index > 150"));

    expectedResults = builder
        .add("1970-01-01", "spot", new String[]{"automotive", "entertainment", "health"})
        .add("1970-01-01", "total_market", new String[]{"mezzanine", "premium", "premium"})
        .add("1970-01-01", "upfront", new String[]{"mezzanine", "premium", "premium"})
        .build();

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByTheta()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(SketchTestHelper.indexThetaAggr))
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(SketchTestHelper.cardinalityOfIndexPostAggr))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setOutputColumns(Arrays.asList("alias", SketchTestHelper.cardinalityOfIndexMetric))
        .build();

    RowBuilder builder = new RowBuilder(new String[]{"alias", SketchTestHelper.cardinalityOfIndexMetric});

    List<Row> expectedResults = builder
        .add("1970-01-01", "automotive", 93.0)
        .add("1970-01-01", "business", 93.0)
        .add("1970-01-01", "entertainment", 93.0)
        .add("1970-01-01", "health", 93.0)
        .add("1970-01-01", "mezzanine", 279.0)
        .add("1970-01-01", "news", 93.0)
        .add("1970-01-01", "premium", 279.0)
        .add("1970-01-01", "technology", 93.0)
        .add("1970-01-01", "travel", 93.0)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withDimFilter(new MathExprFilter("index > 100"));

    expectedResults = builder
        .add("1970-01-01", "automotive", 78.0)
        .add("1970-01-01", "business", 77.0)
        .add("1970-01-01", "entertainment", 80.0)
        .add("1970-01-01", "health", 68.0)
        .add("1970-01-01", "mezzanine", 255.0)
        .add("1970-01-01", "news", 83.0)
        .add("1970-01-01", "premium", 277.0)
        .add("1970-01-01", "technology", 17.0)
        .add("1970-01-01", "travel", 92.0)
        .build();

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupByThetaOnDimension()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnInterval)
        .setDimensions(DefaultDimensionSpec.toSpec("market"))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(SketchTestHelper.qualityThetaAggr))
        .setPostAggregatorSpecs(Arrays.<PostAggregator>asList(SketchTestHelper.cardinalityOfQualityPostAggr))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setOutputColumns(Arrays.asList("market", SketchTestHelper.cardinalityOfQualityMetric))
        .build();

    RowBuilder builder =
        new RowBuilder(new String[]{"market", SketchTestHelper.cardinalityOfQualityMetric});

    List<Row> expectedResults = builder
        .add("1970-01-01", "spot", 9.0)
        .add("1970-01-01", "total_market", 2.0)
        .add("1970-01-01", "upfront", 2.0)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withDimFilter(new MathExprFilter("index > 150"));

    expectedResults = builder
        .add("1970-01-01", "spot", 6.0)
        .add("1970-01-01", "total_market", 2.0)
        .add("1970-01-01", "upfront", 2.0)
        .build();

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
