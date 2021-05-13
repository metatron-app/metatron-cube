/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.LucenePointFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestEstateQuery extends TestHelper
{
  public static final TestQuerySegmentWalker segmentWalker = estateWalker;

  @SuppressWarnings("unchecked")
  private <T> List<T> runQuery(Query query)
  {
    return runQuery(query, segmentWalker);
  }

  private List<Row> runQuery(BaseAggregationQuery query)
  {
    return runQuery(query, segmentWalker);
  }

  @Test
  public void testSchema()
  {
    Schema schema = Iterables.getOnlyElement(runQuery(SchemaQuery.of("estate")));
    Assert.assertEquals("[__time, idx, gu, gis, price, amt, py, hasPrice]", schema.getColumnNames().toString());
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, struct(lat:double,lon:double,addr:string), double, long, float, boolean]",
        schema.getColumnTypes().toString()
    );
    Assert.assertEquals(
        "{gis={coord=point(latitude=lat,longitude=lon), addr=text}}", schema.getDescriptors().toString()
    );
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("estate")
        .setDimensions(DefaultDimensionSpec.of("gu"))
        .setAggregatorSpecs(
            CountAggregatorFactory.predicate("count", "hasPrice"),
            new GenericSumAggregatorFactory("price", "price", null)
        )
        .setGranularity(Granularities.YEAR)
        .havingSpec(new ExpressionHavingSpec("count > 0"))
        .build();
    String[] columnNames = {"__time", "gu", "count", "price"};
    Object[][] objects = {
        array("2018-01-01", "강남구", 46L, 46000.0),
        array("2018-01-01", "강동구", 27L, 27000.0),
        array("2018-01-01", "강북구", 4L, 4000.0),
        array("2018-01-01", "강서구", 9L, 9000.0),
        array("2018-01-01", "관악구", 5L, 5000.0),
        array("2018-01-01", "광진구", 2L, 2000.0),
        array("2018-01-01", "구로구", 10L, 10000.0),
        array("2018-01-01", "금천구", 4L, 4000.0),
        array("2018-01-01", "노원구", 6L, 6000.0)
    };
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    SegmentMetadataQuery meta = Druids.newSegmentMetadataQueryBuilder()
                                      .dataSource("estate")
                                      .toInclude(new ListColumnIncluderator(Arrays.asList("price", "py")))
                                      .analysisTypes(AnalysisType.NULL_COUNT)
                                      .build();

    SegmentAnalysis segment = Iterables.getOnlyElement(runQuery(meta));
    Assert.assertEquals(5862, segment.getNumRows());

    ColumnAnalysis price = segment.getColumns().get("price");
    Assert.assertEquals(5749, price.getNullCount());

    ColumnAnalysis py = segment.getColumns().get("py");
    Assert.assertEquals(0, py.getNullCount());
  }

  @Test
  public void testBooleanFilter()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("estate")
        .setDimensions(DefaultDimensionSpec.of("gu"))
        .setDimFilter(new SelectorDimFilter("hasPrice", "true", null))
        .setAggregatorSpecs(
            new CountAggregatorFactory("count"),
            new GenericSumAggregatorFactory("price", "price", null)
        )
        .setGranularity(Granularities.YEAR)
        .havingSpec(new ExpressionHavingSpec("count > 0"))
        .build();
    String[] columnNames = {"__time", "gu", "count", "price"};
    Object[][] objects = {
        array("2018-01-01", "강남구", 46L, 46000.0),
        array("2018-01-01", "강동구", 27L, 27000.0),
        array("2018-01-01", "강북구", 4L, 4000.0),
        array("2018-01-01", "강서구", 9L, 9000.0),
        array("2018-01-01", "관악구", 5L, 5000.0),
        array("2018-01-01", "광진구", 2L, 2000.0),
        array("2018-01-01", "구로구", 10L, 10000.0),
        array("2018-01-01", "금천구", 4L, 4000.0),
        array("2018-01-01", "노원구", 6L, 6000.0)
    };
    Iterable<Row> results = runQuery(query);
    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withFilter(new MathExprFilter("hasPrice"));
    results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withFilter(new SelectorDimFilter("hasPrice", "false", null));
    objects = new Object[][]{
        array("2018-01-01", "강남구", 259L, 0.0),
        array("2018-01-01", "강동구", 219L, 0.0),
        array("2018-01-01", "강북구", 136L, 0.0),
        array("2018-01-01", "강서구", 496L, 0.0),
        array("2018-01-01", "관악구", 176L, 0.0),
        array("2018-01-01", "광진구", 99L, 0.0),
        array("2018-01-01", "구로구", 288L, 0.0),
        array("2018-01-01", "금천구", 130L, 0.0),
        array("2018-01-01", "노원구", 616L, 0.0),
        array("2018-01-01", "도봉구", 267L, 0.0),
        array("2018-01-01", "동대문구", 208L, 0.0),
        array("2018-01-01", "동작구", 318L, 0.0),
        array("2018-01-01", "마포구", 151L, 0.0),
        array("2018-01-01", "서대문구", 156L, 0.0),
        array("2018-01-01", "서초구", 240L, 0.0),
        array("2018-01-01", "성동구", 207L, 0.0),
        array("2018-01-01", "성북구", 281L, 0.0),
        array("2018-01-01", "송파구", 325L, 0.0),
        array("2018-01-01", "양천구", 340L, 0.0),
        array("2018-01-01", "영등포구", 293L, 0.0),
        array("2018-01-01", "용산구", 77L, 0.0),
        array("2018-01-01", "은평구", 173L, 0.0),
        array("2018-01-01", "종로구", 35L, 0.0),
        array("2018-01-01", "중구", 74L, 0.0),
        array("2018-01-01", "중랑구", 185L, 0.0)
    };
    results = runQuery(query);
    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(columnNames, objects);
    TestHelper.assertExpectedObjects(expectedResults, results, "");

    query = query.withFilter(new MathExprFilter("!hasPrice"));
    results = runQuery(query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testPointFilter()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.addr"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("estate")
        .columns(columns)
        .orderBy(OrderByColumnSpec.descending("amt"))
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));


    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
    );
    builder.filters(LucenePointFilter.distance("gis.coord", 37.492927, 127.020779, 400));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    expected = createExpectedMaps(
        columns,
        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 82700L, 84.82F, "서초동 1687 유원서초"},
        new Object[]{"서초구", 81000L, 84.82F, "서초동 1687 유원서초"},
        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
    );
    builder.filters(LucenePointFilter.nearest("gis.coord", 37.492927, 127.020779, 7));
    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }
}
