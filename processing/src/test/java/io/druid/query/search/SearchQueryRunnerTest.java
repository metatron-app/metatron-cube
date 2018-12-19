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

package io.druid.query.search;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.math.expr.Parser;
import io.druid.query.Druids;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.search.search.FragmentSearchQuerySpec;
import io.druid.query.search.search.LexicographicSearchSortSpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.StrlenSearchSortSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SearchQueryRunnerTest
{
  static {
    Parser.register(ModuleBuiltinFunctions.class);
  }

  private static final Logger LOG = new Logger(SearchQueryRunnerTest.class);

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public SearchQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testSearchHitSerDe() throws Exception
  {
    for (SearchHit hit : Arrays.asList(new SearchHit("dim1", "val1"), new SearchHit("dim2", "val2", 3))) {
      SearchHit read = TestHelper.JSON_MAPPER.readValue(
          TestHelper.JSON_MAPPER.writeValueAsString(hit),
          SearchHit.class
      );
      Assert.assertEquals(hit, read);
      if (hit.getCount() == null) {
        Assert.assertNull(read.getCount());
      } else {
        Assert.assertEquals(hit.getCount(), read.getCount());
      }
    }
  }

  @Test
  public void testSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .query("a")
                                    .build();

    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "a", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 186));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.placementDimension,
                                            QueryRunnerTestHelper.placementishDimension
                                        )
                                    )
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementDimension, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims2()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.placementDimension,
                                            QueryRunnerTestHelper.placementishDimension
                                        )
                                    )
                                    .sortSpec(new StrlenSearchSortSpec())
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementDimension, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testFragmentSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .query(new FragmentSearchQuerySpec(Arrays.asList("auto", "ve")))
                                    .build();

    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithDimensionQuality()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("quality")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionProvider()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("market")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionsQualityAndProvider()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    Druids.SearchQueryBuilder builder =
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.qualityDimension,
                      QueryRunnerTestHelper.marketDimension
                  )
              )
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a");

    checkSearchQuery(builder.build(), expectedHits);

    // count:asc, value:desc
    builder.sortSpec(new LexicographicSearchSortSpec(Arrays.asList("$count", "$value:desc")));

    expectedHits.clear();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));

    checkSearchQueryWithOrder(builder.build(), expectedHits);

    // dimension:desc, count:desc, value:asc
    builder.sortSpec(new LexicographicSearchSortSpec(Arrays.asList("$dimension:desc", "$count:desc", "$value")));

    expectedHits.clear();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQueryWithOrder(builder.build(), expectedHits);

    // value only
    builder.valueOnly(true);

    expectedHits.clear();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive"));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment"));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health"));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine"));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel"));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market"));

    checkSearchQueryWithOrder(builder.build(), expectedHits);
  }

  @Test
  public void testSearchWithResultOrdering()
  {
    final Druids.SearchQueryBuilder builder =
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .sortSpec(new LexicographicSearchSortSpec(Arrays.asList("$count:desc", "$value:desc")))
              .limit(-1);

    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 558));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 372));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 372));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "a", 186));

    checkSearchQueryWithOrder(builder.build(), expectedHits);

    // limit should not affect count value

    expectedHits.clear();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 558));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 372));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 372));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 186));

    checkSearchQueryWithOrder(builder.limit(4).build(), expectedHits);
  }

  @Test
  public void testSearchWithDimensionsPlacementAndProvider()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.placementishDimension,
                      QueryRunnerTestHelper.marketDimension
                  )
              )
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("mark")
              .build(),
        expectedHits
    );
  }


  @Test
  public void testSearchWithExtractionFilter1()
  {
    final String automotiveSnowman = "automotive☃";
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, automotiveSnowman, 93));

    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.<Object, String>of("automotive", automotiveSnowman), false),
        true,
        null,
        true,
        true
    );

    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(dataSource)
                              .granularity(QueryRunnerTestHelper.allGran)
                              .filters(
                                  new SelectorDimFilter(
                                      QueryRunnerTestHelper.qualityDimension,
                                      automotiveSnowman,
                                      lookupExtractionFn
                                  )
                              )
                              .intervals(QueryRunnerTestHelper.fullOnInterval)
                              .dimensions(
                                  new ExtractionDimensionSpec(
                                      QueryRunnerTestHelper.qualityDimension,
                                      null,
                                      lookupExtractionFn,
                                      null
                                  )
                              )
                              .query("☃")
                              .build();

    checkSearchQuery(query, expectedHits);
  }

  @Test
  public void testSearchWithSingleFilter1()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(
                  new AndDimFilter(
                      Arrays.<DimFilter>asList(
                          new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "total_market", null),
                          new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "mezzanine", null))))
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithSingleFilter2()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(QueryRunnerTestHelper.marketDimension, "total_market")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions(QueryRunnerTestHelper.marketDimension)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchMultiAndFilter()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    DimFilter filter = DimFilters.and(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("spot")
              .build(),
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("automotive")
              .build()
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithMultiOrFilter()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    DimFilter filter = DimFilters.or(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("total_market")
              .build(),
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("automotive")
              .build()
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithEmptyResults()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("abcd123")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithFilterEmptyResults()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();

    DimFilter filter = DimFilters.and(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.marketDimension)
              .value("total_market")
              .build(),
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.qualityDimension)
              .value("automotive")
              .build()
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedHits
    );
  }


  @Test
  public void testSearchNonExistingDimension()
  {
    List<SearchHit> expectedHits = Lists.newLinkedList();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions("does_not_exist")
              .query("a")
              .build(),
        expectedHits
    );
  }

  private void checkSearchQuery(SearchQuery searchQuery, List<SearchHit> expectedResults)
  {
    Iterable<Result<SearchResultValue>> results = Sequences.toList(
        searchQuery.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SearchResultValue>>newArrayList()
    );
    for (Object x : expectedResults) {
      System.out.println("e : " + x);
    }
    for (Object x : results) {
      System.out.println("a : " + x);
    }
    List<SearchHit> copy = Lists.newLinkedList(expectedResults);
    for (Result<SearchResultValue> result : results) {
      Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), result.getTimestamp());
      Assert.assertNotNull(result.getValue());

      Iterable<SearchHit> resultValues = result.getValue();
      for (SearchHit resultValue : resultValues) {
        int index = copy.indexOf(resultValue);
        if (index < 0) {
          fail(
              expectedResults, results,
              "No result found containing " + resultValue.getDimension() + " and " + resultValue.getValue()
          );
        }
        SearchHit expected = copy.remove(index);
        if (!resultValue.toString().equals(expected.toString())) {
          fail(
              expectedResults, results,
              "Invalid count for " + resultValue + ".. which was expected to be " + expected.getCount()
          );
        }
      }
    }
    if (!copy.isEmpty()) {
      fail(expectedResults, results, "Some expected results are not shown: " + copy);
    }
  }

  private void checkSearchQueryWithOrder(SearchQuery searchQuery, List<SearchHit> expectedResults)
  {
    List<Result<SearchResultValue>> results = Sequences.toList(
        searchQuery.run(TestIndex.segmentWalker, ImmutableMap.<String, Object>of()),
        Lists.<Result<SearchResultValue>>newArrayList()
    );
    Assert.assertEquals(1, results.size());
    SearchResultValue result = results.get(0).getValue();
    System.out.println(result.getValue());
    Assert.assertEquals(expectedResults.size(), result.getValue().size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertEquals(expectedResults.get(i), result.getValue().get(i));
    }
  }

  private void fail(
      List<SearchHit> expectedResults,
      Iterable<Result<SearchResultValue>> results, String errorMsg
  )
  {
    LOG.info("Expected..");
    for (SearchHit expected : expectedResults) {
      LOG.info(expected.toString());
    }
    LOG.info("Result..");
    for (Result<SearchResultValue> r : results) {
      for (SearchHit v : r.getValue()) {
        LOG.info(v.toString());
      }
    }
    Assert.fail(errorMsg);
  }
}
