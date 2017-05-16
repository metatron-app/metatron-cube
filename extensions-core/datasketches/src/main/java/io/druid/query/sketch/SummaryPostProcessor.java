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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.guice.annotations.Processing;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.UnionAllQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.LexicographicSearchSortSpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.VirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
@JsonTypeName("sketch.summary")
public class SummaryPostProcessor extends PostProcessingOperator.UnionSupport
{
  private static final Logger LOG = new Logger(SimilarityProcessingOperator.class);

  private final QuerySegmentWalker segmentWalker;
  private final ListeningExecutorService exec;

  @JsonCreator
  public SummaryPostProcessor(
      @JacksonInject QuerySegmentWalker segmentWalker,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.segmentWalker = segmentWalker;
    this.exec = MoreExecutors.listeningDecorator(exec);
  }

  @Override
  public QueryRunner postProcess(QueryRunner baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final Query representative = BaseQuery.getRepresentative(query);
        if (!(representative instanceof SketchQuery)) {
          LOG.warn("query should be 'sketch' type");
          return baseRunner.run(query, responseContext);
        }
        final List<VirtualColumn> virtualColumns = ((SketchQuery) representative).getVirtualColumns();

        final List<ListenableFuture<Integer>> futures = Lists.newArrayList();
        final Map<String, Map<String, Object>> results = Maps.newLinkedHashMap();
        Sequence<Pair<Query, Sequence<Result<Map<String, Object>>>>> sequences = baseRunner.run(query, responseContext);
        for (Pair<Query, Sequence<Result<Map<String, Object>>>> pair :
            Sequences.toList(sequences, Lists.<Pair<Query, Sequence<Result<Map<String, Object>>>>>newArrayList())) {
          SketchQuery sketchQuery = (SketchQuery) pair.lhs;
          Result<Map<String, Object>> values = Iterables.getOnlyElement(
              Sequences.toList(pair.rhs, Lists.<Result<Map<String, Object>>>newArrayList())
          );
          final Map<String, Object> value = values.getValue();
          if (sketchQuery.getSketchOp() == SketchOp.QUANTILE) {
            for (Map.Entry<String, Object> entry : value.entrySet()) {
              final String dimension = entry.getKey();
              final ItemsSketch<String> itemsSketch = (ItemsSketch) entry.getValue();
              Map<String, Object> result = results.get(dimension);
              if (result == null) {
                results.put(dimension, result = Maps.newLinkedHashMap());
              }
              String[] quantiles = (String[]) SketchQuantilesOp.QUANTILES.calculate(itemsSketch, 11);
              Set<String> set = Sets.newTreeSet(Arrays.asList(quantiles));
              if (set.size() != quantiles.length) {
                quantiles = set.toArray(new String[set.size()]);
              }
              double[] pmf = (double[]) SketchQuantilesOp.PMF.calculate(itemsSketch, quantiles);
              double[] cdf = (double[]) SketchQuantilesOp.CDF.calculate(itemsSketch, quantiles);
              result.put("min", quantiles[0]);
              result.put("max", quantiles[quantiles.length - 1]);
              result.put("quantiles", quantiles);
              result.put("pmf", pmf);
              result.put("cdf", cdf);
              String lower = itemsSketch.getQuantile(0.25f);
              String upper = itemsSketch.getQuantile(0.75f);
              result.put("iqr", new String[]{lower, upper});
              DimFilter outlier = OrDimFilter.of(
                  new BoundDimFilter(dimension, null, lower, false, true, false, null),
                  new BoundDimFilter(dimension, upper, null, true, false, false, null)
              );

              final SearchQuery search = new SearchQuery(
                  representative.getDataSource(),
                  null,
                  QueryGranularities.ALL,
                  10,
                  representative.getQuerySegmentSpec(),
                  virtualColumns,
                  DefaultDimensionSpec.toSpec(dimension),
                  new SearchQuerySpec.TakeAll(),
                  new LexicographicSearchSortSpec(Arrays.asList("$count:desc")),
                  false,
                  null
              );
              final List<Map> frequentItems = Lists.newArrayList();
              result.put("frequentItems", frequentItems);
              futures.add(
                  exec.submit(
                      new AbstractPrioritizedCallable<Integer>(0)
                      {
                        @Override
                        public Integer call()
                        {
                          int counter = 0;
                          for (Result<SearchResultValue> result : Sequences.toList(
                              search.run(segmentWalker, Maps.<String, Object>newHashMap()),
                              Lists.<Result<SearchResultValue>>newArrayList()
                          )) {
                            SearchResultValue searchHits = result.getValue();
                            for (SearchHit searchHit : searchHits.getValue()) {
                              frequentItems.add(
                                  ImmutableMap.of("value", searchHit.getValue(), "count", searchHit.getCount())
                              );
                              counter++;
                            }
                          }
                          return counter;
                        }
                      }
                  )
              );

              final GroupByQuery groupBy = new GroupByQuery(
                  representative.getDataSource(),
                  representative.getQuerySegmentSpec(),
                  outlier,
                  QueryGranularities.ALL,
                  ImmutableList.<DimensionSpec>of(),
                  virtualColumns,
                  ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("count")),
                  ImmutableList.<PostAggregator>of(),
                  null, null, null, null, null
              );

              futures.add(
                  exec.submit(
                      new AbstractPrioritizedCallable<Integer>(0)
                      {
                        @Override
                        public Integer call()
                        {
                          int counter = 0;
                          for (Row result : Sequences.toList(
                              groupBy.run(segmentWalker, Maps.<String, Object>newHashMap()),
                              Lists.<Row>newArrayList()
                          )) {
                            counter += result.getLongMetric("count");
                          }
                          results.get(dimension).put("outliers", counter);
                          return counter;
                        }
                      }
                  )
              );
            }
          } else if (sketchQuery.getSketchOp() == SketchOp.THETA) {
            for (Map.Entry<String, Object> entry : value.entrySet()) {
              String dimension = entry.getKey();
              Sketch sketch = (Sketch) entry.getValue();
              Map<String, Object> result = results.get(dimension);
              if (result == null) {
                results.put(dimension, result = Maps.newLinkedHashMap());
              }
              if (sketch.isEstimationMode()) {
                result.put("cardinality(estimated)", sketch.getEstimate());
                result.put("cardinality(upper95)", sketch.getUpperBound(2));
                result.put("cardinality(lower95)", sketch.getLowerBound(2));
              } else {
                result.put("cardinality", sketch.getEstimate());
              }
            }
          }
        }
        Futures.getUnchecked(Futures.allAsList(futures));
        return Sequences.simple(Arrays.asList(results));
      }
    };
  }

  @Override
  public boolean hasTabularOutput()
  {
    return false;
  }
}
