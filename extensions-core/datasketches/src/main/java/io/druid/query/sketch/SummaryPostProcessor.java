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
import com.fasterxml.jackson.annotation.JsonProperty;
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
import io.druid.data.ValueType;
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
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.kurtosis.KurtosisAggregatorFactory;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import io.druid.query.aggregation.variance.VarianceAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.LexicographicSearchSortSpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.lang.reflect.Array;
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

  private final boolean includeTimeStats;
  private final QuerySegmentWalker segmentWalker;
  private final ListeningExecutorService exec;

  @JsonCreator
  public SummaryPostProcessor(
      @JsonProperty("includeTimeStats") boolean includeTimeStats,
      @JacksonInject QuerySegmentWalker segmentWalker,
      @JacksonInject @Processing ExecutorService exec
  )
  {
    this.includeTimeStats = includeTimeStats;
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
      public Sequence run(final Query query, Map responseContext)
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
              final String column = entry.getKey();
              final TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) entry.getValue();
              Map<String, Object> result = results.get(column);
              if (result == null) {
                results.put(column, result = Maps.newLinkedHashMap());
              }
              final ItemsSketch itemsSketch = sketch.value();
              Object[] quantiles = (Object[]) SketchQuantilesOp.QUANTILES.calculate(itemsSketch, 11);
              result.put("min", quantiles[0]);
              result.put("max", quantiles[quantiles.length - 1]);
              result.put("median", quantiles[5]);

              Object[] dedup = dedup(quantiles);
              double[] pmf = (double[]) SketchQuantilesOp.PMF.calculate(itemsSketch, dedup);
              double[] cdf = (double[]) SketchQuantilesOp.CDF.calculate(itemsSketch, dedup);
              result.put("quantiles", dedup);
              result.put("pmf", pmf);
              result.put("cdf", cdf);

              Object lower = itemsSketch.getQuantile(0.25f);
              Object upper = itemsSketch.getQuantile(0.75f);
              result.put("iqr", new Object[]{lower, upper});
              result.put("count", itemsSketch.getN());

              if (sketch.type() == ValueType.STRING) {
                final SearchQuery search = new SearchQuery(
                    representative.getDataSource(),
                    null,
                    QueryGranularities.ALL,
                    10,
                    representative.getQuerySegmentSpec(),
                    virtualColumns,
                    DefaultDimensionSpec.toSpec(column),
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
              }

              if (sketch.type() != ValueType.COMPLEX) {
                GroupByQuery baseQuery = new GroupByQuery(
                    representative.getDataSource(),
                    representative.getQuerySegmentSpec(),
                    null,
                    QueryGranularities.ALL,
                    ImmutableList.<DimensionSpec>of(),
                    virtualColumns,
                    ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("count")),
                    ImmutableList.<PostAggregator>of(),
                    null, null, null, null, null
                );

                final GroupByQuery groupBy = configureForType(baseQuery, column, lower, upper, sketch.type());

                final Map<String, Object> stats = results.get(column);
                futures.add(
                    exec.submit(
                        new AbstractPrioritizedCallable<Integer>(0)
                        {
                          @Override
                          public Integer call()
                          {
                            List<Row> rows = Sequences.toList(
                                groupBy.run(segmentWalker, Maps.<String, Object>newHashMap()),
                                Lists.<Row>newArrayList()
                            );
                            if (rows.isEmpty()) {
                              return 0;
                            }
                            Row row = Iterables.getOnlyElement(rows);
                            stats.put("outliers", row.getLongMetric("outlier"));
                            stats.put("missing", row.getLongMetric("missing"));
                            if (ValueType.isNumeric(sketch.type())) {
                              stats.put("mean", row.getDoubleMetric("mean"));
                              stats.put("variance", row.getDoubleMetric("variance"));
                              stats.put("stddev", row.getDoubleMetric("stddev"));
                              stats.put("skewness", row.getDoubleMetric("skewness"));
                            }
                            return 0;
                          }
                        }
                    )
                );
              }
            }
          } else if (sketchQuery.getSketchOp() == SketchOp.THETA) {
            for (Map.Entry<String, Object> entry : value.entrySet()) {
              String dimension = entry.getKey();
              TypedSketch<Sketch> sketch = (TypedSketch<Sketch>) entry.getValue();
              Map<String, Object> result = results.get(dimension);
              if (result == null) {
                results.put(dimension, result = Maps.newLinkedHashMap());
              }
              Sketch thetaSketch = sketch.value();
              if (thetaSketch.isEstimationMode()) {
                result.put("cardinality(estimated)", thetaSketch.getEstimate());
                result.put("cardinality(upper95)", thetaSketch.getUpperBound(2));
                result.put("cardinality(lower95)", thetaSketch.getLowerBound(2));
              } else {
                result.put("cardinality", thetaSketch.getEstimate());
              }
            }
          }
        }
        if (includeTimeStats) {
          final SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
              representative.getDataSource(),
              representative.getQuerySegmentSpec(),
              new NoneColumnIncluderator(),
              false,
              null,
              SegmentMetadataQuery.DEFAULT_NON_COLUMN_STATS,
              false,
              false
          );
          final Map<String, Object> stats = Maps.newLinkedHashMap();
          final List<Map<String, Object>> segments = Lists.newArrayList();
          stats.put("segments", segments);
          futures.add(
              exec.submit(
                  new AbstractPrioritizedCallable<Integer>(0)
                  {
                    @Override
                    public Integer call()
                    {
                      for (SegmentAnalysis meta : Sequences.toList(
                          metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
                          Lists.<SegmentAnalysis>newArrayList()
                      )) {
                        Map<String, Object> value = Maps.newLinkedHashMap();
                        value.put("interval", Iterables.getOnlyElement(meta.getIntervals()));
                        value.put("rows", meta.getNumRows());
                        if (meta.getIngestedNumRows() > 0) {
                          value.put("ingestedRows", meta.getIngestedNumRows());
                        }
                        value.put("serializedSize", meta.getSerializedSize());
                        value.put("lastAccessTime", new DateTime(meta.getLastAccessTime()));

                        segments.add(value);
                      }
                      return 0;
                    }
                  }
              )
          );
          results.put(Column.TIME_COLUMN_NAME, stats);
        }

        Futures.getUnchecked(Futures.allAsList(futures));
        return Sequences.simple(Arrays.asList(results));
      }
    };
  }

  private Object[] dedup(Object[] quantiles)
  {
    Set<Object> set = Sets.newLinkedHashSet();
    for (Object quantile : quantiles) {
      if (!set.contains(quantile)) {
        set.add(quantile);
      }
    }
    if (set.size() != quantiles.length) {
      quantiles = set.toArray((Object[]) Array.newInstance(quantiles.getClass().getComponentType(), set.size()));
    }
    return quantiles;
  }

  private GroupByQuery configureForType(
      GroupByQuery groupBy,
      String column,
      Object lower,
      Object upper,
      ValueType type
  )
  {
    String escaped = "\"" + column + "\"";
    String isNull = "isnull(" + escaped + ")";
    if (type == ValueType.STRING) {
      return groupBy
          .withAggregatorSpecs(
              ImmutableList.<AggregatorFactory>of(
                  new CountAggregatorFactory("outlier"),
                  new CountAggregatorFactory("missing", isNull)
              )
          )
          .withDimFilter(
              OrDimFilter.of(
                  BoundDimFilter.lt(column, (String) lower),
                  BoundDimFilter.gt(column, (String) upper)
              )
          );
    }
    String outlier = escaped + " < " + lower + " || " + escaped + " >  " + upper;
    return groupBy
        .withAggregatorSpecs(
            ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory("count"),
                new CountAggregatorFactory("outlier", outlier),
                new CountAggregatorFactory("missing", isNull),
                new GenericSumAggregatorFactory("sum", column, null),
                new VarianceAggregatorFactory("variance", column),
                new KurtosisAggregatorFactory("skewness", column, null, null)
            )
        )
        .withPostAggregatorSpecs(
            ImmutableList.<PostAggregator>of(
                new MathPostAggregator("mean", "sum / count"),
                new StandardDeviationPostAggregator("stddev", "variance", null)
            )
        );
  }

  @Override
  public boolean hasTabularOutput()
  {
    return false;
  }
}