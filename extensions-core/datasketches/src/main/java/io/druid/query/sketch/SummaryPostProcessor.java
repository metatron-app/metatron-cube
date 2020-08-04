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
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.PrioritizedCallable;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.UnionAllQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.corr.PearsonAggregatorFactory;
import io.druid.query.aggregation.kurtosis.KurtosisAggregatorFactory;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import io.druid.query.aggregation.variance.VarianceAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.LexicographicSearchSortSpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 */
@JsonTypeName("sketch.summary")
public class SummaryPostProcessor extends PostProcessingOperator.UnionSupport implements PostProcessingOperator.Local
{
  private static final Logger LOG = new Logger(SimilarityProcessingOperator.class);

  private static final int DEFAULT_ROUND = 4;

  private final int round;
  private final boolean includeTimeStats;
  private final boolean includeCovariance;
  private final Map<String, Map<String, Integer>> typeDetail;
  private final QuerySegmentWalker segmentWalker;

  @JsonCreator
  public SummaryPostProcessor(
      @JsonProperty("round") Integer round,
      @JsonProperty("includeTimeStats") boolean includeTimeStats,
      @JsonProperty("includeCovariance") boolean includeCovariance,
      @JsonProperty("typeDetail") Map<String, Map<String, Integer>> typeDetail,
      @JacksonInject QuerySegmentWalker segmentWalker
  )
  {
    this.round = round == null ? DEFAULT_ROUND : round;
    this.includeTimeStats = includeTimeStats;
    this.includeCovariance = includeCovariance;
    this.typeDetail = typeDetail;
    this.segmentWalker = segmentWalker;
  }

  @Override
  public QueryRunner postProcess(QueryRunner baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner, final ExecutorService exec)
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
        final ListeningExecutorService listening = MoreExecutors.listeningDecorator(exec);
        final List<VirtualColumn> virtualColumns = ((SketchQuery) representative).getVirtualColumns();
        final List<ListenableFuture<Integer>> futures = Lists.newArrayList();
        final Map<String, Map<String, Object>> results = Maps.newLinkedHashMap();
        final Sequence<Pair<Query, Sequence<Object[]>>> sequences = baseRunner.run(query, responseContext);
        for (Pair<Query, Sequence<Object[]>> pair : Sequences.toList(sequences)) {
          SketchQuery sketchQuery = (SketchQuery) pair.lhs;

          final List<String> columns = sketchQuery.estimatedOutputColumns();
          final Object[] values = Sequences.only(pair.rhs, null);
          if (values == null) {
            continue;   // invalid interval or not-existing
          }
          if (sketchQuery.getSketchOp() == SketchOp.QUANTILE) {
            final Map<String, ValueType> primitiveColumns = Maps.newTreeMap();
            final Map<String, ValueType> numericColumns = Maps.newTreeMap();
            for (int i = 1; i < values.length; i++) {
              if (values[i] == null) {
                continue;   // empty or not-existing
              }
              final String column = columns.get(i);
              final ValueDesc type = ((TypedSketch<ItemsSketch>) values[i]).type();
              if (type.isPrimitive()) {
                primitiveColumns.put(column, type.type());
              }
              if (type.isPrimitiveNumeric()) {
                numericColumns.put(column, type.type());
              }
            }
            List<AggregatorFactory> aggregators = Lists.newArrayList();
            if (includeCovariance && numericColumns.size() >= 2) {
              List<String> columnNames = Lists.newArrayList(numericColumns.keySet());
              for (int i = 0; i < columnNames.size(); i++) {
                for (int j = i + 1; j < columnNames.size(); j++) {
                  String from = columnNames.get(i);
                  String to = columnNames.get(j);
                  String name = "covariance(" + from + "," + to + ")";
                  aggregators.add(new PearsonAggregatorFactory(name, from, to, null, "double"));
                }
              }
            }
            aggregators.add(new CountAggregatorFactory("count"));

            TimeseriesQuery runner = new TimeseriesQuery(
                representative.getDataSource(),
                representative.getQuerySegmentSpec(),
                false,
                null,
                Granularities.ALL,
                virtualColumns,
                aggregators,
                ImmutableList.<PostAggregator>of(),
                null,
                null,
                null,
                null,
                BaseQuery.copyContextForMeta(query.withOverriddenContext(Query.ALL_DIMENSIONS_FOR_EMPTY, false))
            );
            for (int i = 1; i < values.length; i++) {
              if (values[i] == null) {
                continue;   // empty or not-existing
              }
              final String column = columns.get(i);
              final TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) values[i];
              Map<String, Object> result = results.get(column);
              if (result == null) {
                results.put(column, result = Maps.newLinkedHashMap());
              }
              final ItemsSketch itemsSketch = sketch.value();
              final ValueDesc type = sketch.type();
              Object[] quantiles = (Object[]) QuantileOperation.QUANTILES.calculate(
                  itemsSketch,
                  QuantileOperation.DEFAULT_QUANTILE_PARAM
              );
              result.put("type", type);
              if (typeDetail != null && typeDetail.containsKey(column)) {
                result.put("typeDetail", typeDetail.get(column));
              }
              result.put("min", quantiles[0]);
              result.put("max", quantiles[quantiles.length - 1]);
              result.put("median", quantiles[5]);

              Object[] dedup = dedup(quantiles);
              result.put("quantiles", dedup);
              if (dedup.length > 1) {
                Object[] splits = Arrays.copyOfRange(dedup, 1, dedup.length - 1); // remove min value
                result.put("pmf", QuantileOperation.PMF.calculate(itemsSketch, splits));
                result.put("cdf", QuantileOperation.CDF.calculate(itemsSketch, splits));
              }

              Object lower = itemsSketch.getQuantile(0.25f);
              Object upper = itemsSketch.getQuantile(0.75f);
              result.put("iqr", new Object[]{lower, upper});
              result.put("count", itemsSketch.getN());

              if (type.isPrimitiveNumeric()) {
                double q1 = ((Number) lower).doubleValue();
                double q3 = ((Number) upper).doubleValue();
                double delta = (q3 - q1) * 1.5;
                result.put("outlierThreshold", new double[]{q1 - delta, q3 + delta});
              }

              if (type.isStringOrDimension()) {
                final SearchQuery search = new SearchQuery(
                    representative.getDataSource(),
                    null,
                    Granularities.ALL,
                    10,
                    representative.getQuerySegmentSpec(),
                    virtualColumns,
                    DefaultDimensionSpec.toSpec(column),
                    new SearchQuerySpec.TakeAll(),
                    new LexicographicSearchSortSpec(Arrays.asList("$count:desc")),
                    false,
                    BaseQuery.copyContextForMeta(query)
                );
                final List<Map> frequentItems = Lists.newArrayList();
                result.put("frequentItems", frequentItems);
                futures.add(
                    listening.submit(
                        new PrioritizedCallable.Background<Integer>()
                        {
                          @Override
                          public Integer call()
                          {
                            int counter = 0;
                            for (Result<SearchResultValue> result : Sequences.toList(
                                QueryRunners.run(search, segmentWalker))) {
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
              if (type.isPrimitive()) {
                runner = configureForType(runner, column, lower, upper, type);
              }
            }
            final TimeseriesQuery timeseries = runner;
            futures.add(
                listening.submit(
                    new PrioritizedCallable.Background<Integer>()
                    {
                      @Override
                      public Integer call()
                      {
                        List<Row> rows = Sequences.toList(QueryRunners.run(timeseries, segmentWalker));
                        if (rows.isEmpty()) {
                          return 0;
                        }
                        Row row = Iterables.getOnlyElement(rows);
                        for (String column : primitiveColumns.keySet()) {
                          ValueType type = primitiveColumns.get(column);
                          Map<String, Object> stats = results.get(column);
                          if (type.isNumeric()) {
                            stats.put("zeros", row.getLong(column + ".missing"));
                          } else {
                            stats.put("missing", row.getLong(column + ".missing"));
                          }
                          if (type.isNumeric()) {
                            stats.put("mean", Rows.round(row.getDouble(column + ".mean"), round));
                            stats.put("variance", Rows.round(row.getDouble(column + ".variance"), round));
                            stats.put("stddev", Rows.round(row.getDouble(column + ".stddev"), round));
                            stats.put("skewness", Rows.round(row.getDouble(column + ".skewness"), round));
                            stats.put("outliers", row.getLong(column + ".outlier"));
                          }
                          if (includeCovariance && type.isNumeric()) {
                            String covariance = "covariance(" + column + ",";
                            for (String key : row.getColumns()) {
                              if (key.startsWith(covariance) && key.endsWith(")")) {
                                stats.put(
                                    "covariance." + key.substring(covariance.length(), key.length() - 1),
                                    Rows.round(row.getDoubleMetric(key), round)
                                );
                              }
                            }
                          }
                        }
                        return 0;
                      }
                    }
                )
            );
          } else if (sketchQuery.getSketchOp() == SketchOp.THETA) {
            for (int i = 1; i < values.length; i++) {
              final String column = columns.get(i);
              final TypedSketch<Sketch> sketch = ((TypedSketch<Sketch>) values[i]);
              if (sketch == null) {
                continue;
              }
              Map<String, Object> result = results.get(column);
              if (result == null) {
                results.put(column, result = Maps.newLinkedHashMap());
              }
              Sketch thetaSketch = sketch.value();
              if (thetaSketch.isEstimationMode()) {
                result.put("cardinality(estimated)", Math.round(thetaSketch.getEstimate()));
                result.put("cardinality(upper95)", Math.round(thetaSketch.getUpperBound(2)));
                result.put("cardinality(lower95)", Math.round(thetaSketch.getLowerBound(2)));
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
              null,
              new NoneColumnIncluderator(),
              null,
              false,
              SegmentMetadataQuery.DEFAULT_NON_COLUMN_STATS,
              false,
              false,
              BaseQuery.copyContextForMeta(query)
          );
          final Map<String, Object> stats = Maps.newLinkedHashMap();
          final List<Map<String, Object>> segments = Lists.newArrayList();
          stats.put("segments", segments);
          futures.add(
              listening.submit(
                  new PrioritizedCallable.Background<Integer>()
                  {
                    @Override
                    public Integer call()
                    {
                      for (SegmentAnalysis meta : Sequences.toList(QueryRunners.run(metaQuery, segmentWalker))) {
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

  private TimeseriesQuery configureForType(
      TimeseriesQuery query,
      String column,
      Object lower,
      Object upper,
      ValueDesc type
  )
  {
    String escaped = StringUtils.identifier(column);

    List<AggregatorFactory> aggregators = Lists.newArrayList();
    List<PostAggregator> postAggregators = Lists.newArrayList();
    if (type.isPrimitiveNumeric()) {
      aggregators.add(CountAggregatorFactory.predicate(column + ".missing", escaped + " == 0"));
    } else {
      aggregators.add(CountAggregatorFactory.predicate(column + ".missing", "isnull(" + escaped + ")"));
    }

    if (type.isPrimitiveNumeric()) {
      ValueDesc inputType = ValueDesc.of(type.typeName());
      double q1 = ((Number) lower).doubleValue();
      double q3 = ((Number) upper).doubleValue();
      double iqr = q3 - q1;
      String outlier = escaped + " < " + (q1 - iqr * 1.5) + " || " + escaped + " >  " + (q3 + iqr * 1.5);
      aggregators.add(CountAggregatorFactory.predicate(column + ".outlier", outlier));
      aggregators.add(new GenericSumAggregatorFactory(column + ".sum", column, inputType));
      aggregators.add(new VarianceAggregatorFactory(column + ".variance", column, inputType));
      aggregators.add(new KurtosisAggregatorFactory(column + ".skewness", column, null, inputType));

      postAggregators.add(new MathPostAggregator(column + ".mean", StringUtils.identifier(column + ".sum") + "/count"));
      postAggregators.add(new StandardDeviationPostAggregator(column + ".stddev", column + ".variance", null));
    }

    return query.withAggregatorSpecs(GuavaUtils.concat(query.getAggregatorSpecs(), aggregators))
                .withPostAggregatorSpecs(GuavaUtils.concat(query.getPostAggregatorSpecs(), postAggregators));
  }
}
