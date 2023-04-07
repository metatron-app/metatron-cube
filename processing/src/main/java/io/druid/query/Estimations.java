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

package io.druid.query;

import io.druid.common.IntTagged;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SemiJoinFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;

import java.util.List;
import java.util.Map;

public class Estimations
{
  private static final Logger LOG = new Logger(JoinQuery.class);

  private static final int TRIVIAL_SIZE = 100;

  private static Estimation applyLimit(Estimation estimated, Query<?> query)
  {
    LimitSpec limitSpec = BaseQuery.getLimitSpec(query);
    if (limitSpec != null && limitSpec.hasLimit()) {
      estimated = estimated.applyLimit(limitSpec.getLimit());
    }
    return estimated;
  }

  private static final float MIN_SAMPLING = 6000;

  public static Estimation estimate(
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context,
      QuerySegmentWalker segmentWalker
  )
  {
    if (dataSource instanceof QueryDataSource) {
      Query<Object> query = ((QueryDataSource) dataSource).getQuery();
      return estimate(query, segmentSpec, context, segmentWalker);
    }
    if (dataSource instanceof ViewDataSource) {
      ViewDataSource view = (ViewDataSource) dataSource;
      return Estimation.of(
          Queries.filterSelectivity(
              TableDataSource.of(view.getName()),
              segmentSpec,
              view.getVirtualColumns(),
              view.getFilter(),
              BaseQuery.copyContextForMeta(context),
              segmentWalker
          )
      );
    }
    return Estimation.of(
        Queries.filterSelectivity(
            dataSource,
            segmentSpec,
            null,
            BaseQuery.copyContextForMeta(context),
            segmentWalker
        )
    );
  }

  private static Estimation estimate(
      Query query,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context,
      QuerySegmentWalker segmentWalker
  )
  {
    Estimation estimation = estimationFromContext(query);
    if (estimation != null) {
      return estimation.degrade();
    }
    DataSource source = query.getDataSource();
    if (source instanceof QueryDataSource || source instanceof ViewDataSource) {
      Estimation estimated = estimate(source, segmentSpec, context, segmentWalker);
      if (BaseQuery.getDimFilter(query) != null) {
        estimated.update(x -> x >>> 1);
      }
      if (query instanceof TimeseriesQuery) {
        estimated.update(Queries.estimateCardinality((TimeseriesQuery) query, segmentWalker)[0]);
      } else if (query instanceof GroupByQuery || query instanceof TopNQuery) {
        estimated.update(x -> (long) (x * (1 - Math.pow(0.4, BaseQuery.getDimensions(query).size()))));
      }
      return applyLimit(estimated, query);
    }
    LimitSpec limitSpec = BaseQuery.getLimitSpec(query);
    if (limitSpec != null && limitSpec.hasLimit() && limitSpec.getLimit() < TRIVIAL_SIZE) {
      return Estimation.of(limitSpec.getLimit(), 1f);
    }
    if (query instanceof TimeseriesQuery) {
      TimeseriesQuery timeseries = (TimeseriesQuery) query;
      return Estimation.of(Queries.estimateCardinality(timeseries, segmentWalker));
    } else if (query instanceof GroupByQuery) {
      GroupByQuery groupBy = (GroupByQuery) query;
      long[] selectivity = Queries.filterSelectivity(groupBy, segmentWalker);
      if (selectivity[0] <= TRIVIAL_SIZE || selectivity[1] == 0) {
        return Estimation.of(selectivity);
      }
      long start = System.currentTimeMillis();
      List<DimensionSpec> dimensions = groupBy.getDimensions();
      DimensionSamplingQuery sampling = groupBy.toSampling(Math.min(0.05f, MIN_SAMPLING / selectivity[1]));
      IntTagged<Object2IntSortedMap<?>> mapping = SemiJoinFactory.toMap(
          dimensions.size(), Sequences.toIterator(sampling.run(segmentWalker, null))
      );
      Estimation estimated = Estimation.of(selectivity);
//      long cardinality = Queries.estimateCardinality(groupBy.withHavingSpec(null), segmentWalker);
      long cardinality = Math.min(estimated.estimated, estimateBySample(mapping, estimated.estimated));
      if (groupBy.getHavingSpec() == null) {
        estimated.estimated = cardinality;
      } else {
        long threshold = segmentWalker.getJoinConfig().anyMinThreshold();
        if (threshold > 0 && estimated.gt(threshold << 1) && DimensionSpecs.isAllDefault(dimensions)) {
          DimFilter filter = SemiJoinFactory.toFilter(DimensionSpecs.toInputNames(dimensions), mapping.value());
          Query<Row> sampler = groupBy.prepend(filter)
                                      .withOverriddenContext(Query.GBY_LOCAL_SPLIT_CARDINALITY, -1)
                                      .withOverriddenContext("$skip", true);   // for test hook
          int size = SemiJoinFactory.sizeOf(filter);
          int passed = Sequences.size(QueryUtils.resolve(sampler, segmentWalker).run(segmentWalker, null));
          estimated.estimated = Math.max(1, cardinality * passed / size);
          estimated.selectivity *= (float) passed / size;
          LOG.debug("--- 'having' selectivity by sampling: %f", (float) passed / size);
        } else {
          estimated.estimated = Math.max(1, cardinality >> 1);
          estimated.selectivity = 0.5f;
        }
      }
      LOG.debug(
          "--- %s is estimated to %d rows by sampling %d rows from %d rows in %,d msec",
          groupBy.getDataSource(), estimated.estimated, mapping.tag, selectivity[0], System.currentTimeMillis() - start
      );
      return applyLimit(estimated, query);
    }
    return applyLimit(Estimation.of(Queries.filterSelectivity(query, segmentWalker)), query);
  }

  private static long estimateBySample(IntTagged<Object2IntSortedMap<?>> mapping, long N)
  {
    final int n = mapping.tag;
    final Object2IntMap<?> samples = mapping.value;

    float q = (float) n / N;
    final float d = samples.size();

    float f1 = samples.object2IntEntrySet().stream().filter(e -> e.getIntValue() == 1).count();

//    final IntIterator counts = samples.values().iterator();
//    final Int2IntOpenHashMap fn = new Int2IntOpenHashMap();
//    while (counts.hasNext()) {
//      fn.addTo(counts.nextInt(), 1);
//    }
//    float f1 = fn.get(1);
//
//    float sum = 0;
//    float numerator = 0f;
//    float denominator = 0f;
//    for (Int2IntMap.Entry entry : fn.int2IntEntrySet()) {
//      int i = entry.getIntKey();
//      int fi = entry.getIntValue();
//      numerator += Math.pow(1 - q, i) * fi;
//      denominator += i * q * Math.pow(1 - q, i - 1) * fi;
//      sum += i * (i - 1) * fi;
//    }
//    double Dsh = d + f1 * numerator / denominator;
//
//    float f1n = 1 - f1 / n;
//    double Dcl = d + f1 * d * sum / (Math.pow(n, 2) - n - 1) / f1n / f1n;
//    double Dchar = d + f1 * (Math.sqrt(1 / q) - 1);
//    double Dchao = d + 0.5 * Math.pow(f1, 2) / (d - f1);
//    LOG.info("Dsh = %.2f, Dcl = %.2f, Dchar = %.2f, Dchao = %.2f", Dsh, Dcl, Dchar, Dchao);

    if (f1 == d) {
      return (long) (d * Math.sqrt(1 / q));   // Dchar
    } else {
      return (long) (d + 0.5 * Math.pow(f1, 2) / (d - f1));   // Dchao
    }
  }

  public static Query propagate(Query<?> query, Estimation estimation)
  {
    return propagate(query, estimation.estimated, estimation.selectivity);
  }

  public static Query propagate(Query<?> query, long estimation, float selectivity)
  {
    return query.withOverriddenContext(propagate(query.getContext(), estimation, selectivity));
  }

  public static Map<String, Object> propagate(Map<String, Object> context, Estimation estimation)
  {
    return propagate(context, estimation.estimated, estimation.selectivity);
  }

  public static Map<String, Object> propagate(Map<String, Object> context, long estimation, float selectivity)
  {
    return BaseQuery.copyContextForMeta(context, JoinQuery.CARDINALITY, estimation, JoinQuery.SELECTIVITY, selectivity);
  }

  private static Estimation estimationFromContext(Query<?> query)
  {
    int cardinality = JoinQuery.getCardinality(query);
    if (cardinality >= 0) {
      float selectivity = JoinQuery.getSelectivity(query);
      return new Estimation(cardinality, selectivity);
    }
    return null;
  }

  public static long joinEstimation(JoinElement element, Estimation leftEstimated, Estimation rightEstimated)
  {
    if (element.isCrossJoin()) {
      return leftEstimated.estimated * rightEstimated.estimated;
    }
    float estimation = 0;
    switch (element.getJoinType()) {
      case INNER:
        if (leftEstimated.gt(rightEstimated)) {
          estimation = leftEstimated.estimated * rightEstimated.selectivity;
        } else {
          estimation = rightEstimated.estimated * leftEstimated.selectivity;
        }
        break;
      case LO:
        estimation = leftEstimated.estimated;
        break;
      case RO:
        estimation = rightEstimated.estimated;
        break;
      case FULL:
        estimation = leftEstimated.estimated + rightEstimated.estimated;
        break;
    }
    return Math.max(1, (long) estimation);
  }

  public static Query mergeSelectivity(Query<?> query, float selectivity)
  {
    if (selectivity < 0) {
      return query;
    }
    Estimation estimation = estimationFromContext(query);
    if (estimation != null && selectivity < estimation.selectivity) {
      Estimation updated = estimation.duplicate().update(selectivity);
      LOG.debug("--- selectivity %.3f merged into %s(%s to %s)", selectivity, query.getDataSource(), estimation, updated);
      return propagate(query, updated);
    }
    return query;
  }
}
