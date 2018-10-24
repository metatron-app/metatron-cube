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

package io.druid.query.select;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.sketch.QuantileOperation;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.data.GenericIndexed;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class StreamRawQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Object[], StreamRawQuery>
    implements QueryRunnerFactory.Splitable<Object[], StreamRawQuery>
{
  private static final Logger logger = new Logger(StreamRawQueryRunnerFactory.class);

  private final StreamQueryEngine engine;

  @Inject
  public StreamRawQueryRunnerFactory(StreamRawQueryToolChest toolChest, StreamQueryEngine engine)
  {
    super(toolChest, null);
    this.engine = engine;
  }

  @Override
  public Future<Object> preFactoring(
      StreamRawQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Futures.<Object>immediateFuture(new MutableInt(0));
  }

  @Override
  public Iterable<StreamRawQuery> splitQuery(
      StreamRawQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker,
      ObjectMapper mapper
  )
  {
    int numSplit = query.getContextInt(Query.RAW_LOCAL_SPLIT_NUM, 5);
    if (GuavaUtils.isNullOrEmpty(query.getSortOn()) || numSplit < 2) {
      return null;
    }

    Object[] thresholds = null;
    String sortColumn = query.getSortOn().get(0);
    List<GenericIndexed<String>> dictionaries = Segments.findDictionaryIndexed(segments, sortColumn);
    if (!dictionaries.isEmpty()) {
      Union union = (Union) SetOperation.builder().setNominalEntries(64).build(Family.UNION);
      for (GenericIndexed<String> dictionary : dictionaries) {
        if (dictionary.hasTheta()) {
          union.update(dictionary.getTheta());
        }
      }
      int cardinality = (int) union.getResult().getEstimate();
      if (cardinality > 0) {
        numSplit = Math.max(numSplit, 1 + (cardinality >> 20));
        if (numSplit < 2) {
          return null;
        }
      }
      ItemsUnion<String> itemsUnion = ItemsUnion.getInstance(32, Ordering.natural().nullsFirst());
      for (GenericIndexed<String> dictionary : dictionaries) {
        if (dictionary.hasQuantile()) {
          itemsUnion.update(dictionary.getQuantile());
        }
      }
      if (!itemsUnion.isEmpty()) {
        thresholds = (Object[]) QuantileOperation.QUANTILES.calculate(
            itemsUnion.getResult(), QuantileOperation.slopedSpaced(numSplit + 1)
        );
      }
    }
    if (thresholds == null) {
      thresholds = Queries.makeColumnHistogramOn(
          resolver, segmentWalker, mapper, query.asTimeseriesQuery(), DefaultDimensionSpec.of(sortColumn), numSplit
      );
    }
    if (thresholds == null || thresholds.length < 3) {
      return null;
    }
    logger.info("split %s on values : %s", sortColumn, Arrays.toString(thresholds));

    List<StreamRawQuery> splits = Lists.newArrayList();
    for (int i = 1; i < thresholds.length; i++) {
      BoundDimFilter filter;
      if (i == 1) {
        filter = BoundDimFilter.lt(sortColumn, thresholds[i]);
      } else if (i < thresholds.length - 1) {
        filter = BoundDimFilter.between(sortColumn, thresholds[i - 1], thresholds[i]);
      } else {
        filter = BoundDimFilter.gte(sortColumn, thresholds[i - 1]);
      }
      logger.debug("--> filter : %s ", filter);
      splits.add(
          query.withDimFilter(DimFilters.and(query.getDimFilter(), filter))
      );
    }
    return splits;
  }

  @Override
  public QueryRunner<Object[]> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        return engine.process((StreamRawQuery) query, segment, optimizer, cache);
      }
    };
  }

  @Override
  public QueryRunner<Object[]> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Object[]>> queryRunners,
      final Future<Object> optimizer
  )
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(final Query<Object[]> query, final Map<String, Object> responseContext)
      {
        // segment ordered (or time-ordered)
        final Iterable<Sequence<Object[]>> sequences = Iterables.transform(
            queryRunners,
            new Function<QueryRunner<Object[]>, Sequence<Object[]>>()
            {
              @Override
              public Sequence<Object[]> apply(final QueryRunner<Object[]> input)
              {
                return new LazySequence<Object[]>(
                    new Supplier<Sequence<Object[]>>()
                    {
                      @Override
                      public Sequence<Object[]> get()
                      {
                        return input.run(query, responseContext);
                      }
                    }
                );
              }
            }
        );
        return QueryUtils.mergeSort(query, Lists.newArrayList(sequences));
      }
    };
  }
}
