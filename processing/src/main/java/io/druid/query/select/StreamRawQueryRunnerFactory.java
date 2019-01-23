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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.column.DictionaryEncodedColumn;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.Collections;
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
  public List<List<Segment>> splitSegments(
      StreamRawQuery query,
      List<Segment> targets,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    return null;
  }

  private static final int SPLIT_MIN_ROWS = 8192;
  private static final int SPLIT_DEFAULT_ROWS = 131072;

  private static final int SPLIT_MAX_SPLIT = 12;

  @Override
  public Iterable<StreamRawQuery> splitQuery(
      StreamRawQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    if (GuavaUtils.isNullOrEmpty(query.getOrderBySpecs())) {
      return null;
    }
    int numSplit = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1);
    if (numSplit < 2) {
      int splitRows = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_ROWS, SPLIT_DEFAULT_ROWS);
      if (splitRows > SPLIT_MIN_ROWS) {
        int numRows = 0;
        DataSource ds = query.getDataSource();
        DimFilter filter = query.getDimFilter();
        Map<String, Object> context = BaseQuery.copyContextForMeta(query);
        for (Segment segment : segments) {
          SelectMetaQuery meta = SelectMetaQuery.of(
              ds, new SpecificSegmentSpec(((Segment.WithDescriptor) segment).getDescriptor()), filter, context
          );
          Result<SelectMetaResultValue> result = Sequences.only(meta.run(segmentWalker, null), null);
          if (result != null) {
            numRows += result.getValue().getTotalCount();
          }
        }
        logger.info("Total number of rows [%,d] spliting on [%d] rows", numRows, splitRows);
        numSplit = numRows / splitRows;
      }
    }
    if (numSplit < 2) {
      return null;
    }
    numSplit = Math.min(SPLIT_MAX_SPLIT, numSplit);

    String strategy = query.getContextValue(Query.LOCAL_SPLIT_STRATEGY, "slopedSpaced");

    Object[] thresholds = null;
    String sortColumn = query.getOrderBySpecs().get(0).getDimension();
    List<DictionaryEncodedColumn> dictionaries = Segments.findDictionaryWithSketch(segments, sortColumn);
    try {
      if (dictionaries.size() << 2 > segments.size()) {
        numSplit = Math.min(SPLIT_MAX_SPLIT, Queries.getNumSplits(dictionaries, numSplit));
        if (numSplit < 2) {
          return null;
        }
        thresholds = Queries.getThresholds(dictionaries, numSplit, strategy);
      }
    }
    finally {
      GuavaUtils.closeQuietly(dictionaries);
    }
    if (thresholds == null) {
      DimensionSpec dimensionSpec = DefaultDimensionSpec.of(sortColumn);
      thresholds = Queries.makeColumnHistogramOn(
          resolver, segmentWalker, query.asTimeseriesQuery(), dimensionSpec, numSplit, strategy
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
    if (query.isDescending()) {
      splits = Lists.reverse(splits);
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
        StreamRawQuery stream = (StreamRawQuery) query;
        if (!GuavaUtils.isNullOrEmpty(stream.getSortOn())) {
          stream = stream.withLimit(-1);   // should be done after merge
        }
        return engine.process(stream, segment, optimizer, cache);
      }
    };
  }

  @Override
  public QueryRunner<Object[]> mergeRunners(
      final ExecutorService executor,
      final Iterable<QueryRunner<Object[]>> queryRunners,
      final Future<Object> optimizer
  )
  {
    final List<QueryRunner<Object[]>> runners = Lists.newArrayList(queryRunners);
    if (runners.isEmpty()) {
      return QueryRunners.empty();
    }
    if (runners.size() == 1) {
      return new QueryRunner<Object[]>()
      {
        @Override
        public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
        {
          return runners.get(0).run(query, responseContext);
        }
      };
    }
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(final Query<Object[]> query, final Map<String, Object> responseContext)
      {
        StreamRawQuery stream = (StreamRawQuery) query;
        Iterable<Sequence<Object[]>> iterable;
        if (GuavaUtils.isNullOrEmpty(stream.getOrderBySpecs())) {
          iterable = Iterables.transform(
              QueryRunnerHelper.asCallable(runners, query, responseContext),
              Sequences.<Object[]>callableToLazy()
          );
        } else {
          Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(4, runners.size()));
          iterable = Iterables.transform(
              Execs.execute(executor, QueryRunnerHelper.asCallable(runners, semaphore, query, responseContext)),
              FutureSequence.<Object[]>toSequence()
          );
        }
        List<Sequence<Object[]>> sequences = Lists.newArrayList(iterable);
        if (stream.isDescending()) {
          Collections.reverse(sequences);
        }
        // no need to sort on time in here (not like CCC)
        return QueryUtils.mergeSort(query, sequences);
      }
    };
  }
}
