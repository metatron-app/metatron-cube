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

package io.druid.query.select;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.Segment;
import io.druid.segment.Segments;
import io.druid.segment.column.DictionaryEncodedColumn;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class StreamQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Object[], StreamQuery>
    implements QueryRunnerFactory.Splitable<Object[], StreamQuery>
{
  private static final Logger logger = new Logger(StreamQueryRunnerFactory.class);

  private final QueryConfig config;
  private final StreamQueryEngine engine;

  @Inject
  public StreamQueryRunnerFactory(StreamQueryToolChest toolChest, StreamQueryEngine engine, QueryConfig config, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.config = config;
  }

  @Override
  public Future<Object> preFactoring(
      StreamQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Futures.<Object>immediateFuture(new MutableInt(0));
  }

  @Override
  public List<List<Segment>> splitSegments(
      StreamQuery query,
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

  private static final int SPLIT_MAX_SPLIT = 32;

  @Override
  public List<StreamQuery> splitQuery(
      StreamQuery query,
      List<Segment> segments,
      Future<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    if (GuavaUtils.isNullOrEmpty(query.getOrderingSpecs())) {
      return null;
    }
    int numSplit = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1);
    if (numSplit < 2) {
      int splitRows = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_ROWS, SPLIT_DEFAULT_ROWS);
      if (splitRows > SPLIT_MIN_ROWS) {
        int numRows = getNumRows(query, segments, segmentWalker);
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
    OrderByColumnSpec orderingSpec = query.getOrderingSpecs().get(0);
    Map<String, String> mapping = QueryUtils.aliasMapping(query);
    String sortColumn = mapping.getOrDefault(orderingSpec.getDimension(), orderingSpec.getDimension());
    List<DictionaryEncodedColumn> dictionaries = Segments.findDictionaryWithSketch(segments, sortColumn);
    try {
      if (dictionaries.size() << 2 > segments.size()) {
        numSplit = Math.min(SPLIT_MAX_SPLIT, Queries.getNumSplits(dictionaries, numSplit));
        if (numSplit < 2) {
          return null;
        }
        thresholds = Queries.getThresholds(dictionaries, numSplit, strategy, -1, orderingSpec.getComparator());
      }
    }
    finally {
      GuavaUtils.closeQuietly(dictionaries);
    }
    if (thresholds == null) {
      DimensionSpec ordering = orderingSpec.asDimensionSpec();
      thresholds = Queries.makeColumnHistogramOn(
          resolver, segments, segmentWalker, query.asTimeseriesQuery(), ordering, numSplit, strategy, -1
      );
    }
    if (thresholds == null || thresholds.length < 3) {
      return null;
    }
    logger.info("split %s on values : %s", sortColumn, Arrays.toString(thresholds));

    Direction direction = orderingSpec.getDirection();
    List<StreamQuery> splits = Lists.newArrayList();
    for (int i = 1; i < thresholds.length; i++) {
      BoundDimFilter filter;
      if (i == 1) {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.lt(sortColumn, thresholds[i]) :
                     BoundDimFilter.gte(sortColumn, thresholds[i]);
      } else if (i < thresholds.length - 1) {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.between(sortColumn, thresholds[i - 1], thresholds[i]) :
                     BoundDimFilter.between(sortColumn, thresholds[i], thresholds[i - 1]);
      } else {
        filter = direction == Direction.ASCENDING ?
                     BoundDimFilter.gte(sortColumn, thresholds[i - 1]) :
                     BoundDimFilter.lt(sortColumn, thresholds[i - 1]);
      }
      if (!orderingSpec.isNaturalOrdering()) {
        filter = filter.withComparatorType(orderingSpec.getDimensionOrder());
      }
      logger.debug("--> filter : %s", filter);
      splits.add(
          query.withFilter(DimFilters.and(query.getFilter(), filter))
      );
    }
    return splits;
  }

  private int getNumRows(StreamQuery query, List<Segment> segments, QuerySegmentWalker segmentWalker)
  {
    int numRows = 0;
    if (query.getFilter() == null) {
      for (Segment segment : segments) {
        numRows += segment.getNumRows();
      }
      return numRows;
    }
    final DataSource ds = query.getDataSource();
    final DimFilter filter = query.getFilter();
    final Map<String, Object> context = BaseQuery.copyContextForMeta(query);
    context.put(Query.DISABLE_LOG, true);
    for (Segment segment : segments) {
      SelectMetaQuery meta = SelectMetaQuery.of(
          ds, new SpecificSegmentSpec(((Segment.WithDescriptor) segment).getDescriptor()), filter, context
      );
      Result<SelectMetaResultValue> result = Sequences.only(meta.run(segmentWalker, null), null);
      if (result != null) {
        numRows += result.getValue().getTotalCount();
      }
    }
    return numRows;
  }

  @Override
  public QueryRunner<Object[]> _createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        return engine.process((StreamQuery) query, config, segment, optimizer, cache);
      }
    };
  }
}
