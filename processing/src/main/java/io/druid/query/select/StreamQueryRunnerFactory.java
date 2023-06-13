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
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class StreamQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Object[]>
    implements QueryRunnerFactory.Splitable<Object[]>
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
  public Supplier<Object> preFactoring(
      Query<Object[]> query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Suppliers.ofInstance(new MutableInt(0));
  }

  private static final int SPLIT_MIN_ROWS = 8192;
  private static final int SPLIT_DEFAULT_ROWS = 131072;

  private static final int SPLIT_MAX_SPLIT = 32;

  @Override
  public List<Query<Object[]>> splitQuery(
      Query<Object[]> query,
      List<Segment> segments,
      Supplier<Object> optimizer,
      Supplier<RowResolver> resolver,
      QuerySegmentWalker segmentWalker
  )
  {
    StreamQuery stream = (StreamQuery) query;
    List<OrderByColumnSpec> orderingSpecs = stream.getOrderingSpecs();
    if (GuavaUtils.isNullOrEmpty(orderingSpecs) || OrderByColumnSpec.isSimpleTimeOrdered(orderingSpecs)) {
      return null;
    }
    int numSplit = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_NUM, -1);
    if (numSplit < 2) {
      int splitRows = query.getContextInt(Query.STREAM_RAW_LOCAL_SPLIT_ROWS, SPLIT_DEFAULT_ROWS);
      if (splitRows > SPLIT_MIN_ROWS) {
        int numRows = Queries.estimateNumRows(stream, segments, resolver, cache(query), splitRows);
        if (numRows >= 0) {
          logger.info("Total number of rows [%,d] spliting on [%d] rows", numRows, splitRows);
          numSplit = numRows / splitRows;
        }
      }
    }
    if (numSplit < 2) {
      return null;
    }
    numSplit = Math.min(SPLIT_MAX_SPLIT, numSplit);

    String strategy = query.getContextValue(Query.LOCAL_SPLIT_STRATEGY, "slopedSpaced");

    OrderByColumnSpec orderingSpec = stream.getOrderingSpecs().get(0);
    Map<String, String> mapping = QueryUtils.aliasMapping(query);
    String sortColumn = mapping.getOrDefault(orderingSpec.getDimension(), orderingSpec.getDimension());

    DimensionSpec ordering = orderingSpec.asDimensionSpec();
    long start = System.currentTimeMillis();
    Object[] thresholds = Queries.makeColumnHistogramOn(
        resolver, segments, segmentWalker, stream.asTimeseriesQuery(), ordering, numSplit, strategy, -1, cache(query)
    );
    long elapsed = System.currentTimeMillis() - start;
    if (thresholds == null || thresholds.length < 3) {
      return null;
    }
    logger.info("split %s on values : %s (%d msec)", sortColumn, Arrays.toString(thresholds), elapsed);

    List<Query<Object[]>> splits = Lists.newArrayList();
    for (DimFilter filter : QueryUtils.toSplitter(sortColumn, orderingSpec, thresholds)) {
      logger.debug("--> split filter : %s", filter);
      splits.add(stream.withFilter(DimFilters.and(stream.getFilter(), filter)));
    }
    return splits;
  }


  @Override
  public QueryRunner<Object[]> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return (query, response) -> engine.process((StreamQuery) query, config, segment, optimizer, cache);
  }
}
