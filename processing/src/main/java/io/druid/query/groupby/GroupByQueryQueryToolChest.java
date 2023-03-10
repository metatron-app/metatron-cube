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

package io.druid.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.inject.Inject;
import io.druid.collections.StupidPool;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.ISE;
import io.druid.query.BaseAggregationQueryToolChest;
import io.druid.query.JoinQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryUtils;
import io.druid.query.RowSignature;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQueryEngine.KeyValue;
import io.druid.query.groupby.GroupByQueryEngine.RowIterator;
import io.druid.query.select.TableFunctionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.ColumnSelectorFactories;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryQueryToolChest extends BaseAggregationQueryToolChest<GroupByQuery>
{
  private final QueryConfig config;

  private final StupidPool<ByteBuffer> bufferPool;
  private final GroupByQueryEngine engine; // For running the outer query around a subquery
  private final GroupByQueryMetricsFactory metricsFactory;

  @VisibleForTesting
  public GroupByQueryQueryToolChest(
      QueryConfig config,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool
  )
  {
    this(config, engine, bufferPool, DefaultGroupByQueryMetricsFactory.instance());
  }

  @Inject
  public GroupByQueryQueryToolChest(
      QueryConfig config,
      GroupByQueryEngine engine,
      @Global StupidPool<ByteBuffer> bufferPool,
      GroupByQueryMetricsFactory metricsFactory
  )
  {
    this.config = config;
    this.engine = engine;
    this.bufferPool = bufferPool;
    this.metricsFactory = metricsFactory;
  }

  @Override
  protected byte queryCode()
  {
    return GROUPBY_QUERY;
  }

  @Override
  protected Comparator<Row> getMergeOrdering(final GroupByQuery groupBy)
  {
    return groupBy.getCompactRowOrdering();
  }

  @Override
  public <I> QueryRunner<Row> handleSubQuery(QuerySegmentWalker segmentWalker)
  {
    return new SubQueryRunner<I>(segmentWalker)
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        final GroupByQuery groupBy = (GroupByQuery) query;
        final int maxPage = groupBy.getContextIntWithMax(
            Query.GBY_MAX_STREAM_SUBQUERY_PAGE,
            config.getGroupBy().getMaxStreamSubQueryPage()
        );
        final int[][] grouping = groupBy.getGroupings();
        final Query<I> innerQuery = ((QueryDataSource) groupBy.getDataSource()).getQuery();
        if (maxPage < 1 || (grouping != null && grouping.length > 0) || !QueryUtils.coveredBy(innerQuery, groupBy)) {
          return super.run(groupBy, responseContext);
        }
        if (groupBy.getDimensions().size() > 1 && TableFunctionSpec.from(groupBy) != null) {
          return super.run(groupBy, responseContext);
        }
        // this is about using less heap, not about performance
        return runStreaming(groupBy, responseContext);
      }

      @Override
      protected Function<Interval, Sequence<Row>> query(Query<Row> query, final Segment segment)
      {
        final GroupByQuery groupBy = ((GroupByQuery) query).withPostAggregatorSpecs(null);
        return new Function<Interval, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Interval interval)
          {
            QuerySegmentSpec segmentSpec = MultipleIntervalSegmentSpec.of(interval);
            return engine.process(groupBy.withQuerySegmentSpec(segmentSpec), config, segment, true);
          }
        };
      }

      @Override
      @SuppressWarnings("unchecked")
      protected Sequence<Row> mergeQuery(Query<Row> query, Sequence<Sequence<Row>> sequences, Segment segment)
      {
        GroupByQuery groupBy = (GroupByQuery) query;
        Sequence<Row> sequence = super.mergeQuery(query, sequences, segment);
        MergeIndex mergeIndex = sequence.accumulate(
            GroupByQueryHelper.createMergeIndex(groupBy, config, 1),
            GroupByQueryHelper.<Row>newMergeAccumulator(new Execs.Semaphore(1))
        );
        boolean parallel = config.useParallelSort(query);
        sequence = Sequences.withBaggage(mergeIndex.toMergeStream(parallel, true), mergeIndex);
        return postAggregation(groupBy, Sequences.map(sequence, groupBy.compactToMap(sequence.columns())));
      }

      @Override
      public Sequence<Row> runStreaming(Query<Row> query, Map<String, Object> responseContext)
      {
        GroupByQuery groupBy = (GroupByQuery) query;
        QueryDataSource dataSource = (QueryDataSource) query.getDataSource();

        Query subQuery = dataSource.getQuery();
        RowSignature schema = dataSource.getSchema();
        if (subQuery instanceof Query.ArrayOutputSupport && groupBy.getVirtualColumns().isEmpty() &&
            Granularities.isAll(groupBy.getGranularity()) && DimensionSpecs.isAllDefault(groupBy.getDimensions())) {
          Query.ArrayOutputSupport array = (Query.ArrayOutputSupport) subQuery;
          Sequence<Object[]> sequence = QueryRunners.runArray(array, segmentWalker, responseContext);
          String timeColumn = Row.TIME_COLUMN_NAME;
          if (subQuery instanceof JoinQuery.JoinHolder) {
            timeColumn = ((JoinQuery.JoinHolder) subQuery).getTimeColumnName();
          }
          Sequence<Cursor> cursors = ColumnSelectorFactories.toArrayCursors(sequence, schema, timeColumn, query);
          int[] indices = GuavaUtils.indexOf(sequence.columns(), DimensionSpecs.toInputNames(groupBy.getDimensions()));
          int[] mvs = schema.indexOf(t -> t.isDimension() || t.isMultiValued());
          return postAggregation(groupBy, Sequences.map(
              AggregateIndex.of(indices, mvs).aggregate(groupBy, cursors),
              GroupByQueryEngine.arrayToRow(groupBy.withPostAggregatorSpecs(null), false)
          ));
        }
        return super.runStreaming(query, responseContext);
      }

      @Override
      protected Function<Cursor, Sequence<Row>> streamQuery(final Query<Row> query)
      {
        final GroupByQuery groupBy = (GroupByQuery) query;
        final int maxPages = groupBy.getContextIntWithMax(
            Query.GBY_MAX_STREAM_SUBQUERY_PAGE,
            config.getGroupBy().getMaxStreamSubQueryPage()
        );
        return new Function<Cursor, Sequence<Row>>()
        {
          @Override
          public Sequence<Row> apply(Cursor cursor)
          {
            Sequence<Object[]> iterator = new RowIterator(groupBy, config, cursor, bufferPool, maxPages)
            {
              @Override
              protected void nextIteration(long start, List<KeyValue> unprocessedKeys)
              {
                if (unprocessedKeys != null) {
                  // todo: fall back to incremanl index?
                  throw new ISE("Result of subquery is exceeding max pages [%d]", maxPages);
                }
              }
            }.asArray();

            LOG.debug("Running streaming subquery with max pages [%d]", maxPages);
            Sequence<Row> sequence = Sequences.map(
                iterator, GroupByQueryEngine.arrayToRow(groupBy.withPostAggregatorSpecs(null), false)
            );
            return postAggregation(groupBy, sequence);
          }
        };
      }
    };
  }

  @Override
  public GroupByQueryMetrics makeMetrics(Query<Row> query)
  {
    GroupByQueryMetrics queryMetrics = metricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  protected boolean isSortOnTime()
  {
    return config.getGroupBy().isSortOnTime();
  }
}
