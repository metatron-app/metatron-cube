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

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.DataSource;
import io.druid.query.NoopQueryRunner;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.UnionAllQuery;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, QueryToolChestWarehouse
{
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService executor;
  private final QueryConfig queryConfig;
  private final Map<String, VersionedIntervalTimeline<String, Segment>> timeLines;
  private final List<DataSegment> segments;

  public SpecificSegmentsQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.objectMapper = TestHelper.JSON_MAPPER;
    this.conglomerate = conglomerate;
    this.executor = MoreExecutors.sameThreadExecutor();
    this.queryConfig = new QueryConfig();
    this.timeLines = Maps.newHashMap();
    this.segments = Lists.newArrayList();
  }

  private SpecificSegmentsQuerySegmentWalker(
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService executor,
      QueryConfig queryConfig,
      Map<String, VersionedIntervalTimeline<String, Segment>> timeLines,
      List<DataSegment> segments
  )
  {
    this.objectMapper = objectMapper;
    this.conglomerate = conglomerate;
    this.executor = executor;
    this.queryConfig = queryConfig;
    this.timeLines = timeLines;
    this.segments = segments;
  }

  public SpecificSegmentsQuerySegmentWalker withConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines,
        segments
    );
  }

  public SpecificSegmentsQuerySegmentWalker withObjectMapper(ObjectMapper objectMapper)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines,
        segments
    );
  }

  public SpecificSegmentsQuerySegmentWalker withExecutor(ExecutorService executor)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines,
        segments
    );
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return conglomerate;
  }

  public ExecutorService getExecutor()
  {
    return executor;
  }

  public SpecificSegmentsQuerySegmentWalker add(DataSegment descriptor, IncrementalIndex index)
  {
    return addSegment(descriptor, new IncrementalIndexSegment(index, descriptor.getIdentifier()));
  }

  public SpecificSegmentsQuerySegmentWalker add(DataSegment descriptor, QueryableIndex index)
  {
    return addSegment(descriptor, new QueryableIndexSegment(descriptor.getIdentifier(), index));
  }

  private SpecificSegmentsQuerySegmentWalker addSegment(DataSegment descriptor, Segment segment)
  {
    if (!timeLines.containsKey(descriptor.getDataSource())) {
      timeLines.put(descriptor.getDataSource(), new VersionedIntervalTimeline<String, Segment>(Ordering.natural()));
    }
    VersionedIntervalTimeline<String, Segment> timeline = timeLines.get(descriptor.getDataSource());
    timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(segment));
    segments.add(descriptor);
    return this;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  public boolean contains(String dataSource)
  {
    return timeLines.containsKey(dataSource);
  }

  @SuppressWarnings("unchecked")
  private <T> Query<T> prepareQuery(Query<T> query)
  {
    query = Queries.iterate(
        query, new Function<Query, Query>()
        {
          @Override
          public Query apply(Query input)
          {
            if (input instanceof Query.RewritingQuery) {
              input = ((Query.RewritingQuery) input).rewriteQuery(
                  SpecificSegmentsQuerySegmentWalker.this,
                  queryConfig,
                  objectMapper
              );
            }
            return input;
          }
        }
    );
    query = QueryUtils.resolveRecursively(query, SpecificSegmentsQuerySegmentWalker.this);

    final String queryId = query.getId() == null ? UUID.randomUUID().toString() : query.getId();
    return Queries.iterate(
        query, new Function<Query, Query>()
        {
          @Override
          public Query apply(Query input)
          {
            if (input.getId() == null) {
              input = input.withId(queryId);
            }
            return input;
          }
        }
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final Query<T> prepared = prepareQuery(query);
    final QueryRunner<T> runner = makeQueryRunnerForIntervals(prepared, intervals);
    if (query == prepared) {
      return runner;
    }
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return runner.run(prepared, responseContext);
      }
    };
  }

  private <T> QueryRunner<T> makeQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    QueryRunner<T> runner = toBrokerQueryRunner(query);
    if (runner != null) {
      return runner;
    }
    DataSource dataSource = query.getDataSource();
    String dataSourceName = getDataSourceName(dataSource);

    final VersionedIntervalTimeline<String, Segment> timeline = timeLines.get(dataSourceName);

    if (timeline == null) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }

    FunctionalIterable<Pair<SegmentDescriptor, Segment>> segments = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, Segment>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, Segment>> apply(Interval input)
              {
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, Segment>, Iterable<Pair<SegmentDescriptor, Segment>>>()
            {
              @Override
              public Iterable<Pair<SegmentDescriptor, Segment>> apply(
                  @Nullable
                  final TimelineObjectHolder<String, Segment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<Segment>, Pair<SegmentDescriptor, Segment>>()
                        {
                          @Override
                          public Pair<SegmentDescriptor, Segment> apply(PartitionChunk<Segment> chunk)
                          {
                            return Pair.of(
                                new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    chunk.getChunkNumber()
                                ), chunk.getObject()
                            );
                          }
                        }
                    );
              }
            }
        );

    return toLocalQueryRunner(query, segments);
  }

  private String getDataSourceName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final Query<T> prepared = prepareQuery(query);
    final QueryRunner<T> runner = makeQueryRunnerForSegments(prepared, specs);
    if (query == prepared) {
      return runner;
    }
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return runner.run(prepared, responseContext);
      }
    };
  }

  private <T> QueryRunner<T> makeQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    QueryRunner<T> runner = toBrokerQueryRunner(query);
    if (runner != null) {
      return runner;
    }
    String dataSourceName = getDataSourceName(query.getDataSource());

    final VersionedIntervalTimeline<String, Segment> timeline = timeLines.get(dataSourceName);
    if (timeline == null) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }

    List<Pair<SegmentDescriptor, Segment>> segments = Lists.newArrayList(
        Iterables.transform(
            specs, new Function<SegmentDescriptor, Pair<SegmentDescriptor, Segment>>()
            {
              @Override
              public Pair<SegmentDescriptor, Segment> apply(SegmentDescriptor input)
              {
                PartitionHolder<Segment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );
                if (entry != null) {
                  PartitionChunk<Segment> chunk = entry.getChunk(input.getPartitionNumber());
                  if (chunk != null) {
                    return Pair.of(input, chunk.getObject());
                  }
                }
                return Pair.of(input, null);
              }
            }
        )
    );
    return toLocalQueryRunner(query, segments);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> toBrokerQueryRunner(Query<T> query)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (query.getDataSource() instanceof QueryDataSource) {
      Preconditions.checkNotNull(factory, query + " does not supports nested query");

      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
      Query innerQuery = ((QueryDataSource) query.getDataSource()).getQuery()
                                                                  .withOverriddenContext(query.getContext())
                                                                  .withOverriddenContext(Query.FINALIZE, false);
      int maxResult = queryConfig.getMaxResults();
      int maxRowCount = Math.min(
          query.getContextValue(GroupByQueryHelper.CTX_KEY_MAX_RESULTS, maxResult),
          maxResult
      );
      QueryRunner runner = innerQuery.getQuerySegmentSpec().lookup(innerQuery, this);
      runner = toolChest.finalQueryDecoration(
          toolChest.finalizeMetrics(
              toolChest.handleSubQuery(runner, this, executor, maxRowCount)
          )
      );
      return PostProcessingOperators.wrap(runner, objectMapper);
    }
    if (query instanceof UnionAllQuery) {
      return ((UnionAllQuery) query).getUnionQueryRunner(objectMapper, executor, this);
    }
    if (factory == null) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }
    return null;
  }

  private <T> QueryRunner<T> toLocalQueryRunner(
      Query<T> query,
      Iterable<Pair<SegmentDescriptor, Segment>> segments
  )
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final List<Segment> targets = Lists
      .newArrayList(
          Iterables.filter(
              Iterables.transform(
                  segments,
                  Pair.<SegmentDescriptor, Segment>rhsFn()
              ), Predicates.notNull()
          )
      );
    if (targets.isEmpty()) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver);
    final Future<Object> optimizer = factory.preFactoring(query, targets, resolver, executor);

    Iterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(segments)
        .transformCat(
            new Function<Pair<SegmentDescriptor, Segment>, Iterable<QueryRunner<T>>>()
            {
              @Override
              public Iterable<QueryRunner<T>> apply(Pair<SegmentDescriptor, Segment> input)
              {
                if (input.rhs == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input.lhs));
                }
                return Arrays.<QueryRunner<T>>asList(
                    new SpecificSegmentQueryRunner<T>(
                        new BySegmentQueryRunner<T>(
                            input.rhs.getIdentifier(),
                            input.rhs.getDataInterval().getStart(),
                            factory.createRunner(input.rhs, optimizer)
                        ),
                        new SpecificSegmentSpec(input.lhs)
                    )
                );
              }
            }
        );

    QueryRunner<T> runner = factory.mergeRunners(executor, queryRunners, optimizer);
    runner = toolChest.finalQueryDecoration(
        toolChest.finalizeMetrics(
            toolChest.postMergeQueryDecoration(
                toolChest.mergeResults(
                    toolChest.preMergeQueryDecoration(runner)
                )
            )
        )
    );

    if (factory instanceof QueryRunnerFactory.Splitable) {
      QueryRunnerFactory.Splitable<T, Query<T>> splitable = (QueryRunnerFactory.Splitable<T, Query<T>>) factory;
      Iterable<Query<T>> queries = splitable.splitQuery(resolved, targets, optimizer, resolver, this, objectMapper);
      if (queries != null) {
        runner = toConcatRunner(queries, runner);
      }
    }

    final QueryRunner<T> baseRunner = PostProcessingOperators.wrap(runner, objectMapper);
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return baseRunner.run(resolved, responseContext);
      }
    };
  }

  private <T> QueryRunner<T> toConcatRunner(
      final Iterable<Query<T>> queries,
      final QueryRunner<T> runner
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> baseQuery, final Map<String, Object> responseContext)
      {
        return Sequences.concat(
            Iterables.transform(
                queries, new Function<Query<T>, Sequence<T>>()
                {
                  @Override
                  public Sequence<T> apply(final Query<T> splitQuery)
                  {
                    return runner.run(splitQuery, responseContext);
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public QueryConfig getQueryConfig()
  {
    return queryConfig;
  }

  @Override
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    QueryRunnerFactory<T, QueryType> factory = conglomerate.findFactory(query);
    return factory == null ? null : factory.getToolchest();
  }
}
