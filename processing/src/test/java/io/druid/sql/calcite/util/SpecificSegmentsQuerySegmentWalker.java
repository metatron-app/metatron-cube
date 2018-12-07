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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.Pair;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.FluentQueryRunnerBuilder;
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
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
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

  private final PopulatingMap timeLines;

  private static class PopulatingMap
  {
    private final List<DataSegment> segments = Lists.newArrayList();
    private final Map<String, VersionedIntervalTimeline<String, Segment>> node1 = Maps.newHashMap();
    private final Map<String, VersionedIntervalTimeline<String, Segment>> node2 = Maps.newHashMap();
    private final Map<String, Supplier<List<Pair<DataSegment, Segment>>>> populators = Maps.newHashMap();

    private void addSegment(DataSegment descriptor, Segment segment)
    {
      int node = segment.getDataInterval().hashCode() % 2;
      VersionedIntervalTimeline<String, Segment> timeline = get(descriptor.getDataSource(), node);
      timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(segment));
      segments.add(descriptor);
    }

    public VersionedIntervalTimeline<String, Segment> get(String key, int node)
    {
      Supplier<List<Pair<DataSegment, Segment>>> populator = populators.remove(key);
      if (populator == null) {
        return (node == 0 ? node1 : node2).computeIfAbsent(
            key,
            new java.util.function.Function<String, VersionedIntervalTimeline<String, Segment>>()
            {
              @Override
              public VersionedIntervalTimeline<String, Segment> apply(String s)
              {
                return new VersionedIntervalTimeline<>(Ordering.<String>natural());
              }
            }
        );
      }
      VersionedIntervalTimeline<String, Segment> timeline1 = node1.get(key);
      if (timeline1 == null) {
        node1.put(key, timeline1 = new VersionedIntervalTimeline<>(Ordering.<String>natural()));
      }
      VersionedIntervalTimeline<String, Segment> timeline2 = node2.get(key);
      if (timeline2 == null) {
        node2.put(key, timeline2 = new VersionedIntervalTimeline<>(Ordering.<String>natural()));
      }
      for (Pair<DataSegment, Segment> pair : populator.get()) {
        DataSegment descriptor = pair.lhs;
        if (descriptor.getInterval().hashCode() % 2 == 0) {
          timeline1.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(pair.rhs));
        } else {
          timeline2.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(pair.rhs));
        }
        segments.add(descriptor);
      }
      return node == 0 ? timeline1 : timeline2;
    }

    public void addPopulator(String key, Supplier<List<Pair<DataSegment, Segment>>> populator)
    {
      Preconditions.checkArgument(!populators.containsKey(key));
      populators.put(key, Suppliers.memoize(populator));
    }
  }

  public SpecificSegmentsQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.objectMapper = TestHelper.JSON_MAPPER;
    this.conglomerate = conglomerate;
    this.executor = MoreExecutors.sameThreadExecutor();
    this.queryConfig = new QueryConfig();
    this.timeLines = new PopulatingMap();
  }

  private SpecificSegmentsQuerySegmentWalker(
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService executor,
      QueryConfig queryConfig,
      PopulatingMap timeLines
  )
  {
    this.objectMapper = objectMapper;
    this.conglomerate = conglomerate;
    this.executor = executor;
    this.queryConfig = queryConfig;
    this.timeLines = timeLines;
  }

  public SpecificSegmentsQuerySegmentWalker withConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
    );
  }

  public SpecificSegmentsQuerySegmentWalker withObjectMapper(ObjectMapper objectMapper)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
    );
  }

  public SpecificSegmentsQuerySegmentWalker withExecutor(ExecutorService executor)
  {
    return new SpecificSegmentsQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
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
    timeLines.addSegment(descriptor, new IncrementalIndexSegment(index, descriptor.getIdentifier()));
    return this;
  }

  public SpecificSegmentsQuerySegmentWalker add(DataSegment descriptor, QueryableIndex index)
  {
    timeLines.addSegment(descriptor, new QueryableIndexSegment(descriptor.getIdentifier(), index));
    return this;
  }

  public void addPopulator(String dataSource, Supplier<List<Pair<DataSegment, Segment>>> populator)
  {
    timeLines.addPopulator(dataSource, populator);
  }

  public List<DataSegment> getSegments()
  {
    return timeLines.segments;
  }

  @SuppressWarnings("unchecked")
  private <T> Query<T> prepareQuery(Query<T> query)
  {
    String queryId = query.getId() == null ? UUID.randomUUID().toString() : query.getId();

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
    query = QueryUtils.resolveRecursively(query, this);
    return QueryUtils.setQueryId(query, queryId);
  }

  @Override
  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeQueryRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeQueryRunner(query);
  }

  private <T> QueryRunner<T> makeQueryRunner(Query<T> query)
  {
    final Query<T> prepared = prepareQuery(query);
    final QueryRunner<T> runner = toQueryRunner(prepared, false);
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return runner.run(prepared, responseContext);
      }
    };
  }

  private <T> Iterable<Pair<SegmentDescriptor, Segment>> getSegment(Query<T> input, int node)
  {
    final String dataSourceName = Iterables.getOnlyElement(input.getDataSource().getNames());
    final VersionedIntervalTimeline<String, Segment> timeline = timeLines.get(dataSourceName, node);
    if (timeline == null) {
      return ImmutableList.of();
    }
    QuerySegmentSpec segmentSpec = input.getQuerySegmentSpec();
    if (segmentSpec instanceof MultipleSpecificSegmentSpec) {
      List<SegmentDescriptor> segments = ((MultipleSpecificSegmentSpec) segmentSpec).getDescriptors();
      return Iterables.transform(
          segments, new Function<SegmentDescriptor, Pair<SegmentDescriptor, Segment>>()
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
              return Pair.of(input, (Segment) null);
            }
          }
      );
    }
    return Iterables.concat(
        Iterables.transform(
            Iterables.concat(
                Iterables.transform(
                    segmentSpec.getIntervals(),
                    new Function<Interval, Iterable<TimelineObjectHolder<String, Segment>>>()
                    {
                      @Override
                      public Iterable<TimelineObjectHolder<String, Segment>> apply(Interval input)
                      {
                        return timeline.lookup(input);
                      }
                    }
                )
            ),
            new Function<TimelineObjectHolder<String, Segment>, Iterable<Pair<SegmentDescriptor, Segment>>>()
            {
              @Override
              public Iterable<Pair<SegmentDescriptor, Segment>> apply(
                  @Nullable final TimelineObjectHolder<String, Segment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return Iterables.transform(
                    holder.getObject(),
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
        )
    );
  }

  @SuppressWarnings("unchecked")
  private <T> QueryRunner<T> toQueryRunner(Query<T> query, boolean subQuery)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (query.getDataSource() instanceof QueryDataSource) {
      Preconditions.checkNotNull(factory, query + " does not supports nested query");
      QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
      Query innerQuery = toolChest.prepareSubQuery(query, dataSource.getQuery());

      int maxResult = queryConfig.getMaxResults();
      int maxRowCount = Math.min(
          query.getContextValue(GroupByQueryHelper.CTX_KEY_MAX_RESULTS, maxResult),
          maxResult
      );
      QueryRunner runner = toQueryRunner(innerQuery, true);
      runner = toolChest.finalQueryDecoration(
          toolChest.finalizeResults(
              toolChest.handleSubQuery(runner, this, executor, maxRowCount)
          )
      );
      return PostProcessingOperators.wrap(runner, objectMapper);
    }
    if (query instanceof Query.IteratingQuery) {
      return Queries.makeIteratingQueryRunner((Query.IteratingQuery) query, this);
    }
    if (query instanceof UnionAllQuery) {
      return ((UnionAllQuery) query).getUnionQueryRunner(objectMapper, executor, this);
    }
    if (factory == null) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }

    FluentQueryRunnerBuilder<T> runner = FluentQueryRunnerBuilder.create(
        factory.getToolchest(), new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
          {
            return QueryUtils.mergeSort(query, Arrays.asList(
                toLocalQueryRunner(query, getSegment(query, 0)).run(query, responseContext),
                toLocalQueryRunner(query, getSegment(query, 1)).run(query, responseContext)
            ));
          }
        }
    );

    runner = runner.applyPreMergeDecoration()
                   .applyMergeResults()
                   .applyPostMergeDecoration();
    if (!subQuery) {
      runner = runner.applyFinalizeResults();
    }
    return runner.applyFinalQueryDecoration()
                 .applyPostProcessingOperator(objectMapper)
                 .build();
  }

  private <T> QueryRunner<T> toLocalQueryRunner(
      Query<T> query,
      Iterable<Pair<SegmentDescriptor, Segment>> segments
  )
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    List<Segment> targets = Lists.newArrayList(
        Iterables.filter(
            Iterables.transform(segments, Pair.<SegmentDescriptor, Segment>rhsFn()), Predicates.notNull()
        )
    );
    if (targets.isEmpty()) {
      return PostProcessingOperators.wrap(new NoopQueryRunner<T>(), objectMapper);
    }
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver, objectMapper);
    final Future<Object> optimizer = factory.preFactoring(resolved, targets, resolver, executor);

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

    QueryRunner<T> runner = toolChest.finalQueryDecoration(
        toolChest.finalizeResults(
            toolChest.postMergeQueryDecoration(
                toolChest.mergeResults(
                    factory.mergeRunners(executor, queryRunners, optimizer)
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

    Function manipulatorFn = toolChest.makePreComputeManipulatorFn(
        resolved, MetricManipulatorFns.deserializing()
    );
    if (BaseQuery.getContextBySegment(query)) {
      manipulatorFn = BySegmentResultValueClass.applyAll(manipulatorFn);
    }
    final QueryRunner<T> baseRunner = runner;
    final Function deserializer = manipulatorFn;
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return Sequences.map(baseRunner.run(resolved, responseContext), deserializer);
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
