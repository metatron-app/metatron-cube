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

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.ConveyQuery;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.ForwardingSegmentWalker;
import io.druid.query.LocalStorageHandler;
import io.druid.query.NoopQueryRunner;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunners;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.RowResolver;
import io.druid.query.SegmentDescriptor;
import io.druid.query.StorageHandler;
import io.druid.query.UnionAllQuery;
import io.druid.query.aggregation.MetricManipulatorFns;
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
import io.druid.server.DruidNode;
import io.druid.server.ForwardHandler;
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
public class TestQuerySegmentWalker implements ForwardingSegmentWalker, QueryToolChestWarehouse
{
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService executor;
  private final QueryConfig queryConfig;

  private final PopulatingMap timeLines;
  private final ForwardHandler handler;

  @Override
  public StorageHandler getHandler(String scheme)
  {
    return handler.getHandler(scheme);
  }

  @Override
  public <T> QueryRunner<T> handle(Query<T> query, QueryRunner<T> baseRunner)
  {
    return handler.wrapForward(query, baseRunner);
  }

  private static class PopulatingMap
  {
    private final List<DataSegment> segments = Lists.newArrayList();
    private final Map<String, VersionedIntervalTimeline<String, Segment>> node1 = Maps.newHashMap();
    private final Map<String, VersionedIntervalTimeline<String, Segment>> node2 = Maps.newHashMap();
    private final Map<String, Supplier<List<Pair<DataSegment, Segment>>>> populators = Maps.newHashMap();

    private void addSegment(DataSegment descriptor, Segment segment)
    {
      int node = segment.getInterval().hashCode() % 2;
      VersionedIntervalTimeline<String, Segment> timeline = get(descriptor.getDataSource(), node);
      timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpecWithDefault().createChunk(segment));
      segments.add(descriptor);
    }

    public VersionedIntervalTimeline<String, Segment> get(String key, int node)
    {
      Supplier<List<Pair<DataSegment, Segment>>> populator = populators.remove(key);
      if (populator == null) {
        return (node == 0 ? node1 : node2).computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      }
      populate(key, populator);
      return (node == 0 ? node1 : node2).get(key);
    }

    public boolean populate(String key)
    {
      Supplier<List<Pair<DataSegment, Segment>>> populator = populators.remove(key);
      if (populator != null) {
        populate(key, populator);
        return true;
      }
      return false;
    }

    private void populate(String key, Supplier<List<Pair<DataSegment, Segment>>> populator)
    {
      VersionedIntervalTimeline<String, Segment> timeline1 = node1.computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      VersionedIntervalTimeline<String, Segment> timeline2 = node2.computeIfAbsent(key, k -> new VersionedIntervalTimeline<>());
      for (Pair<DataSegment, Segment> pair : populator.get()) {
        DataSegment descriptor = pair.lhs;
        if (descriptor.getInterval().hashCode() % 2 == 0) {
          timeline1.add(
              descriptor.getInterval(),
              descriptor.getVersion(),
              descriptor.getShardSpecWithDefault().createChunk(pair.rhs)
          );
        } else {
          timeline2.add(
              descriptor.getInterval(),
              descriptor.getVersion(),
              descriptor.getShardSpecWithDefault().createChunk(pair.rhs)
          );
        }
        segments.add(descriptor);
      }
    }

    public void addPopulator(String key, Supplier<List<Pair<DataSegment, Segment>>> populator)
    {
      Preconditions.checkArgument(!populators.containsKey(key));
      populators.put(key, Suppliers.memoize(populator));
    }
  }

  public TestQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate, QueryConfig config)
  {
    this(TestHelper.JSON_MAPPER, conglomerate, Execs.newDirectExecutorService(), config, new PopulatingMap());
  }

  private TestQuerySegmentWalker(
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
    this.handler = new ForwardHandler(
        new DruidNode("test", "test", 0),
        objectMapper,
        asWarehouse(queryConfig, conglomerate),
        ImmutableMap.<String, StorageHandler>of("file", new LocalStorageHandler(objectMapper)),
        this
    );
  }

  public TestQuerySegmentWalker withConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    return new TestQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
    );
  }

  public TestQuerySegmentWalker withObjectMapper(ObjectMapper objectMapper)
  {
    return new TestQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
    );
  }

  public TestQuerySegmentWalker withExecutor(ExecutorService executor)
  {
    return new TestQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        timeLines
    );
  }

  public TestQuerySegmentWalker duplicate()
  {
    PopulatingMap duplicate = new PopulatingMap();
    duplicate.segments.addAll(timeLines.segments);
    duplicate.node1.putAll(timeLines.node1);
    duplicate.node2.putAll(timeLines.node2);
    duplicate.populators.putAll(timeLines.populators);
    return new TestQuerySegmentWalker(
        objectMapper,
        conglomerate,
        executor,
        queryConfig,
        duplicate
    );
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return conglomerate;
  }

  @Override
  public ExecutorService getExecutor()
  {
    return executor;
  }

  public TestQuerySegmentWalker add(DataSegment descriptor, IncrementalIndex index)
  {
    timeLines.addSegment(descriptor, new IncrementalIndexSegment(index, descriptor.getIdentifier()));
    return this;
  }

  public TestQuerySegmentWalker add(DataSegment descriptor, QueryableIndex index)
  {
    timeLines.addSegment(descriptor, new QueryableIndexSegment(descriptor.getIdentifier(), index));
    return this;
  }

  public boolean populate(String dataSource)
  {
    return timeLines.populate(dataSource);
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
    query = QueryUtils.setQueryId(query, queryId);
    query = QueryUtils.rewriteRecursively(query, this, queryConfig);
    return QueryUtils.resolveRecursively(query, this);
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
    final QueryRunner<T> runner = toQueryRunner(prepared);
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
              return Pair.of(input, null);
            }
          }
      );
    }
    return Iterables.concat(
        Iterables.transform(
            Iterables.concat(
                Iterables.transform(
                    input.getIntervals(),
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
                                dataSourceName,
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
  private <T> QueryRunner<T> toQueryRunner(Query<T> query)
  {
    if (query instanceof ConveyQuery) {
      return QueryRunners.wrap(((ConveyQuery<T>) query).getValues());
    }
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (query.getDataSource() instanceof QueryDataSource) {
      Preconditions.checkNotNull(factory, "%s does not supports nested query", query);
      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
      QueryRunner<T> runner = toolChest.handleSubQuery(this, queryConfig);
      return FluentQueryRunnerBuilder.create(toolChest, runner)
                                     .applyFinalizeResults()
                                     .applyFinalQueryDecoration()
                                     .applyPostProcessingOperator(objectMapper)
                                     .applySubQueryResolver(this, queryConfig)
                                     .build();
    }
    if (query instanceof UnionAllQuery) {
      return ((UnionAllQuery) query).getUnionQueryRunner(this, queryConfig);
    }
    if (query instanceof Query.IteratingQuery) {
      QueryRunner runner = QueryRunners.getIteratingRunner((Query.IteratingQuery) query, this);
      return FluentQueryRunnerBuilder.create(factory == null ? null : factory.getToolchest(), runner)
                                     .applyFinalizeResults()
                                     .applyFinalQueryDecoration()
                                     .applyPostProcessingOperator(objectMapper)
                                     .build();
    }
    if (factory == null) {
      return PostProcessingOperators.wrap(NoopQueryRunner.instance(), objectMapper);
    }

    // things done in CCC
    QueryRunner<T> runner = new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        QueryRunner<T> runner = new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
          {
            final Query local = query.toLocalQuery();
            return QueryUtils.mergeSort(query, Arrays.asList(
                toLocalQueryRunner(local, getSegment(query, 0)).run(local, responseContext),
                toLocalQueryRunner(local, getSegment(query, 1)).run(local, responseContext)
            ));
          }
        };
        QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
        Function manipulatorFn = toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing());
        if (BaseQuery.isBySegment(query)) {
          manipulatorFn = BySegmentResultValueClass.applyAll(manipulatorFn);
        }
        return Sequences.map(runner.run(query, responseContext), manipulatorFn);
      }
    };

    return FluentQueryRunnerBuilder.create(factory.getToolchest(), runner)
                                   .applyPreMergeDecoration()
                                   .applyMergeResults()
                                   .applyPostMergeDecoration()
                                   .applyFinalizeResults()
                                   .applyFinalQueryDecoration()
                                   .applyPostProcessingOperator(objectMapper)
                                   .build();
  }

  private <T> QueryRunner<T> toLocalQueryRunner(Query<T> query, Iterable<Pair<SegmentDescriptor, Segment>> segments)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    QueryRunnerFactory.Splitable<T, Query<T>> splitable = null;
    if (factory instanceof QueryRunnerFactory.Splitable) {
      splitable = (QueryRunnerFactory.Splitable<T, Query<T>>) factory;
    }

    List<Segment> targets = Lists.newArrayList();
    List<QueryRunner<T>> missingSegments = Lists.newArrayList();
    for (Pair<SegmentDescriptor, Segment> segment : segments) {
      if (segment.rhs != null) {
        targets.add(new Segment.WithDescriptor(segment.rhs, segment.lhs));
      } else {
        missingSegments.add(new ReportTimelineMissingSegmentQueryRunner<T>(segment.lhs));
      }
    }
    if (query.isDescending()) {
      targets = Lists.reverse(targets);
    }

    if (targets.isEmpty()) {
      return PostProcessingOperators.wrap(QueryRunners.<T>empty(), objectMapper);
    }
    final Supplier<RowResolver> resolver = RowResolver.supplier(targets, query);
    final Query<T> resolved = query.resolveQuery(resolver, true);
    final Future<Object> optimizer = factory.preFactoring(resolved, targets, resolver, executor);

    final Function<Iterable<Segment>, QueryRunner<T>> function = new Function<Iterable<Segment>, QueryRunner<T>>()
    {
      @Override
      public QueryRunner<T> apply(Iterable<Segment> segments)
      {
        Iterable<QueryRunner<T>> runners = Iterables.transform(segments, new Function<Segment, QueryRunner<T>>()
        {
          @Override
          public QueryRunner<T> apply(final Segment segment)
          {
            return new SpecificSegmentQueryRunner<T>(
                new BySegmentQueryRunner<T>(
                    segment.getIdentifier(),
                    segment.getInterval().getStart(),
                    factory.createRunner(segment, optimizer)
                ),
                new SpecificSegmentSpec(((Segment.WithDescriptor) segment).getDescriptor())
            );
          }
        });
        return FinalizeResultsQueryRunner.finalize(
            toolChest.mergeResults(
                factory.mergeRunners(resolved, executor, runners, optimizer)
            ),
            toolChest,
            objectMapper
        );
      }
    };

    if (splitable != null) {
      List<List<Segment>> splits = splitable.splitSegments(resolved, targets, optimizer, resolver, this);
      if (!GuavaUtils.isNullOrEmpty(splits)) {
        return QueryRunners.runWith(
            resolved, QueryRunners.concat(Iterables.concat(missingSegments, Iterables.transform(splits, function)))
        );
      }
    }

    QueryRunner<T> runner = QueryRunners.concat(GuavaUtils.concat(missingSegments, function.apply(targets)));
    if (splitable != null) {
      List<Query<T>> splits = splitable.splitQuery(resolved, targets, optimizer, resolver, this);
      if (splits != null) {
        return QueryRunners.concat(runner, splits);
      }
    }
    return QueryRunners.runWith(resolved, runner);
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

  private static QueryToolChestWarehouse asWarehouse(
      final QueryConfig queryConfig,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return new QueryToolChestWarehouse()
    {
      @Override
      public QueryConfig getQueryConfig()
      {
        return queryConfig;
      }

      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
      {
        final QueryRunnerFactory<T, QueryType> factory = conglomerate.findFactory(query);
        return factory == null ? null : factory.getToolchest();
      }
    };
  }
}
