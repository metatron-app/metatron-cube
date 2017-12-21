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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.query.DataSource;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryHelper;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final GroupByQueryConfig groupByConfig;
  private final Map<String, VersionedIntervalTimeline<String, Segment>> timelines = Maps.newHashMap();
  private final List<Closeable> closeables = Lists.newArrayList();
  private final List<DataSegment> segments = Lists.newArrayList();

  public SpecificSegmentsQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
    this.groupByConfig = new GroupByQueryConfig();
  }

  public SpecificSegmentsQuerySegmentWalker add(
      final DataSegment descriptor,
      final QueryableIndex index
  )
  {
    final Segment segment = new QueryableIndexSegment(descriptor.getIdentifier(), index);
    if (!timelines.containsKey(descriptor.getDataSource())) {
      timelines.put(descriptor.getDataSource(), new VersionedIntervalTimeline<String, Segment>(Ordering.natural()));
    }

    final VersionedIntervalTimeline<String, Segment> timeline = timelines.get(descriptor.getDataSource());
    timeline.add(descriptor.getInterval(), descriptor.getVersion(), descriptor.getShardSpec().createChunk(segment));
    segments.add(descriptor);
    closeables.add(index);
    return this;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    DataSource dataSource = query.getDataSource();
    String dataSourceName = getDataSourceName(dataSource);

    final VersionedIntervalTimeline<String, Segment> timeline = timelines.get(dataSourceName);

    if (timeline == null) {
      return new NoopQueryRunner<T>();
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

    return toQueryRunner(query, segments);
  }

  private String getDataSourceName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    String dataSourceName = getDataSourceName(query.getDataSource());

    final VersionedIntervalTimeline<String, Segment> timeline = timelines.get(dataSourceName);
    if (timeline == null) {
      return new NoopQueryRunner<T>();
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
    return toQueryRunner(query, segments);
  }

  private <T> QueryRunner<T> toQueryRunner(Query<T> query, Iterable<Pair<SegmentDescriptor, Segment>> segments)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      return new NoopQueryRunner<T>();
    }

    final Future<Object> optimizer = factory.preFactoring(
        query,
        Lists.newArrayList(
            Iterables.filter(
                Iterables.transform(
                    segments,
                    Pair.<SegmentDescriptor, Segment>rhsFn()
                ), Predicates.notNull()
            )
        ),
        MoreExecutors.sameThreadExecutor()
    );

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(segments)
        .transformCat(
            new Function<Pair<SegmentDescriptor, Segment>, Iterable<QueryRunner<T>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Iterable<QueryRunner<T>> apply(Pair<SegmentDescriptor, Segment> input)
              {
                if (input.rhs == null) {
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input.lhs));
                }
                return Arrays.asList(
                    new SpecificSegmentQueryRunner<T>(
                        factory.createRunner(input.rhs, optimizer),
                        new SpecificSegmentSpec(input.lhs)
                    )
                );
              }
            }
        );

    QueryRunner<T> runner = factory.mergeRunners(MoreExecutors.sameThreadExecutor(), queryRunners, optimizer);
    if (query.getDataSource() instanceof QueryDataSource) {
      Query innerQuery = ((QueryDataSource) query.getDataSource()).getQuery().withOverriddenContext(query.getContext());
      int maxResult = groupByConfig.getMaxResults();
      int maxRowCount = Math.min(
          query.getContextValue(GroupByQueryHelper.CTX_KEY_MAX_RESULTS, maxResult),
          maxResult
      );
      QueryRunner innerRunner = toQueryRunner(innerQuery, segments);
      runner = toolChest.handleSubQuery(innerRunner, this, MoreExecutors.sameThreadExecutor(), maxRowCount);
    }
    return FinalizeResultsQueryRunner.finalize(toolChest.mergeResults(runner), toolChest, null);
  }
}
