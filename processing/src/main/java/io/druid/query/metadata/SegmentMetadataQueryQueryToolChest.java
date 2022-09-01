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

package io.druid.query.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SegmentMetadataQueryQueryToolChest
    extends QueryToolChest.CacheSupport<SegmentAnalysis, SegmentAnalysis, SegmentMetadataQuery>
{
  private static final TypeReference<SegmentAnalysis> TYPE_REFERENCE = new TypeReference<SegmentAnalysis>()
  {
  };
  private static final Function<SegmentAnalysis, SegmentAnalysis> MERGE_TRANSFORM_FN = new Function<SegmentAnalysis, SegmentAnalysis>()
  {
    @Override
    public SegmentAnalysis apply(SegmentAnalysis analysis)
    {
      return finalizeAnalysis(analysis);
    }
  };

  private final SegmentMetadataQueryConfig config;
  private final GenericQueryMetricsFactory metricsFactory;

  @VisibleForTesting
  public SegmentMetadataQueryQueryToolChest(SegmentMetadataQueryConfig config)
  {
    this(config, DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public SegmentMetadataQueryQueryToolChest(SegmentMetadataQueryConfig config, GenericQueryMetricsFactory metricsFactory)
  {
    this.config = config;
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<SegmentAnalysis> mergeResults(final QueryRunner<SegmentAnalysis> runner)
  {
    return new ResultMergeQueryRunner<SegmentAnalysis>(runner)
    {
      @Override
      public Sequence<SegmentAnalysis> doRun(
          QueryRunner<SegmentAnalysis> baseRunner,
          Query<SegmentAnalysis> query,
          Map<String, Object> context
      )
      {
        return Sequences.map(
            CombiningSequence.create(
                baseRunner.run(query, context),
                makeOrdering(query),
                createMergeFn(query)
            ),
            MERGE_TRANSFORM_FN
        );
      }

      @Override
      protected Comparator<SegmentAnalysis> makeOrdering(Query<SegmentAnalysis> query)
      {
        return query.getMergeOrdering(null);
      }

      @Override
      protected BinaryFn.Identical<SegmentAnalysis> createMergeFn(final Query<SegmentAnalysis> inQ)
      {
        final boolean lenient = ((SegmentMetadataQuery) inQ).isLenientAggregatorMerge();
        return (arg1, arg2) -> mergeAnalyses(arg1, arg2, lenient);
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(SegmentMetadataQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  public TypeReference<SegmentAnalysis> getResultTypeReference(SegmentMetadataQuery query)
  {
    return TYPE_REFERENCE;
  }

  @Override
  public IdenticalCacheStrategy getCacheStrategy(SegmentMetadataQuery query)
  {
    return new IdenticalCacheStrategy()
    {
      @Override
      public byte[] computeCacheKey(SegmentMetadataQuery query, int limit)
      {
        return KeyBuilder.get(limit)
                         .append(SEGMENT_METADATA_QUERY)
                         .append(query.getToInclude())
                         .append(query.getColumns())
                         .append(query.getVirtualColumns())
                         .append(query.getAnalysisTypes())
                         .build();
      }
    };
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SegmentMetadataQuery query, List<T> segments)
  {
    if (!query.isUsingDefaultInterval()) {
      return segments;
    }

    if (segments.size() <= 1) {
      return segments;
    }

    final T max = segments.get(segments.size() - 1);

    DateTime targetEnd = max.getInterval().getEnd();
    final Interval targetInterval = new Interval(config.getDefaultHistory(), targetEnd);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return (input.getInterval().overlaps(targetInterval));
              }
            }
        )
    );
  }

  @VisibleForTesting
  public static SegmentAnalysis mergeAnalyses(
      final SegmentAnalysis arg1,
      final SegmentAnalysis arg2,
      final boolean lenientAggregatorMerge
  )
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    List<Interval> newIntervals = null;
    if (arg1.getIntervals() != null) {
      newIntervals = Lists.newArrayList();
      newIntervals.addAll(arg1.getIntervals());
    }
    if (arg2.getIntervals() != null) {
      if (newIntervals == null) {
        newIntervals = Lists.newArrayList();
      }
      newIntervals.addAll(arg2.getIntervals());
    }

    final Map<String, ColumnAnalysis> leftColumns = arg1.getColumns();
    final Map<String, ColumnAnalysis> rightColumns = arg2.getColumns();
    Map<String, ColumnAnalysis> columns = Maps.newTreeMap();

    Set<String> rightColumnNames = Sets.newHashSet(rightColumns.keySet());
    for (Map.Entry<String, ColumnAnalysis> entry : leftColumns.entrySet()) {
      final String columnName = entry.getKey();
      columns.put(columnName, entry.getValue().fold(rightColumns.get(columnName)));
      rightColumnNames.remove(columnName);
    }

    for (String columnName : rightColumnNames) {
      columns.put(columnName, rightColumns.get(columnName));
    }

    final Map<String, AggregatorFactory> aggregators = Maps.newHashMap();

    if (lenientAggregatorMerge) {
      // Merge each aggregator individually, ignoring nulls
      for (SegmentAnalysis analysis : ImmutableList.of(arg1, arg2)) {
        if (analysis.getAggregators() != null) {
          for (Map.Entry<String, AggregatorFactory> entry : analysis.getAggregators().entrySet()) {
            final String aggregatorName = entry.getKey();
            final AggregatorFactory aggregator = entry.getValue();
            AggregatorFactory merged = aggregators.get(aggregatorName);
            if (merged != null) {
              try {
                merged = merged.getMergingFactory(aggregator);
              }
              catch (AggregatorFactoryNotMergeableException e) {
                merged = null;
              }
            } else {
              merged = aggregator;
            }
            aggregators.put(aggregatorName, merged);
          }
        }
      }
    } else {
      final AggregatorFactory[] aggs1 = arg1.getAggregators() != null
                                        ? arg1.getAggregators()
                                              .values()
                                              .toArray(new AggregatorFactory[arg1.getAggregators().size()])
                                        : null;
      final AggregatorFactory[] aggs2 = arg2.getAggregators() != null
                                        ? arg2.getAggregators()
                                              .values()
                                              .toArray(new AggregatorFactory[arg2.getAggregators().size()])
                                        : null;
      final AggregatorFactory[] merged = AggregatorFactory.mergeAggregators(Arrays.asList(aggs1, aggs2));
      if (merged != null) {
        for (AggregatorFactory aggregator : merged) {
          aggregators.put(aggregator.getName(), aggregator);
        }
      }
    }

    final Granularity queryGranularity = Granularity.mergeGranularities(
        Lists.newArrayList(
            arg1.getQueryGranularity(),
            arg2.getQueryGranularity()
        )
    );
    final Granularity segmentGranularity = Granularity.mergeGranularities(
        Lists.newArrayList(
            arg1.getSegmentGranularity(),
            arg2.getSegmentGranularity()
        )
    );

    final String mergedId;

    if (arg1.getId() != null && arg2.getId() != null && arg1.getId().equals(arg2.getId())) {
      mergedId = arg1.getId();
    } else {
      mergedId = "merged";
    }

    long lastAccessTime = Math.max(arg1.getLastAccessTime(), arg2.getLastAccessTime());

    final Boolean rollup;

    if (arg1.isRollup() != null && arg2.isRollup() != null && arg1.isRollup().equals(arg2.isRollup())) {
      rollup = arg1.isRollup();
    } else {
      rollup = null;
    }

    return new SegmentAnalysis(
        mergedId,
        newIntervals,
        columns,
        sumIfPositives(arg1.getSerializedSize(), arg2.getSerializedSize()),
        sumIfPositives(arg1.getNumRows(), arg2.getNumRows()),
        sumIfPositives(arg1.getIngestedNumRows(), arg2.getIngestedNumRows()),
        lastAccessTime,
        aggregators.isEmpty() ? null : aggregators,
        queryGranularity,
        segmentGranularity,
        rollup
    );
  }

  @VisibleForTesting
  public static SegmentAnalysis finalizeAnalysis(SegmentAnalysis analysis)
  {
    List<Interval> intervals = analysis.getIntervals();
    if (intervals == null || intervals.size() == 1) {
      return analysis;
    }
    return new SegmentAnalysis(
        analysis.getId(),
        JodaUtils.condenseIntervals(intervals),
        analysis.getColumns(),
        analysis.getSerializedSize(),
        analysis.getNumRows(),
        analysis.getIngestedNumRows(),
        analysis.getLastAccessTime(),
        analysis.getAggregators(),
        analysis.getQueryGranularity(),
        analysis.getSegmentGranularity(),
        analysis.isRollup()
    );
  }

  private static long sumIfPositives(long x, long y)
  {
    return x < 0 || y < 0 ? -1 : x + y;
  }
}
