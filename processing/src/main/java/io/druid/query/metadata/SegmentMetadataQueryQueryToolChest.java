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
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.MappedSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.common.guava.CombiningSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.Granularity;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
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

import java.nio.ByteBuffer;
import java.util.Arrays;
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
  private final GenericQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public SegmentMetadataQueryQueryToolChest(SegmentMetadataQueryConfig config)
  {
    this(config, DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public SegmentMetadataQueryQueryToolChest(SegmentMetadataQueryConfig config, GenericQueryMetricsFactory queryMetricsFactory)
  {
    this.config = config;
    this.queryMetricsFactory = queryMetricsFactory;
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
        return new MappedSequence<>(
            CombiningSequence.create(
                baseRunner.run(query, context),
                makeOrdering(query),
                createMergeFn(query)
            ),
            MERGE_TRANSFORM_FN
        );
      }

      @Override
      protected Ordering<SegmentAnalysis> makeOrdering(Query<SegmentAnalysis> query)
      {
        if (((SegmentMetadataQuery) query).isMerge()) {
          // Merge everything always
          return GuavaUtils.<SegmentAnalysis>allEquals();
        }

        return query.getMergeOrdering(); // No two elements should be equal, so it should never merge
      }

      @Override
      protected BinaryFn<SegmentAnalysis, SegmentAnalysis, SegmentAnalysis> createMergeFn(final Query<SegmentAnalysis> inQ)
      {
        return new BinaryFn<SegmentAnalysis, SegmentAnalysis, SegmentAnalysis>()
        {
          private final SegmentMetadataQuery query = (SegmentMetadataQuery) inQ;

          @Override
          public SegmentAnalysis apply(SegmentAnalysis arg1, SegmentAnalysis arg2)
          {
            return mergeAnalyses(arg1, arg2, query.isLenientAggregatorMerge());
          }
        };
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(SegmentMetadataQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  public TypeReference<SegmentAnalysis> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public IdentityCacheStrategy getCacheStrategy(SegmentMetadataQuery query)
  {
    return new IdentityCacheStrategy()
    {
      @Override
      public byte[] computeCacheKey(SegmentMetadataQuery query)
      {
        byte[] includeBytes = QueryCacheHelper.computeCacheBytes(query.getToInclude());
        byte[] columnsBytes = QueryCacheHelper.computeCacheBytes(query.getColumns());
        byte[] vcBytes = QueryCacheHelper.computeCacheKeys(query.getVirtualColumns());
        byte[] analysisTypesBytes = query.getAnalysisTypesCacheKey();
        return ByteBuffer.allocate(1
                                   + includeBytes.length
                                   + columnsBytes.length
                                   + vcBytes.length
                                   + analysisTypesBytes.length)
                         .put(SEGMENT_METADATA_QUERY)
                         .put(includeBytes)
                         .put(columnsBytes)
                         .put(vcBytes)
                         .put(analysisTypesBytes)
                         .array();
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
      boolean lenientAggregatorMerge
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
    return new SegmentAnalysis(
        analysis.getId(),
        analysis.getIntervals() != null ? JodaUtils.condenseIntervals(analysis.getIntervals()) : null,
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
