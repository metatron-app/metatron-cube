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

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.granularity.Granularity;
import io.druid.granularity.GranularityType;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SegmentMetadataQueryRunnerFactory extends QueryRunnerFactory.Abstract<SegmentAnalysis>
{
  private static final Logger log = new Logger(SegmentMetadataQueryRunnerFactory.class);

  @Inject
  public SegmentMetadataQueryRunnerFactory(
      SegmentMetadataQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<SegmentAnalysis> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<SegmentAnalysis>()
    {
      @Override
      public Sequence<SegmentAnalysis> run(Query<SegmentAnalysis> inQ, Map<String, Object> responseContext)
      {
        SegmentMetadataQuery query = (SegmentMetadataQuery) inQ;

        final List<Pair<String, ColumnAnalysis>> analyzedColumns = SegmentAnalyzer.analyze(segment, query);

        final StorageAdapter adapter = segment.asStorageAdapter(false);
        final long numRows = adapter.getNumRows();
        final long totalSerializedSize = adapter.getSerializedSize();

        List<Interval> retIntervals = query.analyzingInterval() ? Arrays.asList(segment.getInterval()) : null;

        Metadata metadata = adapter.getMetadata();

        Map<String, AggregatorFactory> aggregators = null;
        if (query.hasAggregators() && metadata != null && metadata.getAggregators() != null) {
          aggregators = AggregatorFactory.asMap(metadata.getAggregators());
        }

        Granularity queryGranularity = null;
        if (metadata != null && query.hasQueryGranularity()) {
          queryGranularity = metadata.getQueryGranularity();
        }
        Granularity segmentGranularity = null;
        if (metadata != null && query.hasQueryGranularity()) {
          segmentGranularity = metadata.getSegmentGranularity();
          if (segmentGranularity == null) {
            Interval interval = segment.getInterval();
            GranularityType granularityType = GranularityType.fromInterval(interval);
            if (granularityType != null) {
              segmentGranularity = granularityType.getDefaultGranularity();
            }
          }
        }
        long ingestedNumRows = -1;
        if (metadata != null && query.hasIngestedNumRows()) {
          ingestedNumRows = metadata.getIngestedNumRows();
        }

        long lastAccessTime = -1;
        if (query.hasLastAccessTime()) {
          lastAccessTime = segment.getLastAccessTime();
        }

        Boolean rollup = null;
        if (query.hasRollup()) {
          rollup = metadata != null ? metadata.isRollup() : null;
          if (rollup == null) {
            // in this case, this segment is built before no-rollup function is coded,
            // thus it is built with rollup
            rollup = Boolean.TRUE;
          }
        }

        List<String> columnNames = GuavaUtils.transform(analyzedColumns, p -> p.lhs);
        List<ColumnAnalysis> columnAnalyses = GuavaUtils.transform(analyzedColumns, p -> p.rhs);
        return Sequences.simple(
            Arrays.asList(
                new SegmentAnalysis(
                    segment.getIdentifier(),
                    retIntervals,
                    columnNames,
                    columnAnalyses,
                    totalSerializedSize,
                    numRows,
                    ingestedNumRows,
                    lastAccessTime,
                    aggregators,
                    queryGranularity,
                    segmentGranularity,
                    1,
                    rollup
                )
            )
        );
      }
    };
  }
}
