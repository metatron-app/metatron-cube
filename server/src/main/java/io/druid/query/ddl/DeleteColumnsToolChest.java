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

package io.druid.query.ddl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.query.BaseQuery;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryException;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class DeleteColumnsToolChest extends QueryToolChest.MetricSupport<DDLResult>
{
  private static final TypeReference<DDLResult> DDL_TYPE_REFERENCE = new TypeReference<DDLResult>()
  {
  };

  private final IndexerMetadataStorageCoordinator metadata;
  private final CoordinatorClient coordinator;
  private final DataSegmentKiller killer;

  @Inject
  public DeleteColumnsToolChest(
      GenericQueryMetricsFactory metricsFactory,
      IndexerMetadataStorageCoordinator metadata,
      CoordinatorClient coordinator,
      DataSegmentKiller killer
  )
  {
    super(metricsFactory);
    this.metadata = Preconditions.checkNotNull(metadata, "metadata");
    this.coordinator = Preconditions.checkNotNull(coordinator, "coordinator");
    this.killer = Preconditions.checkNotNull(killer, "killer");
  }

  @Override
  public QueryRunner<DDLResult> mergeResults(final QueryRunner<DDLResult> queryRunner)
  {
    return new QueryRunner<DDLResult>()
    {
      @Override
      public Sequence<DDLResult> run(Query<DDLResult> query, Map<String, Object> responseContext)
      {
        DeleteColumnsQuery delete = (DeleteColumnsQuery) query;
        List<DDLResult> results = Sequences.toList(queryRunner.run(query, responseContext));
        List<DataSegment> segments = ImmutableList.copyOf(GuavaUtils.explode(results, DDLResult::getSegments));
        if (segments.isEmpty()) {
          return Sequences.simple(DDLResult.success());
        }
        if (Iterables.any(results, d -> !d.isSuccess())) {
          killer.killQuietly(segments, LOG);
          return Sequences.simple(DDLResult.fail());
        }
        if (BaseQuery.isBrokerSide(query)) {
          LOG.info("Announcing [%d] segments to metadata storage", segments.size());
          try {
            metadata.announceHistoricalSegmentsWithCheck(segments);
          }
          catch (Exception e) {
            LOG.warn(e, "Failed to announce segments.. try deleting pushed segments");
            killer.killQuietly(segments, LOG);
            throw new QueryException(e);
          }
          coordinator.scheduleNow(Sets.newHashSet(segments), delete.getAssertTimeout(), delete.isAssertLoaded());
        }
        return Sequences.simple(DDLResult.success(segments));
      }
    };
  }

  @Override
  public TypeReference<DDLResult> getResultTypeReference(Query<DDLResult> query)
  {
    return DDL_TYPE_REFERENCE;
  }
}
