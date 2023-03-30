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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;
import io.druid.data.input.BulkSequence;

import java.util.function.ToIntFunction;

/**
 *
 */
public class DimensionSamplingQueryToolChest extends QueryToolChest<Object[]>
{
  private final GenericQueryMetricsFactory metricsFactory;

  @Inject
  public DimensionSamplingQueryToolChest(GenericQueryMetricsFactory metricsFactory)
  {
    this.metricsFactory = metricsFactory;
  }

  @Override
  public QueryRunner<Object[]> mergeResults(QueryRunner<Object[]> runner)
  {
    return runner;  // nothing
  }

  @Override
  public QueryMetrics makeMetrics(Query<Object[]> query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeReference getResultTypeReference(Query<Object[]> query)
  {
    if (query != null && query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return BulkRow.TYPE_REFERENCE;
    } else {
      return ARRAY_TYPE_REFERENCE;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<Object[]> deserializeSequence(Query<Object[]> query, Sequence sequence)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      sequence = Sequences.explode((Sequence<BulkRow>) sequence, bulk -> Sequences.once(bulk.decompose()));
    }
    return super.deserializeSequence(query, sequence);
  }

  @Override
  public Sequence serializeSequence(Query<Object[]> query, Sequence<Object[]> sequence, QuerySegmentWalker segmentWalker)
  {
    // see CCC.prepareQuery()
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return BulkSequence.fromArray(sequence, Queries.relaySchema(query, segmentWalker));
    }
    return super.serializeSequence(query, sequence, segmentWalker);
  }

  @Override
  public ToIntFunction numRows(Query<Object[]> query)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return v -> ((BulkRow) v).count();
    }
    return super.numRows(query);
  }
}
