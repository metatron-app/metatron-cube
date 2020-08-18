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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.inject.Inject;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.BulkRow;
import io.druid.data.input.BulkSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.segment.Cursor;
import org.apache.commons.lang.mutable.MutableInt;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

/**
 */
public class StreamQueryToolChest extends QueryToolChest<Object[], StreamQuery>
{
  private final GenericQueryMetricsFactory metricsFactory;
  private final QueryConfig config;

  @Inject
  public StreamQueryToolChest(
      GenericQueryMetricsFactory metricsFactory,
      QueryConfig config
  )
  {
    this.metricsFactory = metricsFactory;
    this.config = config;
  }

  @Override
  public QueryRunner<Object[]> mergeResults(final QueryRunner<Object[]> queryRunner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        StreamQuery stream = (StreamQuery) query;
        if (query.getContextBoolean(QueryContextKeys.FINAL_MERGE, true)) {
          return stream.applyLimit(queryRunner.run(query, responseContext));
        }
        Sequence<Object[]> sequence = queryRunner.run(query, responseContext);
        LimitSpec limitSpec = stream.getLimitSpec();
        if (limitSpec.hasLimit()) {
          sequence = Sequences.limit(sequence, limitSpec.getLimit());
        }
        return sequence;
      }
    };
  }

  @Override
  public QueryMetrics<? super StreamQuery> makeMetrics(StreamQuery query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeReference getResultTypeReference(@Nullable StreamQuery query)
  {
    if (query != null && query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return BulkRow.TYPE_REFERENCE;
    } else {
      return ARRAY_TYPE_REFERENCE;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<Object[]> deserializeSequence(StreamQuery query, Sequence sequence)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      sequence = Sequences.explode((Sequence<BulkRow>) sequence, bulk -> Sequences.once(bulk.decompose()));
    }
    return super.deserializeSequence(query, sequence);
  }

  @Override
  public Sequence serializeSequence(StreamQuery query, Sequence<Object[]> sequence, QuerySegmentWalker segmentWalker)
  {
    // see CCC.prepareQuery()
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return BulkSequence.fromArray(sequence, Queries.relaySchema(query, segmentWalker));
    }
    return super.serializeSequence(query, sequence, segmentWalker);
  }

  @Override
  public ToIntFunction numRows(StreamQuery query)
  {
    if (query.getContextBoolean(Query.USE_BULK_ROW, false)) {
      return new ToIntFunction()
      {
        @Override
        public int applyAsInt(Object value)
        {
          return ((BulkRow) value).count();
        }
      };
    }
    return super.numRows(query);
  }

  @Override
  public Function<Sequence<Object[]>, Sequence<Map<String, Object>>> asMap(
      final StreamQuery query,
      final String timestampColumn
  )
  {
    return new Function<Sequence<Object[]>, Sequence<Map<String, Object>>>()
    {
      @Override
      public Sequence<Map<String, Object>> apply(Sequence<Object[]> sequence)
      {
        return query.asMap(sequence);
      }
    };
  }

  @Override
  public <I> QueryRunner<Object[]> handleSubQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    return new StreamingSubQueryRunner<I>(segmentWalker, config)
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Object[]> runStreaming(Query<Object[]> query, Map<String, Object> responseContext)
      {
        StreamQuery streamQuery = (StreamQuery) query;
        QueryDataSource dataSource = (QueryDataSource) query.getDataSource();
        Query subQuery = dataSource.getQuery();
        if (subQuery instanceof Query.ArrayOutputSupport &&
            streamQuery.isSimpleProjection(subQuery.getQuerySegmentSpec())) {
          Query.ArrayOutputSupport<Object[]> arrayOutput = (Query.ArrayOutputSupport) subQuery;
          List<String> outputColumns = Queries.relayColumns(arrayOutput, segmentWalker.getObjectMapper());
          if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
            Sequence<Object[]> sequence = arrayOutput.array(arrayOutput.run(segmentWalker, responseContext));
            return streamQuery.applySimpleProjection(sequence, outputColumns);
          }
        }
        return super.runStreaming(query, responseContext);
      }

      @Override
      protected final Function<Cursor, Sequence<Object[]>> streamQuery(Query<Object[]> query)
      {
        return StreamQueryEngine.processor((StreamQuery) query, config, new MutableInt());
      }
    };
  }
}
