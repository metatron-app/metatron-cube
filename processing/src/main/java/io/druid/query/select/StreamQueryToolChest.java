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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.segment.Cursor;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.List;
import java.util.Map;

/**
 */
public class StreamQueryToolChest extends QueryToolChest<Object[], StreamQuery>
{
  private static final TypeReference<Object[]> TYPE_REFERENCE =
      new TypeReference<Object[]>()
      {
      };

  @Override
  public QueryRunner<Object[]> mergeResults(final QueryRunner<Object[]> queryRunner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(
          Query<Object[]> query, Map<String, Object> responseContext
      )
      {
        boolean finalWork = query.getContextBoolean(QueryContextKeys.FINAL_MERGE, true);
        if (finalWork) {
          query = query.removePostActions();
        }
        StreamQuery stream = (StreamQuery) query;
        Sequence<Object[]> sequence = queryRunner.run(stream.removePostActions(), responseContext);
        if (stream.getLimit() > 0 && finalWork) {
          sequence = Sequences.limit(sequence, stream.getLimit());
        }
        return sequence;
      }
    };
  }

  @Override
  public TypeReference<Object[]> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public TabularFormat toTabularFormat(
      final StreamQuery query,
      final Sequence<Object[]> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return query.asMap(sequence);
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
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
        if (streamQuery.isSimpleProjection() && subQuery instanceof Query.ArrayOutputSupport) {
          Query.ArrayOutputSupport arrayOutput = (Query.ArrayOutputSupport) subQuery;
          List<String> outputColumns = arrayOutput.estimatedOutputColumns();
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
        return StreamQueryEngine.converter((StreamQuery) query, new MutableInt());
      }

      @Override
      protected Sequence<Object[]> streamMerge(Query<Object[]> query, Sequence<Sequence<Object[]>> sequences)
      {
        return ((StreamQuery) query).applySortLimit(Sequences.concat(sequences));
      }
    };
  }
}
