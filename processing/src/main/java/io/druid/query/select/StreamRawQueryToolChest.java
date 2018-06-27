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
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class StreamRawQueryToolChest extends QueryToolChest<Object[], StreamRawQuery>
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
        final boolean finalWork = query.getContextBoolean(QueryContextKeys.FINAL_WORK, true);
        final StreamRawQuery stream = (StreamRawQuery) query.withOverriddenContext(QueryContextKeys.FINAL_WORK, false);

        Sequence<Object[]> sequence = queryRunner.run(stream, responseContext);
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
      final StreamRawQuery query,
      final Sequence<Object[]> sequence,
      final String timestampColumn
  )
  {
    return new TabularFormat()
    {
      private final List<String> columnNames = query.getColumns();

      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(
            sequence, new Function<Object[], Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(Object[] input)
              {
                final Map<String, Object> converted = Maps.newLinkedHashMap();
                for (int i = 0; i < columnNames.size(); i++) {
                  converted.put(columnNames.get(i), input[i]);
                }
                return converted;
              }
            }
        );
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<Object[]> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<Object[]>> function(
          final Query<Object[]> query, Map<String, Object> context,
          final Segment segment
      )
      {
        final StreamQueryEngine engine = new StreamQueryEngine();
        final StreamRawQuery outerQuery = (StreamRawQuery) query;
        return new Function<Interval, Sequence<Object[]>>()
        {
          @Override
          public Sequence<Object[]> apply(Interval interval)
          {
            return engine.process(
                outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                segment,
                null,
                null
            );
          }
        };
      }
    };
  }
}
