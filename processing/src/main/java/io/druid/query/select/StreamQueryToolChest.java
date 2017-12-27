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
import com.google.common.base.Functions;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class StreamQueryToolChest extends QueryToolChest<StreamQueryRow, StreamQuery>
{
  private static final TypeReference<StreamQueryRow> TYPE_REFERENCE =
      new TypeReference<StreamQueryRow>()
      {
      };

  @Override
  public QueryRunner<StreamQueryRow> mergeResults(final QueryRunner<StreamQueryRow> queryRunner)
  {
    return new QueryRunner<StreamQueryRow>()
    {
      @Override
      public Sequence<StreamQueryRow> run(
          Query<StreamQueryRow> query, Map<String, Object> responseContext
      )
      {
        Sequence<StreamQueryRow> sequence = queryRunner.run(query, responseContext);
        if (((StreamQuery)query).getLimit() > 0) {
          sequence = Sequences.limit(sequence, ((StreamQuery)query).getLimit());
        }
        return sequence;
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(StreamQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<StreamQueryRow, StreamQueryRow> makePreComputeManipulatorFn(
      final StreamQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<StreamQueryRow> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public TabularFormat toTabularFormat(final Sequence<StreamQueryRow> sequence, final String timestampColumn)
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.map(sequence, GuavaUtils.<StreamQueryRow, Map<String, Object>>caster());
      }

      @Override
      public Map<String, Object> getMetaData()
      {
        return null;
      }
    };
  }

  @Override
  public <I> QueryRunner<StreamQueryRow> handleSubQuery(
      final QueryRunner<I> subQueryRunner,
      final QuerySegmentWalker segmentWalker,
      final ExecutorService executor,
      final int maxRowCount
  )
  {
    return new SubQueryRunner<I>(subQueryRunner, segmentWalker, executor, maxRowCount)
    {
      @Override
      protected Function<Interval, Sequence<StreamQueryRow>> function(
          Query<StreamQueryRow> query, Map<String, Object> context,
          Segment segment
      )
      {
        final StreamQueryEngine engine = new StreamQueryEngine();
        final StreamQuery outerQuery = (StreamQuery) query;
        final StorageAdapter adapter = segment.asStorageAdapter(true);
        return new Function<Interval, Sequence<StreamQueryRow>>()
        {
          @Override
          public Sequence<StreamQueryRow> apply(Interval interval)
          {
            return engine.process(
                outerQuery.withQuerySegmentSpec(MultipleIntervalSegmentSpec.of(interval)),
                adapter,
                null,
                null
            );
          }
        };
      }
    };
  }
}
