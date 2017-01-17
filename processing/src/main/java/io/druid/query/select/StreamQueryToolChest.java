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
import io.druid.query.DruidMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.MetricManipulationFn;

import java.util.Map;

/**
 */
public class StreamQueryToolChest extends QueryToolChest<StreamQueryRow, StreamQuery>
{
  private static final TypeReference<StreamQueryRow> TYPE_REFERENCE =
      new TypeReference<StreamQueryRow>()
      {
      };

  @Override
  public QueryRunner<StreamQueryRow> mergeResults(QueryRunner<StreamQueryRow> queryRunner)
  {
    return queryRunner; // don't care ordering.. just concat
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
        return Sequences.map(
            sequence, new Function<StreamQueryRow, Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(StreamQueryRow input)
              {
                return input;
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
}
