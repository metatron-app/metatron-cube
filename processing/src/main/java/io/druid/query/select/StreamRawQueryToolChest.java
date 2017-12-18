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
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.TabularFormat;
import io.druid.query.aggregation.MetricManipulationFn;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class StreamRawQueryToolChest extends QueryToolChest<RawRows, StreamRawQuery>
{
  private static final TypeReference<RawRows> TYPE_REFERENCE =
      new TypeReference<RawRows>()
      {
      };

  @Override
  public QueryRunner<RawRows> mergeResults(final QueryRunner<RawRows> queryRunner)
  {
    return new QueryRunner<RawRows>()
    {
      @Override
      public Sequence<RawRows> run(
          Query<RawRows> query, Map<String, Object> responseContext
      )
      {
        Sequence<RawRows> sequence = queryRunner.run(query, responseContext);
        if (((StreamRawQuery)query).getLimit() > 0) {
          sequence = Sequences.limit(sequence, ((StreamRawQuery)query).getLimit());
        }
        return sequence;
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(StreamRawQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<RawRows, RawRows> makePreComputeManipulatorFn(
      final StreamRawQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<RawRows> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public TabularFormat toTabularFormat(final Sequence<RawRows> sequence, final String timestampColumn)
  {
    return new TabularFormat()
    {
      @Override
      public Sequence<Map<String, Object>> getSequence()
      {
        return Sequences.concat(
            Sequences.map(
                sequence, new Function<RawRows, Sequence<Map<String, Object>>>()
                {
                  @Override
                  public Sequence<Map<String, Object>> apply(RawRows input)
                  {
                    final String[] columnNames = input.getSchema().getColumnNames().toArray(new String[0]);
                    final List<Object[]> rows = input.getRows();
                    return Sequences.simple(
                        new Iterable<Map<String, Object>>()
                        {
                          @Override
                          public Iterator<Map<String, Object>> iterator()
                          {
                            return new Iterator<Map<String, Object>>()
                            {
                              private final Iterator<Object[]> rowStream = rows.iterator();
                              @Override
                              public boolean hasNext()
                              {
                                return rowStream.hasNext();
                              }

                              @Override
                              public Map<String, Object> next()
                              {
                                final Object[] row = rowStream.next();
                                final Map<String, Object> converted = Maps.newLinkedHashMap();
                                for (int i = 0; i < columnNames.length; i++) {
                                  converted.put(columnNames[i], row[i]);
                                }
                                return converted;
                              }
                            };
                          }
                        }
                    );
                  }
                }
            )
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
