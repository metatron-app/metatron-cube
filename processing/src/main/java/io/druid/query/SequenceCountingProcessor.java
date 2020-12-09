/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;

import java.util.Arrays;
import java.util.Map;

public class SequenceCountingProcessor extends PostProcessingOperator.ReturnsRow implements PostProcessingOperator.Local
{
  public static SequenceCountingProcessor INSTANCE = new SequenceCountingProcessor();

  @Override
  public QueryRunner<Row> postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query query, Map responseContext)
      {
        final Object accumulated = baseRunner.run(query, responseContext)
                                             .accumulate(0, new Accumulator<Integer, Object>()
                                             {
                                               @Override
                                               public Integer accumulate(Integer accumulated, Object in)
                                               {
                                                 return accumulated + 1;
                                               }
                                             });
        return Sequences.simple(
            Arrays.<Row>asList(new MapBasedRow(0, ImmutableMap.of("cardinality", accumulated)))
        );
      }
    };
  }
}
