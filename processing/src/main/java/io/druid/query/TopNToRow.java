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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.topn.TopNResultValue;

import java.util.Map;

/**
 */
public class TopNToRow extends PostProcessingOperator.Abstract<Result<TopNResultValue>>
{
  @JsonCreator
  public TopNToRow() { }

  @Override
  public QueryRunner postProcess(final QueryRunner<Result<TopNResultValue>> baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        return Sequences.explode(baseRunner.run(query, responseContext), Queries.TOP_N_TO_ROWS);
      }
    };
  }
}
