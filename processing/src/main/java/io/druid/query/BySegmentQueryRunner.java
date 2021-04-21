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

import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class BySegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryToolChest toolChest;
  private final String segmentIdentifier;
  private final DateTime timestamp;
  private final QueryRunner<T> runner;

  public BySegmentQueryRunner(
      QueryToolChest toolChest,
      String segmentIdentifier,
      DateTime timestamp,
      QueryRunner<T> runner
  )
  {
    this.toolChest = toolChest;
    this.segmentIdentifier = segmentIdentifier;
    this.timestamp = timestamp;
    this.runner = runner;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence run(final Query<T> query, Map<String, Object> responseContext)
  {
    final Sequence<T> sequence = runner.run(query, responseContext);
    if (BaseQuery.isBySegment(query)) {
      return Sequences.of(
          sequence.columns(),
          new Result<BySegmentResultValue<T>>(timestamp, toolChest.bySegment(query, sequence, segmentIdentifier))
      );
    }
    return sequence;
  }
}
