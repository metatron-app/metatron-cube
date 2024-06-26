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

import com.google.common.collect.Lists;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;

import java.util.List;
import java.util.Map;

/**
 */
public class ReportTimelineMissingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final SegmentDescriptor descriptor;

  public ReportTimelineMissingSegmentQueryRunner(SegmentDescriptor descriptor)
  {
    this.descriptor = descriptor;
  }

  @Override
  public Sequence<T> run(
      Query<T> query, Map<String, Object> responseContext
  )
  {
    List<SegmentDescriptor> missingSegments = (List<SegmentDescriptor>) responseContext.get(Result.MISSING_SEGMENTS_KEY);
    if (missingSegments == null) {
      missingSegments = Lists.newArrayList();
      responseContext.put(Result.MISSING_SEGMENTS_KEY, missingSegments);
    }
    missingSegments.add(descriptor);
    return Sequences.empty();
  }
}
