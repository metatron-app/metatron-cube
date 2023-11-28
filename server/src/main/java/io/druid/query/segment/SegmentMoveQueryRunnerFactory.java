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

package io.druid.query.segment;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;
import io.druid.segment.loading.SegmentLoader;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Map;

/**
 *
 */
public class SegmentMoveQueryRunnerFactory extends QueryRunnerFactory.Abstract<SegmentLocation>
{
  private final SegmentLoader segmentLoader;

  @Inject
  public SegmentMoveQueryRunnerFactory(
      SegmentLoader segmentLoader,
      SegmentMoveQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.segmentLoader = segmentLoader;
  }

  @Override
  public QueryRunner<SegmentLocation> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<SegmentLocation>()
    {
      @Override
      public Sequence<SegmentLocation> run(Query<SegmentLocation> query, Map<String, Object> responseContext)
      {
        final DataSegment segment = ((SegmentMoveQuery) query).getSegment();
        try {
          File location = segmentLoader.getLocation(segment);
          return Sequences.simple(new SegmentLocation(segment, location));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
