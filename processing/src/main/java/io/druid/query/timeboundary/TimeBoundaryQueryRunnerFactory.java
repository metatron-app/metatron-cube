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

package io.druid.query.timeboundary;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public class TimeBoundaryQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<Result<TimeBoundaryResultValue>>
{
  @Inject
  public TimeBoundaryQueryRunnerFactory(QueryWatcher queryWatcher)
  {
    super(new TimeBoundaryQueryQueryToolChest(), queryWatcher);
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> _createRunner(
      Segment segment, Supplier<Object> optimizer, SessionCache cache
  )
  {
    return new TimeBoundaryQueryRunner(segment);
  }

  private static class TimeBoundaryQueryRunner implements QueryRunner<Result<TimeBoundaryResultValue>>
  {
    private final StorageAdapter adapter;

    public TimeBoundaryQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter(true);
    }

    @Override
    public Sequence<Result<TimeBoundaryResultValue>> run(
        final Query<Result<TimeBoundaryResultValue>> input,
        final Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TimeBoundaryQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeBoundaryQuery.class);
      }

      final TimeBoundaryQuery legacyQuery = (TimeBoundaryQuery) input;

      return Sequences.simple(
          () -> {
            if (adapter == null) {
              throw new ISE(
                  "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
              );
            }

            final Interval timeMinMax = adapter.getTimeMinMax();
            final DateTime minTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MAX_TIME)
                                     ? null
                                     : timeMinMax.getStart();
            final DateTime maxTime = legacyQuery.getBound().equalsIgnoreCase(TimeBoundaryQuery.MIN_TIME)
                                     ? null
                                     : timeMinMax.getEnd();


            return legacyQuery.buildResult(
                adapter.getInterval().getStart(),
                minTime,
                maxTime
            ).iterator();
          }
      );
    }
  }
}
