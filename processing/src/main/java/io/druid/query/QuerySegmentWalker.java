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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.concurrent.Execs;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.Interval;

import java.util.concurrent.ExecutorService;

/**
 */
public interface QuerySegmentWalker
{
  QuerySegmentWalker DUMMY = new QuerySegmentWalker()
  {
    @Override
    public ExecutorService getExecutor()
    {
      return Execs.newDirectExecutorService();
    }

    @Override
    public ObjectMapper getObjectMapper()
    {
      return new DefaultObjectMapper();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      throw new UnsupportedOperationException("getQueryRunnerForIntervals");
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      throw new UnsupportedOperationException("getQueryRunnerForSegments");
    }
  };

  ExecutorService getExecutor();

  ObjectMapper getObjectMapper();

  /**
   * Gets the Queryable for a given interval, the Queryable returned can be any version(s) or partitionNumber(s)
   * such that it represents the interval.
   *
   * @param <T> query result type
   * @param query the query to find a Queryable for
   * @param intervals the intervals to find a Queryable for
   * @return a Queryable object that represents the interval
   */
  <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals);

  /**
   * Gets the Queryable for a given list of SegmentSpecs.
   *
   * @param <T> the query result type
   * @param query the query to return a Queryable for
   * @param specs the list of SegmentSpecs to find a Queryable for
   * @return the Queryable object with the given SegmentSpecs
   */
  <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs);
}
