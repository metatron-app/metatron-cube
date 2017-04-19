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

package io.druid.query;

import com.google.common.base.Throwables;
import org.joda.time.Interval;

/**
 */
public interface QuerySegmentWalker
{
  /**
   * Gets the Queryable for a given interval, the Queryable returned can be any version(s) or partitionNumber(s)
   * such that it represents the interval.
   *
   * @param <T> query result type
   * @param query the query to find a Queryable for
   * @param intervals the intervals to find a Queryable for
   * @return a Queryable object that represents the interval
   */
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals);

  /**
   * Gets the Queryable for a given list of SegmentSpecs.
   *
   * @param <T> the query result type
   * @param query the query to return a Queryable for
   * @param specs the list of SegmentSpecs to find a Queryable for
   * @return the Queryable object with the given SegmentSpecs
   */
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs);

  public static abstract class Wrapper implements QuerySegmentWalker
  {
    private final QuerySegmentWalker delegate;

    public Wrapper(QuerySegmentWalker delegate) {this.delegate = delegate;}

    public QuerySegmentWalker getDelegate()
    {
      return delegate;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      try {
        return wrap(query, delegate.getQueryRunnerForIntervals(query, intervals));
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      try {
        return wrap(query, delegate.getQueryRunnerForSegments(query, specs));
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    protected abstract <T> QueryRunner<T> wrap(Query<T> query, QueryRunner<T> runner) throws Exception;
  }
}
