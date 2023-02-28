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

import com.google.common.base.Function;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public interface BySegmentResultValue<T>
{
  static <T> Function<Result<BySegmentResultValue<T>>, Result<BySegmentResultValue<T>>> applyAll(final Function<T, T> function)
  {
    return new IdentityFunction<Result<BySegmentResultValue<T>>>()
    {
      @Override
      public Result<BySegmentResultValue<T>> apply(Result<BySegmentResultValue<T>> input)
      {
        return input.withValue(input.getValue().withTransform(function));
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <T> List<T> unwrap(Object input)
  {
    return ((Result<BySegmentResultValue<T>>) input).getValue().getResults();
  }

  List<T> getResults();

  String getSegmentId();

  Interval getInterval();

  default BySegmentResultValue<T> withTransform(Function<T, T> function)
  {
    return withResult(GuavaUtils.transform(getResults(), function));
  }

  default BySegmentResultValue<T> withResult(List<T> result)
  {
    return new BySegmentResultValueClass<T>(result, getSegmentId(), getInterval());
  }
}
