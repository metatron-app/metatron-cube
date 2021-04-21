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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Objects;

/**
 */
public class Result<T> implements Comparable<Result<T>>
{
  public static <T> Function<Result<T>, T> unwrap()
  {
    return new Function<Result<T>, T>()
    {
      @Override
      public T apply(Result<T> input)
      {
        return input.getValue();
      }
    };
  }

  public static final Comparator<Result> COMPARATOR = new Comparator<Result>()
  {
    @Override
    public int compare(Result o1, Result o2)
    {
      return o1.timestamp.compareTo(o2.timestamp);
    }
  };

  public static String MISSING_SEGMENTS_KEY = "missingSegments";

  private final DateTime timestamp;
  private final T value;

  @JsonCreator
  public Result(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("result") T value
  )
  {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public int compareTo(Result<T> tResult)
  {
    return timestamp.compareTo(tResult.timestamp);
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty("result")
  public T getValue()
  {
    return value;
  }

  public Result<T> withValue(T value)
  {
    return new Result<>(timestamp, value);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Result result = (Result) o;

    if (timestamp != null ? !(timestamp.isEqual(result.timestamp) && timestamp.getZone().getOffset(timestamp) == result.timestamp.getZone().getOffset(result.timestamp)) : result.timestamp != null) {
      return false;
    }
    if (!Objects.equals(value, result.value)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestamp, value);
  }

  @Override
  public String toString()
  {
    return "Result{" +
           "timestamp=" + timestamp +
           ", value=" + value +
           '}';
  }
}
