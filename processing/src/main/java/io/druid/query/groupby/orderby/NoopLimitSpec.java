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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.metamx.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.query.QueryCacheHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 */
public class NoopLimitSpec implements LimitSpec
{
  private static final byte CACHE_KEY = 0x0;

  private final List<WindowingSpec> windowingSpecs;

  @JsonCreator
  public NoopLimitSpec(
      @JsonProperty("windowingSpecs") List<WindowingSpec> windowingSpecs
  )
  {
    this.windowingSpecs = windowingSpecs;
  }

  public NoopLimitSpec()
  {
    this(null);
  }

  @Override
  @JsonProperty
  public List<WindowingSpec> getWindowingSpecs()
  {
    return windowingSpecs;
  }

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    if (windowingSpecs == null || windowingSpecs.isEmpty()) {
      return Functions.identity();
    }

    return Functions.compose(
        LIST_TO_SEQUENCE,
        Functions.compose(
            new WindowingProcessor(windowingSpecs, dimensions, aggs, postAggs),
            SEQUENCE_TO_LIST
        )
    );
  }

  @Override
  public LimitSpec merge(LimitSpec other)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return "NoopLimitSpec{windowingSpec=" + windowingSpecs + '}';
  }

  @Override
  public boolean equals(Object other)
  {
    return (other instanceof NoopLimitSpec) &&
           (Objects.equals(windowingSpecs, ((NoopLimitSpec) other).windowingSpecs));
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(windowingSpecs);
  }

  @Override
  public byte[] getCacheKey()
  {
    if (windowingSpecs == null || windowingSpecs.isEmpty()) {
      return new byte[]{CACHE_KEY};
    }
    byte[] windowingSpecBytes = QueryCacheHelper.computeAggregatorBytes(windowingSpecs);
    return ByteBuffer.allocate(1 + windowingSpecBytes.length)
                     .put(CACHE_KEY)
                     .put(windowingSpecBytes)
                     .array();
  }
}
