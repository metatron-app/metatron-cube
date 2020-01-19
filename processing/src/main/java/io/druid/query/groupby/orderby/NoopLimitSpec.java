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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.druid.common.KeyBuilder;
import io.druid.java.util.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.select.StreamQuery;

import java.util.List;

/**
 */
public class NoopLimitSpec extends LimitSpec
{
  public static final NoopLimitSpec INSTANCE = new NoopLimitSpec();

  private static final byte CACHE_KEY = 0x0;

  @JsonCreator
  public NoopLimitSpec() {super(null, -1); }

  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(Query.AggregationsSupport<?> query, boolean sortOnTimeForLimit)
  {
    return Functions.identity();
  }

  @Override
  public Function<Sequence<Object[]>, Sequence<Object[]>> build(StreamQuery query, boolean sortOnTimeForLimit)
  {
    // cause remmapping is handled for stream query, we still need to check input/output columns
    return super.build(query, sortOnTimeForLimit);
  }

  @Override
  @JsonIgnore
  public List<OrderByColumnSpec> getColumns()
  {
    return super.getColumns();
  }

  @Override
  @JsonIgnore
  public int getLimit()
  {
    return super.getLimit();
  }

  @Override
  @JsonIgnore
  public OrderedLimitSpec getSegmentLimit()
  {
    return super.getSegmentLimit();
  }

  @Override
  @JsonIgnore
  public OrderedLimitSpec getNodeLimit()
  {
    return super.getNodeLimit();
  }

  @Override
  @JsonIgnore
  public List<WindowingSpec> getWindowingSpecs()
  {
    return super.getWindowingSpecs();
  }

  @Override
  public LimitSpec withNoLocalProcessing()
  {
    return this;
  }

  @Override
  public LimitSpec withNoLimiting()
  {
    return this;
  }

  @Override
  public boolean isSimpleLimiter()
  {
    return true;
  }

  @Override
  public boolean isNoop()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "NoopLimitSpec{}";
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof NoopLimitSpec;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_KEY);
  }
}
