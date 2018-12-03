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

import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class DummyQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  private final Sequence<T> sequence;

  public DummyQuery()
  {
    this(TableDataSource.of("<NOT-EXISTING>"), null, false, null, Maps.<String, Object>newHashMap());
  }

  public DummyQuery(Sequence<T> sequence)
  {
    this(TableDataSource.of("<NOT-EXISTING>"), null, false, sequence, Maps.<String, Object>newHashMap());
  }

  private DummyQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Sequence<T> sequence,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
    this.sequence = sequence == null ? Sequences.<T>empty() : sequence;
  }

  @Override
  public String getType()
  {
    return "dummy";
  }

  @Override
  public DummyQuery<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new DummyQuery<T>(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public DummyQuery<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new DummyQuery<T>(
        getDataSource(),
        spec,
        isDescending(),
        sequence,
        getContext()
    );
  }

  @Override
  public DummyQuery<T> withDataSource(DataSource dataSource)
  {
    return new DummyQuery<T>(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        getContext()
    );
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return sequence;
  }

  @Override
  public Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context)
  {
    return sequence;
  }
}
