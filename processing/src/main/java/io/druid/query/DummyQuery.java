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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
@JsonTypeName("dummy")
public class DummyQuery<T> extends BaseQuery<T> implements QueryRunner<T>
{
  protected final Sequence<T> sequence;

  public static DummyQuery instance()
  {
    return new DummyQuery<>(TableDataSource.of("<NOT-EXISTING>"), null, false, null, Maps.<String, Object>newHashMap());
  }

  public static <T> DummyQuery of(Sequence<T> sequence)
  {
    return new DummyQuery(TableDataSource.of("<NOT-EXISTING>"), null, false, sequence, Maps.<String, Object>newHashMap());
  }

  protected DummyQuery(
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
  public QueryRunner<T> makeQueryRunner(QuerySegmentWalker walker)
  {
    QueryRunner<T> runner = this;
    if (walker instanceof ForwardingSegmentWalker) {
      runner = ((ForwardingSegmentWalker) walker).handle(this, runner);
    }
    return runner;
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return sequence.columns();
  }

  @Override
  public Query<T> resolveQuery(Supplier<RowResolver> resolver, boolean expand)
  {
    return this;
  }

  @Override
  public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    return sequence;
  }
}
