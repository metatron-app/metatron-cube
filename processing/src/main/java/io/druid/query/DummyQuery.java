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
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class DummyQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  public DummyQuery()
  {
    this(new TableDataSource("<NOT-EXISTING>"), null, false, Maps.<String, Object>newHashMap());
  }

  public DummyQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, context);
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
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public DummyQuery<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new DummyQuery<T>(
        getDataSource(),
        spec,
        isDescending(),
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
        getContext()
    );
  }
}
