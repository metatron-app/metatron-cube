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

import io.druid.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

public class MaterializedQuery extends DummyQuery<Object[]> implements Query.ArrayOutput
{
  public static MaterializedQuery of(DataSource dataSource, Sequence<Object[]> sequence, Map<String, Object> context)
  {
    return new MaterializedQuery(dataSource, null, false, sequence, context);
  }

  protected MaterializedQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Sequence<Object[]> sequence,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, descending, sequence, context);
  }

  @Override
  public MaterializedQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new MaterializedQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public MaterializedQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new MaterializedQuery(
        getDataSource(),
        spec,
        isDescending(),
        sequence,
        getContext()
    );
  }

  @Override
  public MaterializedQuery withDataSource(DataSource dataSource)
  {
    return new MaterializedQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        sequence,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "MaterializedQuery{dataSource=" + getDataSource().getNames() + "}";
  }
}
