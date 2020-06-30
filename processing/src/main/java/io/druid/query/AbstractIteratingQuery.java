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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
public abstract class AbstractIteratingQuery<I, F> extends BaseQuery<I> implements Query.IteratingQuery<I, F>
{
  protected int index;
  protected final Query<I> query;

  public AbstractIteratingQuery(
      @JsonProperty("query") Query<I> query,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), context);
    this.query = query;
  }

  @Override
  public String getType()
  {
    return getClass().getAnnotation(JsonTypeName.class).value();
  }

  @Override
  public Query<I> withOverriddenContext(Map<String, Object> contextOverride)
  {
    Map<String, Object> context = computeOverriddenContext(contextOverride);
    return newInstance(query.withOverriddenContext(contextOverride), context);
  }

  @Override
  public Query<I> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return newInstance(query.withQuerySegmentSpec(spec), getContext());
  }

  @Override
  public Query<I> withDataSource(DataSource dataSource)
  {
    return newInstance(query.withDataSource(dataSource), getContext());
  }

  protected abstract Query<I> newInstance(Query<I> query, Map<String, Object> context);
}
