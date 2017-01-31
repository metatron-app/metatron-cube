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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DelegateQuery;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class SelectForwardQuery extends BaseQuery implements DelegateQuery
{
  @JsonProperty("query")
  private final Query query;

  public SelectForwardQuery(
      @JsonProperty("query") Query query,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.isDescending(),
        context
    );
    if (query instanceof SelectQuery) {
      SelectQuery select = (SelectQuery) query;
      int threshold = select.getPagingSpec().getThreshold();
      Preconditions.checkArgument(threshold < 0 || threshold == Integer.MAX_VALUE, "cannot use threshold");
    }
    this.query = query;
  }

  @Override
  public boolean hasFilters()
  {
    return query.hasFilters();
  }

  @Override
  public String getType()
  {
    return Query.SELECT_DELEGATE;
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SelectForwardQuery(query.withQuerySegmentSpec(spec), getContext());
  }

  @Override
  public Query withDataSource(DataSource dataSource)
  {
    return new SelectForwardQuery(query.withDataSource(dataSource), getContext());
  }

  @Override
  public Query withOverriddenContext(Map contextOverride)
  {
    return new SelectForwardQuery(query.withOverriddenContext(contextOverride), getContext());
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    return query;
  }

  @Override
  public String toString()
  {
    return "SelectDelegateQuery{" +
           "forwarding=" + query +
           toString(FINALIZE, POST_PROCESSING, FORWARD_URL, FORWARD_CONTEXT) +
           '}';
  }
}
