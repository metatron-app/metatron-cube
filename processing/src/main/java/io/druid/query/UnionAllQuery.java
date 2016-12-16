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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;

/**
 */
public class UnionAllQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  private final Query<T> query;
  private final List<Query<T>> queries;
  private final boolean sortOnUnion;

  @JsonCreator
  public UnionAllQuery(
      @JsonProperty("query") Query<T> query,
      @JsonProperty("queries") List<Query<T>> queries,
      @JsonProperty("sortOnUnion") boolean sortOnUnion,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(getFirstQueryWithValidation(query, queries), false, context);
    this.query = getFirstQueryWithValidation(query, queries);
    this.queries = queries;
    this.sortOnUnion = sortOnUnion;
  }

  @JsonProperty
  public List<Query<T>> getQueries()
  {
    return queries;
  }

  @JsonProperty
  public Query<T> getQuery()
  {
    return query;
  }

  @JsonProperty
  public boolean isSortOnUnion()
  {
    return sortOnUnion;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return Query.UNION_ALL;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    if (queries == null) {
      return new UnionAllQuery(query, null, sortOnUnion, computeOverridenContext(contextOverride));
    }
    return new UnionAllQuery(null, queries, sortOnUnion, computeOverridenContext(contextOverride));
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  @SuppressWarnings("unchecked")
  public Query withQueries(List<Query> queries)
  {
    return new UnionAllQuery(null, queries, sortOnUnion, getContext());
  }

  @SuppressWarnings("unchecked")
  public Query withQuery(Query query)
  {
    return new UnionAllQuery(query, null, sortOnUnion, getContext());
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
    if (!super.equals(o)) {
      return false;
    }

    UnionAllQuery that = (UnionAllQuery) o;

    if (sortOnUnion != that.sortOnUnion) {
      return false;
    }
    if (queries != null ? !queries.equals(that.queries) : that.queries != null) {
      return false;
    }
    if (query != null ? !query.equals(that.query) : that.query != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (query != null ? query.hashCode() : 0);
    result = 31 * result + (queries != null ? queries.hashCode() : 0);
    result = 31 * result + (sortOnUnion ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "UnionAllQuery{" +
           "query=" + query +
           ", queries=" + queries +
           ", sortOnUnion=" + sortOnUnion +
           '}';
  }
}
