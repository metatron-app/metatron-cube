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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class UnionAllQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  // dummy datasource for authorization
  private static <T> DataSource unionDataSource(Query<T> query, List<Query<T>> queries)
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query).getDataSource();
    }
    Set<String> names = Sets.newLinkedHashSet();
    for (Query q : queries) {
      names.addAll(q.getDataSource().getNames());
    }
    return UnionDataSource.of(names);
  }

  private final Query<T> query;
  private final List<Query<T>> queries;
  private final boolean sortOnUnion;
  private final int limit;
  private final int parallelism;
  private final int queue;

  @JsonCreator
  public UnionAllQuery(
      @JsonProperty("query") Query<T> query,
      @JsonProperty("queries") List<Query<T>> queries,
      @JsonProperty("sortOnUnion") boolean sortOnUnion,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("queue") int queue,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(unionDataSource(query, queries), null, false, context);
    this.query = query;
    this.queries = queries;
    this.sortOnUnion = sortOnUnion;
    this.limit = limit;
    this.parallelism = parallelism;
    this.queue = queue;
  }

  public Query getRepresentative()
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query);
    }
    Preconditions.checkArgument(query == null);
    Preconditions.checkArgument(!Iterables.contains(queries, null), "should not contain null query in union");
    Query<T> first = queries.get(0);
    for (int i = 1; i < queries.size(); i++) {
      if (!first.getType().equals(queries.get(i).getType())) {
        throw new IllegalArgumentException("sub queries in union should not be mixed");
      }
    }
    return first;
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

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty
  public int getParallelism()
  {
    return parallelism;
  }

  @JsonProperty
  public int getQueue()
  {
    return queue;
  }

  @Override
  public boolean hasFilters()
  {
    if (query != null) {
      return query.hasFilters();
    }
    for (Query q : queries) {
      if (q.hasFilters()) {
        return true;
      }
    }
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
    Map<String, Object> context = computeOverridenContext(contextOverride);
    if (queries == null) {
      return new UnionAllQuery(query, null, sortOnUnion, limit, parallelism, queue, context);
    }
    return new UnionAllQuery(null, queries, sortOnUnion, limit, parallelism, queue, context);
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
    return new UnionAllQuery(null, queries, sortOnUnion, limit, parallelism, queue, getContext());
  }

  @SuppressWarnings("unchecked")
  public Query withQuery(Query query)
  {
    return new UnionAllQuery(query, null, sortOnUnion, limit, parallelism, queue, getContext());
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
    if (limit != that.limit) {
      return false;
    }
    if (parallelism != that.parallelism) {
      return false;
    }
    if (queue != that.queue) {
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
    result = 31 * result + limit;
    result = 31 * result + parallelism;
    result = 31 * result + queue;
    return result;
  }

  @Override
  public String toString()
  {
    return "UnionAllQuery{" +
           "query=" + query +
           ", queries=" + queries +
           ", sortOnUnion=" + sortOnUnion +
           ", limit=" + limit +
           ", parallelism=" + parallelism +
           ", queue=" + queue +
           '}';
  }

}
