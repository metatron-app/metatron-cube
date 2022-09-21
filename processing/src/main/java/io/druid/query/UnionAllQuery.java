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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.input.Row;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 */
public class UnionAllQuery<T> extends BaseQuery<T> implements Query.RewritingQuery<T>
{
  private static final Logger LOG = new Logger(UnionAllQuery.class);

  public static UnionAllQuery union(List<Query> queries)
  {
    return union(queries, -1, -1, null);
  }

  public static UnionAllQuery union(List<Query> queries, int limit)
  {
    return union(queries, limit, -1, null);
  }

  public static UnionAllQuery union(List<Query> queries, int limit, Map<String, Object> context)
  {
    return union(queries, limit, -1, context);
  }

  @SuppressWarnings("unchecked")
  public static UnionAllQuery union(List<Query> queries, int limit, int parallelism, Map<String, Object> context)
  {
    return new UnionAllQuery(null, queries, false, limit, parallelism, context != null ? context : Maps.newHashMap());
  }

  @SuppressWarnings("unchecked")
  public static UnionAllQuery union(
      List<Query> queries,
      Query.SchemaProvider provider,
      QuerySegmentWalker segmentWalker
  )
  {
    return union(queries).withSchema(Suppliers.memoize(() -> provider.schema(segmentWalker)));
  }

  // dummy datasource for authorization
  static <T> DataSource unionDataSource(Query<T> query, List<Query<T>> queries)
  {
    if (GuavaUtils.isNullOrEmpty(queries)) {
      return DataSources.from(Preconditions.checkNotNull(query).getDataSource().getNames());
    }
    Set<String> names = Sets.newLinkedHashSet();
    for (Query q : queries) {
      names.addAll(q.getDataSource().getNames());
    }
    return DataSources.from(Lists.newArrayList(names));
  }

  private static <T> QuerySegmentSpec unionQuerySegmentSpec(Query<T> query, List<Query<T>> queries)
  {
    if (queries == null || queries.isEmpty()) {
      return Preconditions.checkNotNull(query).getQuerySegmentSpec();
    }
    List<Interval> intervals = Lists.newArrayList();
    for (Query q : queries) {
      QuerySegmentSpec segmentSpec = q.getQuerySegmentSpec();
      if (segmentSpec == null) {
        return null;
      }
      intervals.addAll(segmentSpec.getIntervals());
    }
    return new MultipleIntervalSegmentSpec(JodaUtils.condenseIntervals(intervals));
  }

  private final Query<T> query;
  private final List<Query<T>> queries;
  private final boolean sortOnUnion;
  private final int limit;
  private final int parallelism;

  protected transient Supplier<RowSignature> schema;  //  optional schema provider

  @JsonCreator
  public UnionAllQuery(
      @JsonProperty("query") Query<T> query,
      @JsonProperty("queries") List<Query<T>> queries,
      @JsonProperty("sortOnUnion") boolean sortOnUnion,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(unionDataSource(query, queries), unionQuerySegmentSpec(query, queries), false, context);
    this.query = query;
    this.queries = queries;
    this.sortOnUnion = sortOnUnion;
    this.limit = limit;
    this.parallelism = parallelism;
  }

  public RowSignature getSchema()
  {
    return schema == null ? null : schema.get();
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

  public Query getFirst()
  {
    return query != null ? query : Iterables.getFirst(queries, null);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Query<T>> getQueries()
  {
    return queries;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
  public Query<T> withOverriddenContext(final Map<String, Object> contextOverride)
  {
    return newInstance(query, queries, parallelism, computeOverriddenContext(contextOverride));
  }

  @SuppressWarnings("unchecked")
  protected UnionAllQuery newInstance(
      Query<T> query,
      List<Query<T>> queries,
      int parallelism,
      Map<String, Object> context
  )
  {
    return new UnionAllQuery(query, queries, sortOnUnion, limit, parallelism, context).withSchema(schema);
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

  public UnionAllQuery withQueries(List<Query> queries)
  {
    return newInstance(null, GuavaUtils.cast(queries), parallelism, getContext());
  }

  @SuppressWarnings("unchecked")
  public UnionAllQuery withQuery(Query query)
  {
    return newInstance(query, null, parallelism, getContext());
  }

  public UnionAllQuery withParallelism(int parallelism)
  {
    return newInstance(query, queries, parallelism, getContext());
  }

  public UnionAllQuery withSchema(Supplier<RowSignature> schema)
  {
    this.schema = schema;
    return this;
  }

  @SuppressWarnings("unchecked")
  public Sequence<Row> asRow(Sequence sequence)
  {
    RowSignature schema = getSchema();
    if (schema == null) {
      return Sequences.map(sequence, GuavaUtils.<Object, Row>caster());
    }
    return Sequences.map(sequence, ArrayToRow.arrayToRow(ImmutableList.copyOf(schema.getColumnNames())));
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
    if (limit != that.limit) {
      return false;
    }
    if (parallelism != that.parallelism) {
      return false;
    }
    if (!Objects.equals(queries, that.queries)) {
      return false;
    }
    if (!Objects.equals(query, that.query)) {
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
    return result;
  }

  @Override
  public String toString()
  {
    return "UnionAllQuery{" +
           (query != null ? "query=" + query : "queries=" + queries) +
           ", sortOnUnion=" + sortOnUnion +
           ", limit=" + limit +
           ", parallelism=" + parallelism +
           '}';
  }


  public QueryRunner<T> getUnionQueryRunner(final QuerySegmentWalker segmentWalker, final QueryConfig queryConfig)
  {
    return PostProcessingOperators.wrap(toUnionAllRunner(segmentWalker, queryConfig), segmentWalker);
  }

  @SuppressWarnings("unchecked")
  private UnionAllQueryRunner<T> toUnionAllRunner(
      final QuerySegmentWalker segmentWalker,
      final QueryConfig queryConfig
  )
  {
    if (parallelism <= 1) {
      return new UnionAllQueryRunner<T>()
      {
        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(
            final Query<T> union,
            final Map<String, Object> responseContext
        )
        {
          final List<Query<T>> queries = ((UnionAllQuery<T>) union).toTargetQueries();
          final Pair<Query<T>, Sequence<T>>[] sequences = new Pair[queries.size()];
          for (int i = 0; i < sequences.length; i++) {
            Query<T> query = queries.get(i);
            if (Queries.isNestedQuery(queries.get(i))) {
              sequences[i] = Pair.<Query<T>, Sequence<T>>of(
                  query, QueryUtils.rewrite(query, segmentWalker, queryConfig)
                                   .run(segmentWalker, responseContext)
              );
            }
          }
          for (int i = 0; i < sequences.length; i++) {
            Query<T> query = queries.get(i);
            if (!Queries.isNestedQuery(queries.get(i))) {
              sequences[i] = Pair.<Query<T>, Sequence<T>>of(
                  query, QueryUtils.rewrite(query, segmentWalker, queryConfig)
                                   .run(segmentWalker, responseContext)
              );
            }
          }
          return Sequences.of(sequences);
        }
      };
    } else {
      // executing now
      return new UnionAllQueryRunner<T>()
      {
        final int priority = BaseQuery.getContextPriority(UnionAllQuery.this, 0);

        @Override
        public Sequence<Pair<Query<T>, Sequence<T>>> run(
            final Query<T> query,
            final Map<String, Object> responseContext
        )
        {
          final List<Query<T>> ready = ((UnionAllQuery) query).toTargetQueries();
          final Execs.Semaphore semaphore = new Execs.Semaphore(Math.min(getParallelism(), ready.size()));
          LOG.info("Starting %d parallel works with %d threads", ready.size(), semaphore.availablePermits());
          final List<ListenableFuture<Sequence<T>>> futures = Execs.execute(
              segmentWalker.getExecutor(), Lists.transform(
                  ready, new Function<Query<T>, Callable<Sequence<T>>>()
                  {
                    @Override
                    public Callable<Sequence<T>> apply(final Query<T> query)
                    {
                      return new Callable<Sequence<T>>()
                      {
                        @Override
                        public Sequence<T> call()
                        {
                          Sequence<T> sequence = QueryUtils.rewrite(query, segmentWalker, queryConfig)
                                                           .run(segmentWalker, responseContext);
                          return Sequences.materialize(Sequences.withBaggage(sequence, semaphore));    // eagely
                        }

                        @Override
                        public String toString()
                        {
                          return query.toString();
                        }
                      };
                    }
                  }
              ), semaphore, priority
          );
          Sequence<Pair<Query<T>, Sequence<T>>> sequence = Sequences.simple(
              GuavaUtils.zip(ready, Lists.transform(futures, FutureSequence.<T>toSequence()))
          );
          return Sequences.withBaggage(sequence, () -> semaphore.destroy());
        }
      };
    }
  }

  private List<Query<T>> toTargetQueries()
  {
    return queries != null ? queries : Arrays.asList(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker)
  {
    if (query != null && query.getDataSource() instanceof UnionDataSource) {
      List<String> dataSources = ((UnionDataSource) query.getDataSource()).getDataSources();
      List<Query> queries = GuavaUtils.transform(dataSources, ds -> query.withDataSource(TableDataSource.of(ds)));
      return withQueries(queries).withSchema(Suppliers.memoize(() -> Queries.relaySchema(query, segmentWalker)));
    }
    return this;
  }
}
