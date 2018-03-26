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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JoinQuery<T extends Comparable<T>> extends BaseQuery<T> implements Query.RewritingQuery<T>
{
  private final Map<String, DataSource> dataSources;
  private final List<JoinElement> elements;
  private final boolean prefixAlias;
  private final int numPartition;
  private final int scannerLen;
  private final int limit;
  private final int parallelism;
  private final int queue;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("dataSources") Map<String, DataSource> dataSources,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("numPartition") int numPartition,
      @JsonProperty("scannerLen") int scannerLen,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("queue") int queue,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(toDummyDataSource(dataSources), querySegmentSpec, false, context);
    this.dataSources = validateDataSources(dataSources, getQuerySegmentSpec());
    this.prefixAlias = prefixAlias;
    this.elements = validateElements(this.dataSources, Preconditions.checkNotNull(elements));
    this.numPartition = numPartition <= 0 && scannerLen <= 0 ? 1 : numPartition;
    this.scannerLen = scannerLen;
    this.limit = limit;
    this.parallelism = parallelism;   // warn : can take "(n-way + 1) x parallelism" threads
    this.queue = queue;
    Preconditions.checkArgument(
        this.numPartition > 0 || this.scannerLen > 0, "one of 'numPartition' or 'scannerLen' should be configured"
    );
  }

  // dummy datasource for authorization
  private static DataSource toDummyDataSource(Map<String, DataSource> dataSources)
  {
    Set<String> names = Sets.newLinkedHashSet();
    for (DataSource dataSource : dataSources.values()) {
      names.addAll(Preconditions.checkNotNull(dataSource).getNames());
    }
    return UnionDataSource.of(names);
  }

  private static Map<String, DataSource> validateDataSources(
      Map<String, DataSource> dataSources,
      QuerySegmentSpec segmentSpec
  )
  {
    Map<String, DataSource> validated = Maps.newHashMap();
    for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
      DataSource dataSource = entry.getValue();
      if (dataSource instanceof QueryDataSource) {
        Query query = ((QueryDataSource) dataSource).getQuery();
        QuerySegmentSpec currentSpec = query.getQuerySegmentSpec();
        if (currentSpec == null || currentSpec.getIntervals().isEmpty()) {
          dataSource = new QueryDataSource(query.withQuerySegmentSpec(segmentSpec));
        }
      }
      validated.put(entry.getKey(), dataSource);
    }
    return validated;
  }

  private static List<JoinElement> validateElements(Map<String, DataSource> dataSources, List<JoinElement> elements)
  {
    Preconditions.checkArgument(!elements.isEmpty());
    List<JoinElement> rewrite = Lists.newArrayList();
    for (JoinElement element : elements) {
      rewrite.add(element.rewrite(dataSources.keySet()));
    }
    JoinElement firstJoin = rewrite.get(0);
    validateDataSource(dataSources, firstJoin.getLeftAlias(), firstJoin.getLeftJoinColumns());
    for (JoinElement element : rewrite) {
      validateDataSource(dataSources, element.getRightAlias(), element.getRightJoinColumns());
    }
    return rewrite;
  }

  // I hate this.. just add join columns into view columns.. is it so hard?
  private static void validateDataSource(Map<String, DataSource> dataSources, String alias, List<String> joinColumns)
  {
    DataSource dataSource = dataSources.get(alias);
    Preconditions.checkNotNull(dataSource, "failed to find alias " + alias);
    DataSource rewritten = rewriteDataSource(dataSource, joinColumns);
    if (rewritten != null) {
      dataSources.put(alias, rewritten);
    }
  }

  private static DataSource rewriteDataSource(DataSource ds, List<String> joinColumns)
  {
    if (ds instanceof ViewDataSource) {
      ViewDataSource view = (ViewDataSource) ds;
      boolean changed = false;
      List<String> columns = Lists.newArrayList(view.getColumns());
      for (String joinColumn : joinColumns) {
        if (!columns.contains(joinColumn)) {
          columns.add(joinColumn);
          changed = true;
        }
      }
      if (changed) {
        return view.withColumns(columns);
      }
    }
    return null;
  }

  @JsonProperty
  public Map<String, DataSource> getDataSources()
  {
    return dataSources;
  }

  @JsonProperty
  public List<JoinElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public boolean isPrefixAlias()
  {
    return prefixAlias;
  }

  @JsonProperty
  public int getNumPartition()
  {
    return numPartition;
  }

  @JsonProperty
  public int getScannerLen()
  {
    return scannerLen;
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
    for (DataSource dataSource : dataSources.values()) {
      if (dataSource instanceof QueryDataSource) {
        Query query = ((QueryDataSource) dataSource).getQuery();
        if (query.hasFilters()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String getType()
  {
    return Query.JOIN;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        dataSources,
        elements,
        prefixAlias,
        getQuerySegmentSpec(),
        numPartition,
        scannerLen,
        limit,
        parallelism,
        queue,
        computeOverridenContext(contextOverride)
    );
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

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    String queryId = getId();
    JoinPostProcessor joinProcessor = jsonMapper.convertValue(
        ImmutableMap.of("type", "join", "elements", elements, "prefixAlias", prefixAlias),
        new TypeReference<JoinPostProcessor>()
        {
        }
    );
    Map<String, Object> joinContext = ImmutableMap.<String, Object>of(QueryContextKeys.POST_PROCESSING, joinProcessor);

    final List<String> aliases = Lists.newArrayList();
    aliases.add(elements.get(0).getLeftAlias());
    for (JoinElement element : elements) {
      aliases.add(element.getRightAlias());
    }
    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    JoinPartitionSpec partitions = partition(segmentWalker, jsonMapper);
    if (partitions == null || partitions.size() == 1) {
      List<Query> queries = Lists.newArrayList();
      JoinElement firstJoin = elements.get(0);
      queries.add(JoinElement.toQuery(dataSources.get(firstJoin.getLeftAlias()), segmentSpec).withId(queryId));
      for (JoinElement element : elements) {
        queries.add(JoinElement.toQuery(dataSources.get(element.getRightAlias()), segmentSpec).withId(queryId));
      }
      Map<String, Object> context = computeOverridenContext(joinContext);
      return new JoinDelegate(queries, prefixAlias ? aliases : null, limit, parallelism, queue, context);
    }

    JoinElement element = Preconditions.checkNotNull(Iterables.getFirst(elements, null));

    boolean first = true;
    List<Query> queries = Lists.newArrayList();
    for (List<DimFilter> filters : partitions) {
      List<Query> partitioned = Lists.newArrayList();
      DataSource left = dataSources.get(element.getLeftAlias());
      DataSource right = dataSources.get(element.getRightAlias());
      partitioned.add(JoinElement.toQuery(left, segmentSpec, filters.get(0)).withId(queryId));
      partitioned.add(JoinElement.toQuery(right, segmentSpec, filters.get(1)).withId(queryId));
      if (first) {
        for (int i = 1; i < elements.size(); i++) {
          DataSource dataSource = dataSources.get(elements.get(i).getRightAlias());
          partitioned.add(
              JoinElement.toQuery(dataSource, segmentSpec)
                         .withOverriddenContext(ImmutableMap.of("hash", true))
                         .withId(queryId)
          );
        }
      }
      Map<String, Object> context = computeOverridenContext(joinContext);
      queries.add(new JoinDelegate(partitioned, prefixAlias ? aliases : null, -1, 0, 0, context));
      first = false;
    }
    return new UnionAllQuery(null, queries, false, limit, parallelism, queue, Maps.newHashMap(getContext()));
  }

  private JoinPartitionSpec partition(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    if (numPartition <= 1 && scannerLen <= 0) {
      return null;
    }
    for (DataSource dataSource : dataSources.values()) {
      Preconditions.checkArgument(
          !(dataSource instanceof QueryDataSource) ||
          ((QueryDataSource) dataSource).getQuery() instanceof DimFilterSupport,
          "cannot apply partition on dataSource " + dataSource
      );
    }
    JoinElement element = elements.get(0);
    boolean rightJoin = element.getJoinType() == JoinType.RO;
    String alias = rightJoin ? element.getRightAlias() : element.getLeftAlias();
    String partitionKey = rightJoin ? element.getRightJoinColumns().get(0) : element.getLeftJoinColumns().get(0);

    DataSource dataSource = dataSources.get(alias);
    List<String> partitions = QueryUtils.runSketchQuery(
        new DummyQuery(dataSource, getQuerySegmentSpec(), false, getContext()),
        segmentWalker,
        jsonMapper,
        partitionKey,
        numPartition,
        scannerLen
    );
    if (partitions != null && partitions.size() > 2) {
      return new JoinPartitionSpec(element.getFirstKeys(), partitions);
    }
    return null;
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "dataSources=" + dataSources +
           ", elements=" + elements +
           ", prefixAlias=" + prefixAlias +
           ", numPartition=" + numPartition +
           ", scannerLen=" + scannerLen +
           ", parallelism=" + parallelism +
           ", queue=" + queue +
           ", limit=" + limit +
           '}';
  }

  private static class JoinPartitionSpec implements Iterable<List<DimFilter>>
  {
    final String[] firstKeys;
    final List<String> partitions;

    private JoinPartitionSpec(
        String[] firstKeys,
        List<String> partitions
    )
    {
      this.firstKeys = firstKeys;
      this.partitions = Preconditions.checkNotNull(partitions);
    }

    @Override
    public Iterator<List<DimFilter>> iterator()
    {
      final List<Iterator<DimFilter>> filters = Lists.newArrayList();
      for (String firstKey : firstKeys) {
        filters.add(QueryUtils.toFilters(firstKey, partitions).iterator());
      }
      return new Iterator<List<DimFilter>>()
      {
        @Override
        public boolean hasNext()
        {
          return filters.get(0).hasNext();
        }

        @Override
        public List<DimFilter> next()
        {
          List<DimFilter> result = Lists.newArrayList();
          for (Iterator<DimFilter> filter : filters) {
            result.add(filter.next());
          }
          return result;
        }
      };
    }

    public int size()
    {
      return partitions.size();
    }
  }

  @SuppressWarnings("unchecked")
  public static class JoinDelegate<T extends Comparable<T>> extends UnionAllQuery<T>
  {
    private final List<String> prefixAliases;  // for schema resolving

    public JoinDelegate(
        List<Query<T>> list,
        List<String> prefixAliases,
        int limit,
        int parallelism,
        int queue,
        Map<String, Object> context
    )
    {
      super(null, list, false, limit, parallelism, queue, context);
      this.prefixAliases = prefixAliases;
    }

    public List<String> getPrefixAliases()
    {
      return prefixAliases;
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(queries, prefixAliases, getLimit(), getParallelism(), getQueue(), getContext());
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Query<T> newInstance(Query<T> query, List<Query<T>> queries, Map<String, Object> context)
    {
      Preconditions.checkArgument(query == null);
      return new JoinDelegate(queries, prefixAliases, getLimit(), getParallelism(), getQueue(), context);
    }

    @Override
    public String toString()
    {
      return "JoinDelegate{" +
             "queries=" + getQueries() +
             ", parallelism=" + getParallelism() +
             ", prefixAliases=" + getPrefixAliases() +
             ", queue=" + getQueue() +
             ", limit=" + getLimit() +
             '}';
    }
  }
}
