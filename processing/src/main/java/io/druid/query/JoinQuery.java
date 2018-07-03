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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JoinQuery extends BaseQuery<Map<String, Object>> implements Query.RewritingQuery<Map<String, Object>>
{
  private final Map<String, DataSource> dataSources;
  private final List<JoinElement> elements;
  private final String timeColumnName;
  private final boolean prefixAlias;
  private final int limit;
  private final int parallelism;
  private final int queue;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("dataSources") Map<String, DataSource> dataSources,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("timeColumnName") String timeColumnName,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("limit") int limit,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("queue") int queue,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(toDummyDataSource(dataSources), querySegmentSpec, false, context);
    this.dataSources = validateDataSources(dataSources, getQuerySegmentSpec());
    this.prefixAlias = prefixAlias;
    this.timeColumnName = timeColumnName;
    this.elements = validateElements(this.dataSources, Preconditions.checkNotNull(elements));
    this.limit = limit;
    this.parallelism = parallelism;   // warn : can take "(n-way + 1) x parallelism" threads
    this.queue = queue;
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
      } else if (dataSource instanceof TableDataSource) {
        dataSource = ViewDataSource.of(((TableDataSource) dataSource).getName());
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
      if (view.getColumns().isEmpty()) {
        return ds;
      }
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

  public String getTimeColumnName()
  {
    return timeColumnName;
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
  public JoinQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        dataSources,
        elements,
        prefixAlias,
        timeColumnName,
        getQuerySegmentSpec(),
        limit,
        parallelism,
        queue,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public Query<Map<String, Object>> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<Map<String, Object>> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper)
  {
    XJoinPostProcessor joinProcessor = jsonMapper.convertValue(
        ImmutableMap.of("type", "join", "elements", elements, "prefixAlias", prefixAlias),
        new TypeReference<XJoinPostProcessor>()
        {
        }
    );
    Map<String, Object> joinContext = ImmutableMap.<String, Object>of(QueryContextKeys.POST_PROCESSING, joinProcessor);

    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    List<Query<Map<String, Object>>> queries = Lists.newArrayList();
    JoinElement firstJoin = elements.get(0);
    DataSource left = dataSources.get(firstJoin.getLeftAlias());
    queries.add(JoinElement.toQuery(left, firstJoin.getLeftJoinColumns(), segmentSpec, getContext()));
    for (JoinElement element : elements) {
      DataSource right = dataSources.get(element.getRightAlias());
      queries.add(JoinElement.toQuery(right, element.getRightJoinColumns(), segmentSpec, getContext()));
    }

    JoinElement lastElement = elements.get(elements.size() - 1);

    List<String> prefixAliases;
    List<List<String>> sortColumns;
    String timeColumn;
    if (prefixAlias) {
      prefixAliases = JoinElement.getAliases(elements);
      sortColumns = Arrays.asList(
          GuavaUtils.prependEach(lastElement.getLeftAlias() + ".", lastElement.getLeftJoinColumns()),
          GuavaUtils.prependEach(lastElement.getRightAlias() + ".", lastElement.getRightJoinColumns())
      );
      timeColumn = timeColumnName == null ? prefixAliases.get(0) + "." + Column.TIME_COLUMN_NAME : timeColumnName;
    } else {
      prefixAliases = null;
      sortColumns = Arrays.asList(lastElement.getLeftJoinColumns(), lastElement.getRightJoinColumns());
      timeColumn = timeColumnName == null ? Column.TIME_COLUMN_NAME : timeColumnName;
    }

    Map<String, Object> context = computeOverriddenContext(joinContext);
    return new JoinDelegate(queries, prefixAliases, sortColumns, timeColumn, limit, parallelism, queue, context);
  }

  public JoinQuery withPrefixAlias(boolean prefixAlias)
  {
    return new JoinQuery(
        getDataSources(),
        getElements(),
        prefixAlias,
        getTimeColumnName(),
        getQuerySegmentSpec(),
        limit,
        parallelism,
        queue,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "dataSources=" + dataSources +
           ", elements=" + elements +
           ", prefixAlias=" + prefixAlias +
           ", parallelism=" + parallelism +
           ", queue=" + queue +
           ", limit=" + limit +
           '}';
  }

  @SuppressWarnings("unchecked")
  public static class JoinDelegate extends UnionAllQuery<Map<String, Object>>
      implements ArrayOutputSupport<Map<String, Object>>
  {
    private final List<String> prefixAliases;  // for schema resolving
    private final List<List<String>> sortColumns;
    private final String timeColumnName;

    public JoinDelegate(
        List<Query<Map<String, Object>>> list,
        List<String> prefixAliases,
        List<List<String>> sortColumns,
        String timeColumnName,
        int limit,
        int parallelism,
        int queue,
        Map<String, Object> context
    )
    {
      super(null, list, false, limit, parallelism, queue, context);
      this.prefixAliases = prefixAliases;
      this.sortColumns = sortColumns;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
    }

    public List<String> getPrefixAliases()
    {
      return prefixAliases;
    }

    public List<List<String>> getSortColumns()
    {
      return sortColumns;
    }

    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(
          queries,
          prefixAliases,
          sortColumns,
          timeColumnName,
          getLimit(),
          getParallelism(),
          getQueue(),
          getContext()
      );
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Query newInstance(
        Query<Map<String, Object>> query,
        List<Query<Map<String, Object>>> queries,
        Map<String, Object> context
    )
    {
      Preconditions.checkArgument(query == null);
      return new JoinDelegate(
          queries,
          prefixAliases,
          sortColumns,
          timeColumnName,
          getLimit(),
          getParallelism(),
          getQueue(),
          context
      );
    }

    @Override
    public String toString()
    {
      return "JoinDelegate{" +
             "queries=" + getQueries() +
             ", parallelism=" + getParallelism() +
             ", prefixAliases=" + getPrefixAliases() +
             ", timeColumnName=" + getTimeColumnName() +
             ", queue=" + getQueue() +
             ", limit=" + getLimit() +
             '}';
    }

    public JoinDelegate toArrayJoin()
    {
      PostProcessingOperator processor = getContextValue(QueryContextKeys.POST_PROCESSING);
      if (processor instanceof ListPostProcessingOperator) {
        List<PostProcessingOperator> processors = ((ListPostProcessingOperator<?>) processor).getProcessors();
        int index = processors.size() - 1;
        processors.set(index, ((XJoinPostProcessor) processors.get(index)).withAsArray(true));
        processor = new ListPostProcessingOperator(processors);
      } else {
        processor = ((XJoinPostProcessor) processor).withAsArray(true);
      }
      return (JoinDelegate) withOverriddenContext(QueryContextKeys.POST_PROCESSING, processor);
    }

    @Override
    public List<String> estimatedOutputColumns()
    {
      List<Query<Map<String, Object>>> queries = getQueries();
      List<String> outputColumns = Lists.newArrayList();
      for (int i = 0; i < queries.size(); i++) {
        List<String> columns = ((ArrayOutputSupport<?>) queries.get(i)).estimatedOutputColumns();
        Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(columns));
        if (prefixAliases == null) {
          outputColumns.addAll(columns);
        } else {
          String alias = prefixAliases.get(i) + ".";
          for (String column : columns) {
            outputColumns.add(alias + column);
          }
        }
      }
      return outputColumns;
    }

    @Override
    public Sequence<Object[]> array(Sequence sequence)
    {
      PostProcessingOperator processor = getContextValue(QueryContextKeys.POST_PROCESSING);
      if (processor instanceof ListPostProcessingOperator) {
        processor = ((ListPostProcessingOperator)processor).getLast();
      }
      if (((XJoinPostProcessor)processor).asArray()) {
        return sequence;
      }
      return Sequences.map(
          sequence, new Function<Map<String, Object>, Object[]>()
          {
            private final String[] columns = estimatedOutputColumns().toArray(new String[0]);

            @Override
            public Object[] apply(Map<String, Object> input)
            {
              final Object[] array = new Object[columns.length];
              for (int i = 0; i < columns.length; i++) {
                array[i] = input.get(columns[i]);
              }
              return array;
            }
          }
      );
    }
  }
}
