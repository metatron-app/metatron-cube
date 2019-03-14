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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class JoinQuery extends BaseQuery<Map<String, Object>> implements Query.RewritingQuery<Map<String, Object>>
{
  private static final Logger LOG = new Logger(JoinQuery.class);

  private final Map<String, DataSource> dataSources;
  private final List<JoinElement> elements;
  private final String timeColumnName;
  private final boolean prefixAlias;
  private final boolean asArray;
  private final int limit;
  private final int maxRowsInGroup;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("dataSources") Map<String, DataSource> dataSources,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("timeColumnName") String timeColumnName,
      @JsonProperty("limit") int limit,
      @JsonProperty("maxRowsInGroup") int maxRowsInGroup,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(toDummyDataSource(dataSources), querySegmentSpec, false, context);
    this.dataSources = validateDataSources(dataSources, getQuerySegmentSpec());
    this.prefixAlias = prefixAlias;
    this.asArray = asArray;
    this.timeColumnName = timeColumnName;
    this.elements = validateElements(this.dataSources, Preconditions.checkNotNull(elements));
    this.limit = limit;
    this.maxRowsInGroup = maxRowsInGroup;
  }

  // dummy datasource for authorization
  private static DataSource toDummyDataSource(Map<String, DataSource> dataSources)
  {
    Set<String> names = Sets.newLinkedHashSet();
    for (DataSource dataSource : dataSources.values()) {
      names.addAll(Preconditions.checkNotNull(dataSource).getNames());
    }
    return names.size() == 1 ? TableDataSource.of(Iterables.getOnlyElement(names)) : UnionDataSource.of(names);
  }

  private static Map<String, DataSource> validateDataSources(
      Map<String, DataSource> dataSources,
      QuerySegmentSpec segmentSpec
  )
  {
    Map<String, DataSource> validated = Maps.newHashMap();
    for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
      DataSource dataSource = entry.getValue();
      if (dataSource instanceof TableDataSource) {
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

  @JsonProperty
  public boolean isAsArray()
  {
    return asArray;
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

  @Override
  public boolean hasFilters()
  {
    for (DataSource dataSource : dataSources.values()) {
      if (DataSources.hasFilter(dataSource)) {
        return true;
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
        getQuerySegmentSpec(),
        elements,
        prefixAlias,
        asArray,
        timeColumnName,
        limit,
        maxRowsInGroup,
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
  public JoinDelegate rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    ObjectMapper jsonMapper = segmentWalker.getObjectMapper();
    XJoinPostProcessor joinProcessor = jsonMapper.convertValue(
        ImmutableMap.of(
            "type", "join",
            "elements", elements,
            "asArray", asArray,
            "prefixAlias", prefixAlias,
            "maxRowsInGroup", maxRowsInGroup
        ),
        new TypeReference<XJoinPostProcessor>() {}
    );
    int threshold = queryConfig.getJoin().getHashJoinThreshold();
    Map<String, Object> joinContext = ImmutableMap.<String, Object>of(QueryContextKeys.POST_PROCESSING, joinProcessor);

    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    List<Query<Map<String, Object>>> queries = Lists.newArrayList();
    for (int i = 0; i < elements.size(); i++) {
      JoinElement element = elements.get(i);
      JoinType joinType = element.getJoinType();
      boolean hashed = false;
      if (i == 0) {
        DataSource left = dataSources.get(element.getLeftAlias());
        Query query = JoinElement.toQuery(left, element.getLeftJoinColumns(), segmentSpec, getContext());
        if (threshold > 0 && joinType.isRightDrivable()) {
          long estimated = JoinElement.estimatedNumRows(left, segmentSpec, getContext(), segmentWalker, queryConfig);
          hashed = estimated > 0 && estimated < threshold;
          if (hashed) {
            query = query.withOverriddenContext(JoinElement.HASHING, true);
          }
          LOG.info("%s (L) -----> %d rows, hashed? %s", element.getLeftAlias(), estimated, hashed);
        }
        queries.add(query);
      }
      DataSource right = dataSources.get(element.getRightAlias());
      Query query = JoinElement.toQuery(right, element.getRightJoinColumns(), segmentSpec, getContext());
      if (threshold > 0 && joinType.isLeftDriving() && !hashed) {
        long estimated = JoinElement.estimatedNumRows(right, segmentSpec, getContext(), segmentWalker, queryConfig);
        hashed = estimated > 0 && estimated < threshold;
        if (hashed) {
          query = query.withOverriddenContext(JoinElement.HASHING, true);
        }
        LOG.info("%s (R) -----> %d rows, hashed? %s", element.getRightAlias(), estimated, hashed);
      }
      queries.add(query);
    }

    JoinElement lastElement = elements.get(elements.size() - 1);

    List<String> prefixAliases;
    String timeColumn;
    if (prefixAlias) {
      prefixAliases = JoinElement.getAliases(elements);
      timeColumn = timeColumnName == null ? prefixAliases.get(0) + "." + Column.TIME_COLUMN_NAME : timeColumnName;
    } else {
      prefixAliases = null;
      timeColumn = timeColumnName == null ? Column.TIME_COLUMN_NAME : timeColumnName;
    }

    // removed parallelism.. executed parallel in join post processor
    return new JoinDelegate(
        queries, prefixAliases, timeColumn, limit, computeOverriddenContext(joinContext)
    );
  }

  public JoinQuery withPrefixAlias(boolean prefixAlias)
  {
    return new JoinQuery(
        getDataSources(),
        getQuerySegmentSpec(),
        getElements(),
        prefixAlias,
        asArray,
        getTimeColumnName(),
        limit,
        maxRowsInGroup,
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
           ", asArray=" + asArray +
           ", maxRowsInGroup=" + maxRowsInGroup +
           ", limit=" + limit +
           '}';
  }

  @SuppressWarnings("unchecked")
  public static class JoinDelegate extends UnionAllQuery<Map<String, Object>>
      implements ArrayOutputSupport<Map<String, Object>>
  {
    private final List<String> prefixAliases;  // for schema resolving
    private final String timeColumnName;

    private List<String> sortedColumns;        // set when smb join is applied

    public JoinDelegate(
        List<Query<Map<String, Object>>> list,
        List<String> prefixAliases,
        String timeColumnName,
        int limit,
        Map<String, Object> context
    )
    {
      super(null, list, false, limit, -1, context);
      this.prefixAliases = prefixAliases;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
    }

    public List<String> getPrefixAliases()
    {
      return prefixAliases;
    }

    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    public List<String> getSortedColumns()
    {
      return sortedColumns;
    }

    public void setSortedColumns(List<String> sortedColumns)
    {
      // todo set this
      this.sortedColumns = sortedColumns;
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(
          queries,
          prefixAliases,
          timeColumnName,
          getLimit(),
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
          timeColumnName,
          getLimit(),
          context
      );
    }

    @Override
    public String toString()
    {
      return "JoinDelegate{" +
             "queries=" + getQueries() +
             ", prefixAliases=" + getPrefixAliases() +
             ", timeColumnName=" + getTimeColumnName() +
             ", limit=" + getLimit() +
             '}';
    }

    @Override
    public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
    {
      return this;
    }

    private XJoinPostProcessor getJoinProcessor()
    {
      PostProcessingOperator processor = getContextValue(QueryContextKeys.POST_PROCESSING);
      if (processor instanceof ListPostProcessingOperator) {
        processor = ((ListPostProcessingOperator) processor).getLast();
      }
      return (XJoinPostProcessor) processor;
    }

    private boolean isArrayOutput()
    {
      return getJoinProcessor().asArray();
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
      Set<String> uniqueNames = Sets.newHashSet();
      List<String> outputColumns = Lists.newArrayList();

      List<Query<Map<String, Object>>> queries = getQueries();
      for (int i = 0; i < queries.size(); i++) {
        List<String> columns = ((ArrayOutputSupport<?>) queries.get(i)).estimatedOutputColumns();
        Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(columns));
        if (prefixAliases == null) {
          Queries.uniqueNames(columns, uniqueNames, outputColumns);
        } else {
          String alias = prefixAliases.get(i) + ".";
          for (String column : columns) {
            outputColumns.add(Queries.uniqueName(alias + column, uniqueNames));
          }
        }
      }
      return outputColumns;
    }

    @Override
    public Sequence<Object[]> array(Sequence sequence)
    {
      if (isArrayOutput()) {
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

    public Sequence<Row> asRow(Sequence sequence)
    {
      if (isArrayOutput()) {
        final List<String> columns = estimatedOutputColumns();
        final int timeIndex = columns.indexOf(timeColumnName);
        return Sequences.map(sequence, new Function<Object[], Row>()
        {
          @Override
          public Row apply(final Object[] input)
          {
            final Map<String, Object> event = Maps.newHashMapWithExpectedSize(columns.size());
            for (int i = 0; i < columns.size(); i++) {
              if (i != timeIndex) {
                event.put(columns.get(i), input[i]);
              }
            }
            DateTime timestamp = null;
            if (timeIndex >= 0 && input[timeIndex] != null) {
              timestamp = DateTimes.utc(((Number) input[timeIndex]).longValue());
            }
            return new MapBasedRow(timestamp, event);
          }
        });
      } else {
        return Sequences.map(sequence, Rows.mapToRow(timeColumnName));
      }
    }
  }
}
