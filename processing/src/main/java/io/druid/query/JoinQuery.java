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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.druid.query.JoinType.LO;
import static io.druid.query.JoinType.RO;

/**
 */
@JsonTypeName("join")
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
  JoinQuery(
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
    super(UnionDataSource.of(JoinElement.getAliases(elements)), querySegmentSpec, false, context);
    this.dataSources = dataSources;
    this.prefixAlias = prefixAlias;
    this.asArray = asArray;
    this.timeColumnName = timeColumnName;
    this.elements = elements;
    this.limit = limit;
    this.maxRowsInGroup = maxRowsInGroup;
  }

  static Map<String, DataSource> normalize(Map<String, DataSource> dataSources)
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

  static List<JoinElement> validateElements(Map<String, DataSource> dataSources, List<JoinElement> elements)
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

  static final long ROWNUM_NOT_EVALUATED = -2;
  static final long ROWNUM_UNKNOWN = -1;

  @Override
  @SuppressWarnings("unchecked")
  public JoinDelegate rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final int threshold = queryConfig.getHashJoinThreshold(this);
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();

    long estimation0 = ROWNUM_UNKNOWN;
    long estimation1 = ROWNUM_UNKNOWN;
    double currentEstimation = ROWNUM_UNKNOWN;
    final List<Query<Map<String, Object>>> queries = Lists.newArrayList();
    for (int i = 0; i < elements.size(); i++) {
      JoinElement element = elements.get(i);
      JoinType joinType = element.getJoinType();
      DataSource left = dataSources.get(element.getLeftAlias());
      DataSource right = dataSources.get(element.getRightAlias());
      long rightEstimated = ROWNUM_NOT_EVALUATED;
      boolean rightHashing = false;
      if (threshold > 0 && joinType.isLeftDrivable()) {
        rightEstimated = JoinElement.estimatedNumRows(right, segmentSpec, getContext(), segmentWalker, queryConfig);
        rightHashing = rightEstimated > 0 && rightEstimated < threshold;
      }
      long leftEstimated = ROWNUM_NOT_EVALUATED;
      boolean leftHashing = false;
      if (threshold > 0 && joinType.isRightDrivable() && i == 0 && !rightHashing) {
        leftEstimated = JoinElement.estimatedNumRows(left, segmentSpec, getContext(), segmentWalker, queryConfig);
        leftHashing = leftEstimated > 0 && leftEstimated < threshold;
      }
      if (i == 0) {
        LOG.info("%s (L) -----> %d rows, hashing? %s", element.getLeftAlias(), leftEstimated, leftHashing);
        List<String> sortOn = leftHashing || (joinType != LO && rightHashing) ? null : element.getLeftJoinColumns();
        Query query = JoinElement.toQuery(left, sortOn, segmentSpec, getContext());
        if (leftHashing) {
          query = query.withOverriddenContext(JoinElement.HASHING, true);
        }
        currentEstimation = leftEstimated;
        estimation0 = leftEstimated;
        estimation1 = rightEstimated;
        queries.add(query);
      }
      LOG.info("%s (R) -----> %d rows, hashing? %s", element.getRightAlias(), rightEstimated, rightHashing);
      List<String> sortOn = (joinType != RO && leftHashing) || rightHashing ? null : element.getRightJoinColumns();
      Query query = JoinElement.toQuery(right, sortOn, segmentSpec, getContext());
      if (rightHashing) {
        query = query.withOverriddenContext(JoinElement.HASHING, true);
      }
      switch (element.getJoinType()) {
        case INNER:
          if (currentEstimation > 0 && rightEstimated > 0) {
            currentEstimation = Math.min(currentEstimation, rightEstimated);
          } else if (currentEstimation < 0) {
            currentEstimation = rightEstimated;
          }
          break;
        case LO:
          if (currentEstimation > 0 && rightEstimated > currentEstimation) {
            currentEstimation *= ((double) rightEstimated) / currentEstimation;
          }
          break;
        case RO:
          if (rightEstimated > 0) {
            if (currentEstimation > rightEstimated) {
              currentEstimation = rightEstimated * (currentEstimation / rightEstimated);
            } else {
              currentEstimation = rightEstimated;
            }
          }
          break;
        case FULL:
          currentEstimation = ROWNUM_UNKNOWN;
          break;
      }
      queries.add(query);
    }

    final Query query0 = queries.get(0);
    final Query query1 = queries.get(1);
    final JoinElement element = elements.get(0);
    final JoinType type = element.getJoinType();
    if (type.isLeftDrivable() && query0.hasFilters() &&
        (isHashed(query0) || estimation0 > 0 && estimation1 > 0 && estimation1 > estimation0 * 2)) {
      // left to right
      queries.set(1, inject(query0, element.getLeftJoinColumns(), estimation0, query1, element.getRightJoinColumns()));
    } else if (type.isRightDrivable() && query1.hasFilters() &&
               (isHashed(query1) || estimation0 > 0 && estimation1 > 0 && estimation0 > estimation1 * 2)) {
      // right to left
      queries.set(0, inject(query1, element.getRightJoinColumns(), estimation1, query0, element.getLeftJoinColumns()));
    }

    List<String> prefixAliases;
    String timeColumn;
    if (prefixAlias) {
      prefixAliases = JoinElement.getAliases(elements);
      timeColumn = timeColumnName == null ? prefixAliases.get(0) + "." + Column.TIME_COLUMN_NAME : timeColumnName;
    } else {
      prefixAliases = null;
      timeColumn = timeColumnName == null ? Column.TIME_COLUMN_NAME : timeColumnName;
    }

    // to get join config & executor service
    ObjectMapper jsonMapper = segmentWalker.getObjectMapper();
    Map<String, Object> joinProc = ImmutableMap.of(
        "type", "join",
        "elements", elements,
        "asArray", asArray,
        "prefixAlias", prefixAlias,
        "maxRowsInGroup", maxRowsInGroup
    );
    Map<String, Object> joinContext = ImmutableMap.<String, Object>of(
        QueryContextKeys.POST_PROCESSING, jsonMapper.convertValue(joinProc, JoinPostProcessor.class)
    );

    // removed parallelism.. executed parallel in join post processor
    return new JoinDelegate(
        queries, prefixAliases, timeColumn, (long) currentEstimation, limit, computeOverriddenContext(joinContext)
    );
  }

  private boolean isHashed(Query query)
  {
    return query.getContextBoolean(JoinElement.HASHING, false);
  }

  private Query inject(
      Query source,
      List<String> sourceJoinOn,
      long sourceCardinality,
      Query target,
      List<String> targetJoinOn
  )
  {
    if (source instanceof StreamQuery &&
        target instanceof Query.FilterSupport &&
        source.getDataSource() instanceof TableDataSource &&
        target.getDataSource() instanceof TableDataSource) {
      // this is evaluated in UnionQueryRunner
      List<DimensionSpec> extracted = Queries.extractInputFields(target, targetJoinOn);
      if (extracted != null) {
        ViewDataSource sourceView = BaseQuery.asView(source, sourceJoinOn);
        DimFilter factory = BloomDimFilter.Factory.fields(extracted, sourceView, Ints.checkedCast(sourceCardinality));
        return DimFilters.and((FilterSupport<?>) target, factory);
      }
    }
    return target;
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

  public JoinQuery withDataSources(Map<String, DataSource> dataSources)
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
    private final long estimatedCardinality;

    private List<OrderByColumnSpec> collation;        // set when smb join is applied

    public JoinDelegate(
        List<Query<Map<String, Object>>> list,
        List<String> prefixAliases,
        String timeColumnName,
        long estimatedCardinality,
        int limit,
        Map<String, Object> context
    )
    {
      super(null, list, false, limit, -1, context);
      this.prefixAliases = prefixAliases;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
      this.estimatedCardinality = estimatedCardinality;
    }

    public List<String> getPrefixAliases()
    {
      return prefixAliases;
    }

    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    public List<OrderByColumnSpec> getCollation()
    {
      return collation;
    }

    public void setCollation(List<OrderByColumnSpec> collation)
    {
      this.collation = collation;
    }

    public long getEstimatedCardinality()
    {
      return estimatedCardinality;
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(
          queries,
          prefixAliases,
          timeColumnName,
          estimatedCardinality,
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
          estimatedCardinality,
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

    private JoinPostProcessor getJoinProcessor()
    {
      PostProcessingOperator processor = getContextValue(QueryContextKeys.POST_PROCESSING);
      if (processor instanceof ListPostProcessingOperator) {
        processor = ((ListPostProcessingOperator) processor).getLast();
      }
      return (JoinPostProcessor) processor;
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
        processors.set(index, ((JoinPostProcessor) processors.get(index)).withAsArray(true));
        processor = new ListPostProcessingOperator(processors);
      } else {
        processor = ((JoinPostProcessor) processor).withAsArray(true);
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
