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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.data.ValueDesc;
import io.druid.data.input.BulkRow;
import io.druid.data.input.BulkSequence;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ValuesFilter;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.druid.query.JoinType.LO;
import static io.druid.query.JoinType.RO;

/**
 */
@JsonTypeName("join")
public class JoinQuery extends BaseQuery<Map<String, Object>> implements Query.RewritingQuery<Map<String, Object>>
{
  public static final String HASHING = "$hash";
  public static final String CARDINALITY = "$cardinality";

  private static final Logger LOG = new Logger(JoinQuery.class);

  private final Map<String, DataSource> dataSources;
  private final List<JoinElement> elements;
  private final String timeColumnName;
  private final boolean prefixAlias;
  private final boolean asArray;
  private final int limit;
  private final int maxOutputRow;
  private final List<String> outputColumns;

  private transient RowSignature schema;

  @JsonCreator
  JoinQuery(
      @JsonProperty("dataSources") Map<String, DataSource> dataSources,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("prefixAlias") boolean prefixAlias,
      @JsonProperty("asArray") boolean asArray,
      @JsonProperty("timeColumnName") String timeColumnName,
      @JsonProperty("limit") int limit,
      @JsonProperty("maxOutputRow") int maxOutputRow,
      @JsonProperty("outputColumns") List<String> outputColumns,
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
    this.maxOutputRow = maxOutputRow;
    this.outputColumns = outputColumns;
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
    Preconditions.checkNotNull(dataSource, "failed to find alias %s", alias);
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

  @Override
  public String getType()
  {
    return Query.JOIN;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getOutputColumns()
  {
    return outputColumns;
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

  public JoinQuery withSchema(RowSignature schema)
  {
    this.schema = schema;
    return this;
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
        maxOutputRow,
        outputColumns,
        computeOverriddenContext(contextOverride)
    ).withSchema(schema);
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

  public static final long ROWNUM_UNKNOWN = -1;
  public static final long ROWNUM_NOT_EVALUATED = -2;

  // use it if already set
  public static long estimatedNumRows(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      return query.getContextLong(CARDINALITY, ROWNUM_NOT_EVALUATED);
    }
    return ROWNUM_NOT_EVALUATED;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig config)
  {
    if (schema != null && outputColumns != null) {
      Preconditions.checkArgument(
          outputColumns.equals(schema.getColumnNames()),
          "Invalid schema %s, expected column names %s", schema, outputColumns
      );
    }
    final int hashThreshold = config.getHashJoinThreshold(this);
    final int semiJoinThrehold = config.getSemiJoinThreshold(this);
    final int broadcastThreshold = config.getBroadcastJoinThreshold(this);
    final int bloomFilterThreshold = config.getBloomFilterThreshold(this);

    long currentEstimation = ROWNUM_UNKNOWN;

    final Map<String, Object> context = getContext();
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    final List<Query<Map<String, Object>>> queries = Lists.newArrayList();

    for (int i = 0; i < elements.size(); i++) {
      final JoinElement element = elements.get(i);
      final JoinType joinType = element.getJoinType();
      final List<String> leftJoinColumns = element.getLeftJoinColumns();
      final List<String> rightJoinColumns = element.getRightJoinColumns();

      final String leftAlias = element.getLeftAlias();
      final String rightAlias = element.getRightAlias();

      final DataSource left = i == 0 ? dataSources.get(leftAlias) : null;
      final DataSource right = dataSources.get(rightAlias);

      long leftEstimated = i == 0 ? estimatedNumRows(left) : currentEstimation;
      if (leftEstimated == ROWNUM_NOT_EVALUATED) {
        leftEstimated = JoinElement.estimatedNumRows(left, segmentSpec, context, segmentWalker, config);
      }
      long rightEstimated = estimatedNumRows(right);
      if (rightEstimated == ROWNUM_NOT_EVALUATED) {
        rightEstimated = JoinElement.estimatedNumRows(right, segmentSpec, context, segmentWalker, config);
      }
      LOG.info(">> %s (%s:%d + %s:%d)", element.getJoinType(), leftAlias, leftEstimated, rightAlias, rightEstimated);

      // try convert semi-join to filter
      if (semiJoinThrehold > 0 && joinType == JoinType.INNER && outputColumns != null) {
        if (i == 0 && isUnderThreshold(rightEstimated, semiJoinThrehold) && element.isLeftSemiJoinable(left, right, outputColumns)) {
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          List<String> rightColumns = array.estimatedOutputColumns();
          if (rightColumns != null && rightColumns.containsAll(rightJoinColumns)) {
            int[] indices = GuavaUtils.indexOf(rightColumns, rightJoinColumns);
            Supplier<List<Object[]>> fieldValues =
                () -> Sequences.toList(Sequences.map(
                    array.array(array.run(segmentWalker, null)), GuavaUtils.mapper(indices)));
            DataSource filtered = DataSources.applyFilterAndProjection(
                left, ValuesFilter.fieldNames(leftJoinColumns, fieldValues), outputColumns
            );
            LOG.info(
                "%s (R) is merged into %s (L) as a filter with expected number of values = %d",
                rightAlias, leftAlias, rightEstimated
            );
            queries.add(JoinElement.toQuery(segmentWalker, filtered, segmentSpec, context));
            if (leftEstimated >= 0) {
              currentEstimation = resultEstimation(joinType, leftEstimated, rightEstimated);
            }
            continue;
          }
        }
        if (isUnderThreshold(leftEstimated, semiJoinThrehold) && element.isRightSemiJoinable(left, right, outputColumns)) {
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          List<String> leftColumns = array.estimatedOutputColumns();
          if (leftColumns != null && leftColumns.containsAll(leftJoinColumns)) {
            int[] indices = GuavaUtils.indexOf(leftColumns, leftJoinColumns);
            Supplier<List<Object[]>> fieldValues =
                () -> Sequences.toList(Sequences.map(
                    array.array(array.run(segmentWalker, null)), GuavaUtils.mapper(indices)));
            DataSource filtered = DataSources.applyFilterAndProjection(
                right, ValuesFilter.fieldNames(rightJoinColumns, fieldValues), outputColumns
            );
            LOG.info(
                "%s (L) is merged into %s (R) as a filter with expected number of values = %d",
                leftAlias, rightAlias, leftEstimated
            );
            queries.add(JoinElement.toQuery(segmentWalker, filtered, segmentSpec, context));
            if (rightEstimated >= 0) {
              currentEstimation = resultEstimation(joinType, leftEstimated, rightEstimated);
            }
            continue;
          }
        }
      }

      if (i == 0 && broadcastThreshold > 0) {
        boolean leftBroadcast = isUnderThreshold(leftEstimated, broadcastThreshold) && element.isLeftBroadcastable(right);
        boolean rightBroadcast = isUnderThreshold(rightEstimated, broadcastThreshold) && element.isRightBroadcastable(left);
        if (leftBroadcast && rightBroadcast) {
          if (leftEstimated > rightEstimated) {
            leftBroadcast = false;
          } else {
            rightBroadcast = false;
          }
        }
        if (leftBroadcast) {
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info(
              "%s (L) is broadcasted for %s (R) with expected number of values = %d",
              leftAlias, rightAlias, leftEstimated
          );
          RowSignature signature = Queries.relaySchema(query0, segmentWalker);
          Sequence<BulkRow> values = BulkSequence.fromArray(
              query0.array(QueryRunners.run(query0, segmentWalker)), signature
          );
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              config.getJoin(), element, true, signature, prefixAlias, asArray, outputColumns, values
          );
          Map<String, Object> joinContext = BaseQuery.copyContextForMeta(context, Query.POST_PROCESSING, processor);
          if (rightEstimated > 0) {
            joinContext.put(CARDINALITY, rightEstimated);
          }
          queries.add(new BroadcastJoinHolder(query1, processor, joinContext));
          if (rightEstimated > 0) {
            currentEstimation = rightEstimated;
          }
          continue;
        }
        if (rightBroadcast) {
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info(
              "%s (R) is broadcasted for %s (L) with expected number of values = %d",
              rightAlias, leftAlias, rightEstimated
          );
          RowSignature signature = Queries.relaySchema(query1, segmentWalker);
          Sequence<BulkRow> values = BulkSequence.fromArray(
              query1.array(QueryRunners.run(query1, segmentWalker)), signature
          );
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              config.getJoin(), element, false, signature, prefixAlias, asArray, outputColumns, values
          );
          Map<String, Object> joinContext = BaseQuery.copyContextForMeta(context, Query.POST_PROCESSING, processor);
          if (leftEstimated > 0) {
            joinContext.put(CARDINALITY, leftEstimated);
          }

          queries.add(new BroadcastJoinHolder(query0, processor, joinContext));
          if (leftEstimated > 0) {
            currentEstimation = leftEstimated;
          }
          continue;
        }
      }

      // try hash-join
      boolean leftHashing = i == 0 && joinType.isRightDrivable() && isUnderThreshold(leftEstimated, hashThreshold);
      boolean rightHashing = joinType.isLeftDrivable() && isUnderThreshold(rightEstimated, hashThreshold);
      if (leftHashing && rightHashing) {
        if (leftEstimated > rightEstimated) {
          leftHashing = false;
        } else {
          rightHashing = false;
        }
      }
      if (i == 0) {
        LOG.info("%s (L) -----> %d rows, type? %s, hashing? %s", leftAlias, leftEstimated, joinType, leftHashing);
        List<String> sortOn = leftHashing || (joinType != LO && rightHashing) ? null : leftJoinColumns;
        Query query = JoinElement.toQuery(segmentWalker, left, sortOn, segmentSpec, context);
        if (leftHashing) {
          query = query.withOverriddenContext(HASHING, true);
        }
        if (leftEstimated >= 0) {
          query = query.withOverriddenContext(CARDINALITY, leftEstimated);
        }
        queries.add(query);
      }
      LOG.info("%s (R) -----> %d rows, type? %s, hashing? %s", rightAlias, rightEstimated, joinType, rightHashing);
      List<String> sortOn = rightHashing || (joinType != RO && leftHashing) ? null : rightJoinColumns;
      Query query = JoinElement.toQuery(segmentWalker, right, sortOn, segmentSpec, context);
      if (rightHashing) {
        query = query.withOverriddenContext(HASHING, true);
      }
      if (rightEstimated >= 0) {
        query = query.withOverriddenContext(CARDINALITY, rightEstimated);
      }
      queries.add(query);

      // try bloom filter
      if (i == 0) {
        final Query query0 = queries.get(0);
        final Query query1 = queries.get(1);
        if (joinType.isLeftDrivable() && query0.hasFilters() && rightEstimated > bloomFilterThreshold) {
          // left to right
          Query applied = bloom(query0, leftJoinColumns, leftEstimated, rightEstimated, query1, rightJoinColumns);
          if (applied != null) {
            rightEstimated = Math.max(1, rightEstimated >>> 2);
            queries.set(1, applied);
          }
        }
        if (joinType.isRightDrivable() && query1.hasFilters() && leftEstimated > bloomFilterThreshold) {
          // right to left
          Query applied = bloom(query1, rightJoinColumns, rightEstimated, leftEstimated, query0, leftJoinColumns);
          if (applied != null) {
            leftEstimated = Math.max(1, leftEstimated >>> 2);
            queries.set(0, applied);
          }
        }
      }
      if (leftEstimated >= 0 && rightEstimated >= 0) {
        currentEstimation = resultEstimation(joinType, leftEstimated, rightEstimated);
      }
    }
    // todo: properly handle filter converted semijoin
    if (queries.size() == 1) {
      return queries.get(0);
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

    Map<String, Object> joinContext = BaseQuery.copyContextForMeta(this);
    if (currentEstimation > 0) {
      joinContext.put(CARDINALITY, currentEstimation);
    }
    CommonJoinHolder query = new CommonJoinHolder(queries, prefixAliases, timeColumn, outputColumns, limit, joinContext);
    if (schema != null) {
      query = query.withSchema(Suppliers.ofInstance(schema));
    }
    return PostProcessingOperators.append(
        query,
        segmentWalker.getObjectMapper(),
        new JoinPostProcessor(config.getJoin(), elements, prefixAlias, asArray, outputColumns, maxOutputRow)
    );
  }

  private boolean isUnderThreshold(long estimation, long threshold)
  {
    return estimation >= 0 && threshold >= 0 && estimation < threshold;
  }

  private long resultEstimation(JoinType type, long leftEstimation, long rightEstimated)
  {
    switch (type) {
      case INNER:
        if (leftEstimation == 0 || rightEstimated == 0) {
          leftEstimation = 0;
        } else {
          leftEstimation = Math.min(leftEstimation, rightEstimated) + Math.abs(leftEstimation - rightEstimated) / 2;
        }
        break;
      case LO:
        if (leftEstimation == 0) {
          leftEstimation = 0;
        } else if (rightEstimated > leftEstimation) {
          leftEstimation *= ((double) rightEstimated) / leftEstimation;
        }
        break;
      case RO:
        if (rightEstimated == 0) {
          leftEstimation = 0;
        } else if (leftEstimation > rightEstimated) {
          leftEstimation *= (double) leftEstimation / rightEstimated;
        }
        break;
      case FULL:
        leftEstimation += rightEstimated;
        break;
    }
    return leftEstimation;
  }

  private Query bloom(
      Query source,
      List<String> sourceJoinOn,
      long sourceCardinality,
      long targetCardinality,
      Query target,
      List<String> targetJoinOn
  )
  {
    if (source instanceof StreamQuery && !Queries.isNestedQuery(source) &&
        target instanceof FilterSupport && !Queries.isNestedQuery(target)) {
      // this is evaluated in UnionQueryRunner
      List<DimensionSpec> extracted = Queries.extractInputFields(target, targetJoinOn);
      if (extracted != null) {
        ViewDataSource sourceView = BaseQuery.asView(source, sourceJoinOn);
        DimFilter factory = BloomDimFilter.Factory.fields(extracted, sourceView, Ints.checkedCast(sourceCardinality));
        LOG.info("Applying bloom filter from [%s] to [%s]", sourceView, DataSources.getName(target));
        return DimFilters.and((FilterSupport<?>) target, factory);
      }
    }
    return null;
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
        maxOutputRow,
        outputColumns,
        getContext()
    ).withSchema(schema);
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
        maxOutputRow,
        outputColumns,
        getContext()
    ).withSchema(schema);
  }

  public JoinQuery withOutputColumns(List<String> outputColumns)
  {
    return new JoinQuery(
        dataSources,
        getQuerySegmentSpec(),
        elements,
        prefixAlias,
        asArray,
        timeColumnName,
        limit,
        maxOutputRow,
        outputColumns,
        getContext()
    ).withSchema(schema);
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

    JoinQuery that = (JoinQuery) o;

    if (!Objects.equals(dataSources, that.dataSources)) {
      return false;
    }
    if (!Objects.equals(elements, that.elements)) {
      return false;
    }
    if (prefixAlias != that.prefixAlias) {
      return false;
    }
    if (asArray != that.asArray) {
      return false;
    }
    if (maxOutputRow != that.maxOutputRow) {
      return false;
    }
    if (limit != that.limit) {
      return false;
    }
    if (!Objects.equals(timeColumnName, that.timeColumnName)) {
      return false;
    }
    if (!Objects.equals(outputColumns, that.outputColumns)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "dataSources=" + dataSources +
           ", elements=" + elements +
           ", prefixAlias=" + prefixAlias +
           ", asArray=" + asArray +
           ", maxOutputRow=" + maxOutputRow +
           (outputColumns == null ? "" : ", outputColumns= " + outputColumns) +
           ", limit=" + limit +
           '}';
  }

  public static abstract class JoinHolder extends UnionAllQuery<Map<String, Object>>
      implements ArrayOutputSupport<Map<String, Object>>, SchemaProvider
  {
    public JoinHolder(
        Query<Map<String, Object>> query,
        List<Query<Map<String, Object>>> queries,
        boolean sortOnUnion,
        int limit,
        int parallelism,
        Map<String, Object> context
    )
    {
      super(query, queries, sortOnUnion, limit, parallelism, context);
    }

    @Override
    public String getType()
    {
      return Query.JOIN;
    }

    @Override
    public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
    {
      return this;
    }

    protected CommonJoinProcessor getLastJoinProcessor()
    {
      PostProcessingOperator processor = getContextValue(Query.POST_PROCESSING);
      if (processor instanceof ListPostProcessingOperator) {
        for (PostProcessingOperator element : ((ListPostProcessingOperator<?>) processor).getProcessorsInReverse()) {
          if (element instanceof CommonJoinProcessor) {
            return (CommonJoinProcessor) element;
          }
        }
      }
      return processor instanceof CommonJoinProcessor ? (CommonJoinProcessor) processor : null;
    }

    protected boolean isArrayOutput()
    {
      return Preconditions.checkNotNull(getLastJoinProcessor(), "no join processor").isAsArray();
    }

    private static final IdentityFunction<PostProcessingOperator> AS_ARRAY = new IdentityFunction<PostProcessingOperator>()
    {
      @Override
      public PostProcessingOperator apply(PostProcessingOperator input)
      {
        if (input instanceof CommonJoinProcessor) {
          input = ((CommonJoinProcessor) input).withAsArray(true);
        }
        return input;
      }
    };

    public JoinHolder toArrayJoin()
    {
      PostProcessingOperator processor = getContextValue(Query.POST_PROCESSING);
      if (processor != null) {
        return (JoinHolder) withOverriddenContext(
            Query.POST_PROCESSING, PostProcessingOperators.rewrite(processor, AS_ARRAY)
        );
      }
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
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
  }

  @SuppressWarnings("unchecked")
  public static class BroadcastJoinHolder extends JoinHolder
  {
    private final BroadcastJoinProcessor processor;

    public BroadcastJoinHolder(
        Query query,
        BroadcastJoinProcessor processor,
        Map<String, Object> context
    )
    {
      super(query, null, false, -1, -1, context);
      this.processor = processor;
    }

    @Override
    protected UnionAllQuery newInstance(
        Query<Map<String, Object>> query,
        List<Query<Map<String, Object>>> queries,
        int parallism,
        Map<String, Object> context
    )
    {
      Preconditions.checkArgument(queries == null);
      return new BroadcastJoinHolder(
          query,
          processor,
          context
      ).withSchema(schema);
    }

    @Override
    public List<String> estimatedOutputColumns()
    {
      return processor.estimatedOutputColumns(((Query.ArrayOutputSupport) getQuery()).estimatedOutputColumns());
    }

    @Override
    protected boolean isArrayOutput()
    {
      return processor.isAsArray();
    }

    @Override
    public JoinHolder toArrayJoin()
    {
      if (processor.isAsArray()) {
        return this;
      }
      final BroadcastJoinProcessor asArray = processor.withAsArray(true);
      return new BroadcastJoinHolder(
          getQuery(), asArray, BaseQuery.copyContext(this, Query.POST_PROCESSING, processor)
      );
    }

    @Override
    public RowSignature schema(QuerySegmentWalker segmentWalker)
    {
      return processor.estimatedSchema(getQuery(), segmentWalker);
    }

    @Override
    public String toString()
    {
      return "BroadcastJoin{query=" + getQuery() + '}';
    }
  }

  @SuppressWarnings("unchecked")
  public static class CommonJoinHolder extends JoinHolder
  {
    private final List<String> prefixAliases;  // for schema resolving
    private final List<String> outputColumns;

    private final String timeColumnName;

    private final Execs.SettableFuture<List<OrderByColumnSpec>> future = new Execs.SettableFuture();

    public CommonJoinHolder(
        List<Query<Map<String, Object>>> list,
        List<String> prefixAliases,
        String timeColumnName,
        List<String> outputColumns,
        int limit,
        Map<String, Object> context
    )
    {
      super(null, list, false, limit, -1, context);     // not needed to be parallel (handled in post processor)
      this.prefixAliases = prefixAliases;
      this.outputColumns = outputColumns;
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

    public List<OrderByColumnSpec> getCollation()
    {
      return future.isDone() ? Futures.getUnchecked(future) : null;
    }

    public void setCollation(List<OrderByColumnSpec> collation)
    {
      future.set(collation);
    }

    public void setException(Throwable ex)
    {
      future.setException(ex);
    }

    @Override
    protected CommonJoinHolder newInstance(
        Query<Map<String, Object>> query,
        List<Query<Map<String, Object>>> queries,
        int parallism,
        Map<String, Object> context
    )
    {
      Preconditions.checkArgument(query == null);
      return new CommonJoinHolder(
          queries,
          prefixAliases,
          timeColumnName,
          outputColumns,
          getLimit(),
          context
      ).withSchema(schema);
    }

    @Override
    public CommonJoinHolder withSchema(Supplier<RowSignature> schema)
    {
      this.schema = schema;
      return this;
    }

    @Override
    public List<String> estimatedOutputColumns()
    {
      if (outputColumns != null) {
        return outputColumns;
      }
      RowSignature schema = getSchema();
      if (schema != null) {
        return schema.getColumnNames();   // resolved already
      }
      Set<String> uniqueNames = Sets.newHashSet();
      List<String> columnNames = Lists.newArrayList();

      List<Query<Map<String, Object>>> queries = getQueries();
      for (int i = 0; i < queries.size(); i++) {
        List<String> columns = ((ArrayOutputSupport<?>) queries.get(i)).estimatedOutputColumns();
        Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(columns));
        if (prefixAliases == null) {
          Queries.uniqueNames(columns, uniqueNames, columnNames);
        } else {
          String alias = prefixAliases.get(i) + ".";
          for (String column : columns) {
            columnNames.add(Queries.uniqueName(alias + column, uniqueNames));
          }
        }
      }
      return columnNames;
    }

    @Override
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

    @Override
    public RowSignature schema(QuerySegmentWalker segmentWalker)
    {
      if (schema == null) {
        List<String> columnNames = Lists.newArrayList();
        List<ValueDesc> columnTypes = Lists.newArrayList();

        List queries = getQueries();
        List<String> aliases = getPrefixAliases();
        Set<String> uniqueNames = Sets.newHashSet();
        for (int i = 0; i < queries.size(); i++) {
          final RowSignature element = Queries.relaySchema((Query) queries.get(i), segmentWalker);
          final String prefix = aliases == null ? "" : aliases.get(i) + ".";
          for (Pair<String, ValueDesc> pair : element.columnAndTypes()) {
            columnNames.add(Queries.uniqueName(prefix + pair.lhs, uniqueNames));
            columnTypes.add(pair.rhs);
          }
        }
        schema = Suppliers.ofInstance(new RowSignature.Simple(columnNames, columnTypes));
      }
      return schema.get();
    }

    @Override
    public String toString()
    {
      return "CommonJoin{" +
             "queries=" + getQueries() +
             (prefixAliases == null ? "" : ", prefixAliases=" + getPrefixAliases()) +
             (timeColumnName == null ? "" : ", timeColumnName=" + getTimeColumnName()) +
             (getLimit() > 0 ? ", limit=" + getLimit() : "") +
             '}';
    }
  }
}
