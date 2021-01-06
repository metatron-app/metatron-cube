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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.data.ValueDesc;
import io.druid.data.input.BulkSequence;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.bloomfilter.BloomFilterAggregator;
import io.druid.query.aggregation.bloomfilter.BloomKFilter;
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

import java.util.Arrays;
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
  private final List<String> outputAlias;
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
      @JsonProperty("outputAlias") List<String> outputAlias,
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
    this.outputAlias = outputAlias;
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
  public List<String> getOutputAlias()
  {
    return outputAlias;
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
        outputAlias,
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
    final int maxResult = config.getJoin().getMaxOutputRow(maxOutputRow);
    final int hashThreshold = config.getHashJoinThreshold(this);
    final int semiJoinThrehold = config.getSemiJoinThreshold(this);
    final int broadcastThreshold = config.getBroadcastJoinThreshold(this);
    final int bloomFilterThreshold = config.getBloomFilterThreshold(this);

    long currentEstimation = ROWNUM_UNKNOWN;

    final Map<String, Object> context = getContext();
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    final List<Query<Map<String, Object>>> queries = Lists.newArrayList();

    final ObjectMapper mapper = segmentWalker.getObjectMapper();
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

      element.earlyCheckMaxJoin(leftEstimated, rightEstimated, maxResult);

      // try convert semi-join to filter
      if (semiJoinThrehold > 0 && joinType == JoinType.INNER && outputColumns != null) {
        if (i == 0 && isUnderThreshold(rightEstimated, semiJoinThrehold) && element.isLeftSemiJoinable(left, right, outputColumns)) {
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          List<String> rightColumns = array.estimatedOutputColumns();
          if (rightColumns != null && rightColumns.containsAll(rightJoinColumns)) {
            int[] indices = GuavaUtils.indexOf(rightColumns, rightJoinColumns);
            Supplier<List<Object[]>> fieldValues =
                () -> Sequences.toList(Sequences.map(
                    QueryRunners.runArray(array, segmentWalker), GuavaUtils.mapper(indices)));
            DataSource filtered = DataSources.applyFilterAndProjection(
                left, ValuesFilter.fieldNames(leftJoinColumns, fieldValues), outputColumns
            );
            LOG.info("-- %s:%d (R) is merged into %s (L) as a filter", rightAlias, rightEstimated, leftAlias);
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
                    QueryRunners.runArray(array, segmentWalker), GuavaUtils.mapper(indices)));
            DataSource filtered = DataSources.applyFilterAndProjection(
                right, ValuesFilter.fieldNames(rightJoinColumns, fieldValues), outputColumns
            );
            LOG.info("-- %s:%d (L) is merged into %s (R) as a filter", leftAlias, leftEstimated, rightAlias);
            queries.add(JoinElement.toQuery(segmentWalker, filtered, segmentSpec, context));
            if (rightEstimated >= 0) {
              currentEstimation = resultEstimation(joinType, leftEstimated, rightEstimated);
            }
            continue;
          }
        }
      }

      if (i == 0 && broadcastThreshold > 0) {
        boolean leftBroadcast =
            isUnderThreshold(leftEstimated, broadcastThreshold) && element.isLeftBroadcastable(left, right);
        boolean rightBroadcast =
            isUnderThreshold(rightEstimated, broadcastThreshold) && element.isRightBroadcastable(left, right);
        if (leftBroadcast && rightBroadcast) {
          if (leftEstimated > rightEstimated) {
            leftBroadcast = false;
          } else {
            rightBroadcast = false;
          }
        }
        if (leftBroadcast) {
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info("-- %s:%d (L) will be broadcasted to %s (R)", leftAlias, leftEstimated, rightAlias);
          RowSignature signature = Queries.relaySchema(query0, segmentWalker);
          List<Object[]> values = Sequences.toList(QueryRunners.runArray(query0, segmentWalker));
          LOG.info("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
          byte[] bytes = writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          if (query0.hasFilters() && query1 instanceof FilterSupport && !element.isCrossJoin() && leftEstimated > 0) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(
                resolver, element.getLeftJoinColumns(), leftEstimated, values.iterator()
            );
            query1 = DimFilters.and((FilterSupport<?>) query1, BloomDimFilter.of(element.getRightJoinColumns(), bloom));
            LOG.info(".. apply bloom filter %s(%s) to %s (R)", leftAlias, BaseQuery.getDimFilter(query0), rightAlias);
            rightEstimated = rightEstimated > 0 && rightEstimated > broadcastThreshold ? rightEstimated >> 1 : rightEstimated;
          }
          currentEstimation = rightEstimated;
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, true, signature, prefixAlias, asArray, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query1 = query1.withOverriddenContext(PostProcessingOperators.appendLocal(
              BaseQuery.copyContext(query1, CARDINALITY, rightEstimated), processor
          ));
          query1 = ((LastProjectionSupport) query1).withOutputColumns(null);
          queries.add(query1);
          continue;
        }
        if (rightBroadcast) {
          Query query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info("-- %s:%d (R) will be broadcasted to %s (L)", rightAlias, rightEstimated, leftAlias);
          RowSignature signature = Queries.relaySchema(query1, segmentWalker);
          List<Object[]> values = Sequences.toList(QueryRunners.runArray(query1, segmentWalker));
          LOG.info("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          byte[] bytes = writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          if (query1.hasFilters() && query0 instanceof FilterSupport && !element.isCrossJoin() && rightEstimated > 0) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(
                resolver, element.getRightJoinColumns(), rightEstimated, values.iterator()
            );
            query0 = DimFilters.and((FilterSupport<?>) query0, BloomDimFilter.of(element.getLeftJoinColumns(), bloom));
            LOG.info("-- .. with bloom filter %s(%s) to %s (L)", rightAlias, BaseQuery.getDimFilter(query1), leftAlias);
            leftEstimated = leftEstimated > 0 && leftEstimated > broadcastThreshold ? leftEstimated >> 1 : leftEstimated;
          }
          currentEstimation = leftEstimated;
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, false, signature, prefixAlias, asArray, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query0 = query0.withOverriddenContext(PostProcessingOperators.appendLocal(
              BaseQuery.copyContext(query0, CARDINALITY, leftEstimated), processor
          ));
          query0 = ((LastProjectionSupport) query0).withOutputColumns(null);
          queries.add(query0);
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
        List<String> sortOn = leftHashing || (joinType != LO && rightHashing) ? null : leftJoinColumns;
        LOG.info(
            "-- %s:%d (L) (%s)", leftAlias, leftEstimated, leftHashing ? "hash" : sortOn != null ? "sort" : "-"
        );
        Query query = JoinElement.toQuery(segmentWalker, left, sortOn, segmentSpec, context);
        if (leftHashing) {
          query = query.withOverriddenContext(HASHING, true);
        }
        query = query.withOverriddenContext(CARDINALITY, leftEstimated);
        queries.add(query);
      }
      List<String> sortOn = rightHashing || (joinType != RO && leftHashing) ? null : rightJoinColumns;
      LOG.info(
          "-- %s:%d (R) (%s)", rightAlias, rightEstimated, rightHashing ? "hash" : sortOn != null ? "sort" : "-"
      );
      Query query = JoinElement.toQuery(segmentWalker, right, sortOn, segmentSpec, context);
      if (rightHashing) {
        query = query.withOverriddenContext(HASHING, true);
      }
      query = query.withOverriddenContext(CARDINALITY, rightEstimated);
      queries.add(query);

      // try bloom filter
      if (i == 0 && !element.isCrossJoin()) {
        final Query query0 = queries.get(0);
        final Query query1 = queries.get(1);
        if (joinType.isLeftDrivable() && query0.hasFilters() && query0.getContextValue(Query.LOCAL_POST_PROCESSING) == null && rightEstimated > bloomFilterThreshold) {
          // left to right
          Query applied = bloom(query0, leftJoinColumns, leftEstimated, rightEstimated, query1, rightJoinColumns);
          if (applied != null) {
            rightEstimated = Math.max(1, rightEstimated >>> 2);
            queries.set(1, applied);
          }
        }
        if (joinType.isRightDrivable() && query1.hasFilters() && query1.getContextValue(Query.LOCAL_POST_PROCESSING) == null && leftEstimated > bloomFilterThreshold) {
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

    List<String> aliases = JoinElement.getAliases(elements);

    String timeColumn;
    if (prefixAlias) {
      timeColumn = timeColumnName == null ? aliases.get(0) + "." + Column.TIME_COLUMN_NAME : timeColumnName;
    } else {
      timeColumn = timeColumnName == null ? Column.TIME_COLUMN_NAME : timeColumnName;
    }

    Map<String, Object> joinContext = BaseQuery.copyContextForMeta(context, CARDINALITY, currentEstimation);
    JoinHolder query = new JoinHolder(
        StringUtils.concat("+", aliases), queries, timeColumn, outputAlias, outputColumns, limit, joinContext
    );
    Supplier<RowSignature> signature = schema == null ? null : Suppliers.ofInstance(schema);
    if (signature == null) {
      signature = Suppliers.memoize(() -> {
        List<String> columnNames = Lists.newArrayList();
        List<ValueDesc> columnTypes = Lists.newArrayList();
        Set<String> uniqueNames = Sets.newHashSet();
        for (int i = 0; i < queries.size(); i++) {
          final RowSignature element = Queries.relaySchema(queries.get(i), segmentWalker);
          final String prefix = aliases == null ? "" : aliases.get(i) + ".";
          for (Pair<String, ValueDesc> pair : element.columnAndTypes()) {
            columnNames.add(Queries.uniqueName(prefix + pair.lhs, uniqueNames));
            columnTypes.add(pair.rhs);
          }
        }
        return RowSignature.of(columnNames, columnTypes);
      });
    }
    return PostProcessingOperators.append(
        query.withSchema(signature),
        new JoinPostProcessor(config.getJoin(), elements, prefixAlias, asArray, outputAlias, outputColumns, maxOutputRow)
    );
  }

  private static byte[] writeBytes(ObjectMapper objectMapper, Object value)
  {
    try {
      return objectMapper.writeValueAsBytes(value);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
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
        LOG.info("-- applying bloom filter from [%s] to [%s]", sourceView, DataSources.getName(target));
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
        outputAlias,
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
        outputAlias,
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
        outputAlias,
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

  public static class JoinHolder extends UnionAllQuery<Map<String, Object>>
      implements ArrayOutputSupport<Map<String, Object>>, SchemaProvider
  {
    private final String alias;   // just for debug
    private final List<String> outputAlias;
    private final List<String> outputColumns;

    private final String timeColumnName;

    private final Execs.SettableFuture<List<List<OrderByColumnSpec>>> future = new Execs.SettableFuture<>();

    public JoinHolder(
        String alias,
        List<Query<Map<String, Object>>> queries,
        String timeColumnName,
        List<String> outputAlias,
        List<String> outputColumns,
        int limit,
        Map<String, Object> context
    )
    {
      super(null, queries, false, limit, -1, context);     // not needed to be parallel (handled in post processor)
      this.alias = alias;
      this.outputAlias = outputAlias;
      this.outputColumns = outputColumns;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
    }

    public String getAlias()
    {
      return alias;
    }

    public String getTimeColumnName()
    {
      return timeColumnName;
    }

    public List<String> getOutputAlias()
    {
      return outputAlias;
    }

    public List<List<OrderByColumnSpec>> getCollations()
    {
      return future.isDone() ? Futures.getUnchecked(future) : ImmutableList.of();
    }

    public void setCollation(List<List<OrderByColumnSpec>> collations)
    {
      future.set(collations);
    }

    public void setException(Throwable ex)
    {
      future.setException(ex);
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
          CommonJoinProcessor join = (CommonJoinProcessor) input;
          if (!join.isAsArray()) {
            input = join.withAsArray(true);
          }
        }
        return input;
      }
    };

    public JoinHolder toArrayJoin(List<String> sortColumns)
    {
      JoinHolder holder = this;
      PostProcessingOperator processor = PostProcessingOperators.rewrite(
          getContextValue(Query.POST_PROCESSING), AS_ARRAY
      );
      holder = (JoinHolder) holder.withOverriddenContext(Query.POST_PROCESSING, processor);
      if (getQueries().size() == 2 && !GuavaUtils.isNullOrEmpty(sortColumns)) {
        // best effort
        JoinPostProcessor proc = PostProcessingOperators.find(processor, JoinPostProcessor.class);
        JoinElement element = proc.getElements()[0];
        Query<Map<String, Object>> query0 = getQueries().get(0);
        Query<Map<String, Object>> query1 = getQueries().get(1);
        boolean hashing0 = query0.getContextBoolean(HASHING, false);
        boolean hashing1 = query1.getContextBoolean(HASHING, false);
        if (hashing0 && hashing1) {
          return holder;    // todo use tree map?
        }
        final JoinType type = element.getJoinType();
        if (type.isLeftDrivable()) {
          if (hashing1 && query0 instanceof OrderingSupport) {
            OrderingSupport reordered = tryReordering((OrderingSupport) query0, sortColumns);
            if (reordered != null) {
              LOG.info("reordered.. %s : %s", query0.getDataSource().getNames(), reordered.getResultOrdering());
              return (JoinHolder) holder.withQueries(Arrays.asList(reordered, query1));
            }
          }
        }
        if (type.isRightDrivable()) {
          if (hashing0 && query1 instanceof OrderingSupport) {
            OrderingSupport reordered = tryReordering((OrderingSupport) query1, sortColumns);
            if (reordered != null) {
              LOG.info("reordered.. %s : %s", query1.getDataSource().getNames(), reordered.getResultOrdering());
              return (JoinHolder) holder.withQueries(Arrays.asList(query0, reordered));
            }
          }
        }
      }
      return holder;
    }

    private static OrderingSupport tryReordering(OrderingSupport<?> query, List<String> expected)
    {
      List<String> outputColumns = query.estimatedOutputColumns();
      if (outputColumns == null || !outputColumns.containsAll(expected)) {
        return null;
      }
      List<OrderByColumnSpec> current = query.getResultOrdering();
      if (GuavaUtils.isNullOrEmpty(current)) {
        return query.withResultOrdering(OrderByColumnSpec.ascending(expected));
      }
      // append ordering
      List<String> columns = OrderByColumnSpec.getColumns(current);
      if (expected.size() > columns.size() && GuavaUtils.startsWith(expected, columns)) {
        return query.withResultOrdering(GuavaUtils.concat(
            current, OrderByColumnSpec.ascending(expected.subList(current.size(), columns.size()))
        ));
      }
      return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Sequence<Object[]> array(Sequence sequence)
    {
      if (isArrayOutput()) {
        return sequence;
      }
      final List<String> columns = sequence.columns();
      return Sequences.map(
          sequence, new Function<Map<String, Object>, Object[]>()
          {
            private final String[] columnNames = columns.toArray(new String[0]);

            @Override
            public Object[] apply(Map<String, Object> input)
            {
              final Object[] array = new Object[columnNames.length];
              for (int i = 0; i < columnNames.length; i++) {
                array[i] = input.get(columnNames[i]);
              }
              return array;
            }
          }
      );
    }

    @Override
    protected JoinHolder newInstance(
        Query<Map<String, Object>> query,
        List<Query<Map<String, Object>>> queries,
        int parallism,
        Map<String, Object> context
    )
    {
      Preconditions.checkArgument(query == null);
      return new JoinHolder(
          alias,
          queries,
          timeColumnName,
          outputAlias,
          outputColumns,
          getLimit(),
          context
      ).withSchema(schema);
    }

    @Override
    public JoinHolder withSchema(Supplier<RowSignature> schema)
    {
      this.schema = schema;
      return this;
    }

    @Override
    public List<String> estimatedOutputColumns()
    {
      return outputColumns != null ? outputColumns : outputAlias;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Sequence<Row> asRow(Sequence sequence)
    {
      if (isArrayOutput()) {
        final List<String> columns = sequence.columns();
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
      return schema.get();
    }

    @Override
    public String toString()
    {
      return "CommonJoin{" +
             "queries=" + getQueries() +
             (timeColumnName == null ? "" : ", timeColumnName=" + timeColumnName) +
             (getLimit() > 0 ? ", limit=" + getLimit() : "") +
             '}';
    }
  }
}
