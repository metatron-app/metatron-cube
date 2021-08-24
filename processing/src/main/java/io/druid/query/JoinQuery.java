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
import com.fasterxml.jackson.annotation.JsonIgnore;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.BloomDimFilter.Factory;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.SemiJoinFactory;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.druid.query.JoinType.LO;
import static io.druid.query.JoinType.RO;

/**
 */
@JsonTypeName("join")
public class JoinQuery extends BaseQuery<Object[]> implements Query.RewritingQuery<Object[]>
{
  public static final String HASHING = "$hash";
  public static final String CARDINALITY = "$cardinality";
  public static final String ROWNUM = "$rownum";

  private static final Logger LOG = new Logger(JoinQuery.class);

  private final Map<String, DataSource> dataSources;
  private final List<JoinElement> elements;
  private final String timeColumnName;
  private final boolean prefixAlias;
  private final boolean asMap;
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
      @JsonProperty("asMap") boolean asMap,
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
    this.asMap = asMap;
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
  public boolean isAsMap()
  {
    return asMap;
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

  @JsonIgnore
  public RowSignature getSchema()
  {
    return schema;
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
        asMap,
        timeColumnName,
        limit,
        maxOutputRow,
        outputAlias,
        outputColumns,
        computeOverriddenContext(contextOverride)
    ).withSchema(schema);
  }

  @Override
  public Query<Object[]> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<Object[]> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  // use it if already set
  public static long[] estimatedCardinality(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      long cardinality = query.getContextLong(CARDINALITY, Queries.NOT_EVALUATED);
      if (cardinality != Queries.NOT_EVALUATED) {
        return new long[]{cardinality, query.getContextLong(ROWNUM, Queries.NOT_EVALUATED)};
      }
      long[] estimate = estimatedCardinality(query.getDataSource());
      if (estimate[0] > 0 && query instanceof Query.AggregationsSupport) {
        estimate[0] = (long) Math.max(1, estimate[0] * (1 - Math.pow(0.4, BaseQuery.getDimensions(query).size())));
      }
      return estimate;
    }
    return new long[]{Queries.NOT_EVALUATED, Queries.NOT_EVALUATED};
  }

  private static final boolean LEFT_TO_RIGHT = true;
  private static final boolean RIGHT_TO_LEFT = false;

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
    final int forcedFilterHugeThreshold = config.getForcedFilterHugeThreshold(this);
    final int forcedFilterTinyThreshold = config.getForcedFilterTinyThreshold(this);

    final long[] currentEstimation = new long[]{Queries.UNKNOWN, Queries.UNKNOWN};

    final Map<String, Object> context = getContext();
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    final List<Query<Object[]>> queries = Lists.newArrayList();

    final ObjectMapper mapper = segmentWalker.getMapper();
    for (int i = 0; i < elements.size(); i++) {
      final JoinElement element = elements.get(i);
      final JoinType joinType = element.getJoinType();
      final List<String> leftJoinColumns = element.getLeftJoinColumns();
      final List<String> rightJoinColumns = element.getRightJoinColumns();

      final String leftAlias = element.getLeftAlias();
      final String rightAlias = element.getRightAlias();

      DataSource left = i == 0 ? dataSources.get(leftAlias) : null;
      DataSource right = dataSources.get(rightAlias);

      long[] leftEstimated = i == 0 ? estimatedCardinality(left) : currentEstimation;
      if (leftEstimated[0] == Queries.NOT_EVALUATED) {
        leftEstimated = JoinElement.estimatedNumRows(left, segmentSpec, context, segmentWalker, config);
      }
      long[] rightEstimated = estimatedCardinality(right);
      if (rightEstimated[0] == Queries.NOT_EVALUATED) {
        rightEstimated = JoinElement.estimatedNumRows(right, segmentSpec, context, segmentWalker, config);
      }
      LOG.info(">> %s (%s:%d + %s:%d)", element.getJoinTypeString(), leftAlias, leftEstimated[0], rightAlias, rightEstimated[0]);

      if (!getContextBoolean(Query.OUTERMOST_JOIN, false)) {
        element.earlyCheckMaxJoin(leftEstimated[0], rightEstimated[0], maxResult);
      }

      // try convert semi-join to filter
      if (semiJoinThrehold > 0 && element.isInnerJoin() && outputColumns != null) {
        if (i == 0 && isUnderThreshold(rightEstimated[0], semiJoinThrehold) && element.isLeftSemiJoinable(left, right, outputColumns)) {
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          List<String> rightColumns = array.estimatedOutputColumns();
          if (rightColumns != null && rightColumns.containsAll(rightJoinColumns)) {
            Sequence<Object[]> sequence = Sequences.map(
                QueryRunners.runArray(array, segmentWalker), GuavaUtils.mapper(rightColumns, rightJoinColumns)
            );
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                leftJoinColumns, sequence, JoinElement.allowDuplication(left, leftJoinColumns)
            );
            DataSource filtered = DataSources.applyFilterAndProjection(left, converted.lhs, outputColumns, converted.rhs == null);
            LOG.info("-- %s:%d (R) is merged into %s (L) as filter on %s", rightAlias, rightEstimated[0], leftAlias, leftJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, context);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, RIGHT_TO_LEFT);
            if (currentEstimation[0] == Queries.UNKNOWN) {
              currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]) >> 2;
            }
            queries.add(propagateCardinality(query, currentEstimation));
            continue;
          }
        }
        if (isUnderThreshold(leftEstimated[0], semiJoinThrehold) && element.isRightSemiJoinable(left, right, outputColumns)) {
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          List<String> leftColumns = array.estimatedOutputColumns();
          if (leftColumns != null && leftColumns.containsAll(leftJoinColumns)) {
            Sequence<Object[]> sequence = Sequences.map(
                QueryRunners.runArray(array, segmentWalker), GuavaUtils.mapper(leftColumns, leftJoinColumns)
            );
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                rightJoinColumns, sequence, JoinElement.allowDuplication(right, rightJoinColumns)
            );
            DataSource filtered = DataSources.applyFilterAndProjection(right, converted.lhs, outputColumns, converted.rhs == null);
            LOG.info("-- %s:%d (L) is merged into %s (R) as filter on %s", leftAlias, leftEstimated[0], rightAlias, rightJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, context);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, LEFT_TO_RIGHT);
            if (currentEstimation[0] == Queries.UNKNOWN) {
              currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]) >> 2;
            }
            queries.add(propagateCardinality(query, currentEstimation));
            continue;
          }
        }
      }

      if (i == 0 && broadcastThreshold > 0 &&
          DataSources.isDataNodeSourced(left) &&
          DataSources.isDataNodeSourced(right)) {
        boolean leftBroadcast = joinType.isRightDrivable() && isUnderThreshold(leftEstimated[0], broadcastThreshold);
        boolean rightBroadcast = joinType.isLeftDrivable() && isUnderThreshold(rightEstimated[0], broadcastThreshold);
        if (leftBroadcast && rightBroadcast) {
          if (leftEstimated[0] > rightEstimated[0]) {
            leftBroadcast = false;
          } else {
            rightBroadcast = false;
          }
        }
        if (leftBroadcast) {
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info("-- %s:%d (L) will be broadcasted to %s (R)", leftAlias, leftEstimated[0], rightAlias);
          RowSignature signature = Queries.relaySchema(query0, segmentWalker);
          List<Object[]> values = Sequences.toList(QueryRunners.runArray(query0, segmentWalker));
          LOG.info("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
          byte[] bytes = writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature, -1));
          if (values.size() < forcedFilterTinyThreshold << 1 && rightEstimated[0] >= forcedFilterHugeThreshold &&
              element.forceLeftToFilter(query0, query1) != null) {
            Iterator<Object[]> iterator = Iterators.transform(
                values.iterator(), GuavaUtils.mapper(signature.columnNames, leftJoinColumns)
            );
            DimFilter filter = SemiJoinFactory.from(rightJoinColumns, iterator);
            query1 = DimFilters.and((FilterSupport) query1, filter);
            LOG.info("-- .. with forced filter %s to %s (R)", filter, rightAlias);
          } else if (query0.hasFilters() && DataSources.isFilterableOn(query1, rightJoinColumns) && !element.isCrossJoin()) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(resolver, leftJoinColumns, values.size(), values);
            query1 = DataSources.applyFilter(query1, BloomDimFilter.of(rightJoinColumns, bloom));
            LOG.info("-- .. with bloom filter %s(%s) to %s (R)", leftAlias, BaseQuery.getDimFilter(query0), rightAlias);
          }
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, true, signature, prefixAlias,
              asMap, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query1 = PostProcessingOperators.appendLocal(query1, processor);
          query1 = ((LastProjectionSupport) query1).withOutputColumns(null);
          if (element.isInnerJoin()) {
            currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, LEFT_TO_RIGHT);
            if (currentEstimation[0] == Queries.UNKNOWN) {
              currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]);
            }
          }
          queries.add(propagateCardinality(query1, currentEstimation));
          continue;
        }
        if (rightBroadcast) {
          Query query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context);
          LOG.info("-- %s:%d (R) will be broadcasted to %s (L)", rightAlias, rightEstimated[0], leftAlias);
          RowSignature signature = Queries.relaySchema(query1, segmentWalker);
          List<Object[]> values = Sequences.toList(QueryRunners.runArray(query1, segmentWalker));
          LOG.info("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          byte[] bytes = writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature, -1));
          if (values.size() < forcedFilterTinyThreshold << 1 && leftEstimated[0] >= forcedFilterHugeThreshold &&
              element.forceRightToFilter(query0, query1) != null) {
            Iterator<Object[]> iterator = Iterators.transform(
                values.iterator(), GuavaUtils.mapper(signature.columnNames, rightJoinColumns)
            );
            DimFilter filter = SemiJoinFactory.from(leftJoinColumns, iterator);
            query0 = DimFilters.and((FilterSupport) query0, filter);
            LOG.info("-- .. with forced filter %s to %s (L)", filter, leftAlias);
          } else if (query1.hasFilters() && DataSources.isFilterableOn(query0, leftJoinColumns) && !element.isCrossJoin()) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(resolver, rightJoinColumns, values.size(), values);
            query0 = DataSources.applyFilter(query0, BloomDimFilter.of(leftJoinColumns, bloom));
            LOG.info("-- .. with bloom filter %s(%s) to %s (L)", rightAlias, BaseQuery.getDimFilter(query1), leftAlias);
          }
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, false, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query0 = PostProcessingOperators.appendLocal(query0, processor);
          query0 = ((LastProjectionSupport) query0).withOutputColumns(null);
          if (element.isInnerJoin()) {
            currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, RIGHT_TO_LEFT);
            if (currentEstimation[0] == Queries.UNKNOWN) {
              currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]);
            }
          }
          queries.add(propagateCardinality(query0, currentEstimation));
          continue;
        }
      }

      // try hash-join
      boolean leftHashing = i == 0 && joinType.isRightDrivable() && isUnderThreshold(leftEstimated[0], hashThreshold);
      boolean rightHashing = joinType.isLeftDrivable() && isUnderThreshold(rightEstimated[0], hashThreshold);
      if (leftHashing && rightHashing) {
        if (leftEstimated[0] > rightEstimated[0]) {
          leftHashing = false;
        } else {
          rightHashing = false;
        }
      }
      if (i == 0) {
        List<String> sortOn = leftHashing || (joinType != LO && rightHashing) ? null : leftJoinColumns;
        LOG.info(
            "-- %s:%d (L) (%s)", leftAlias, leftEstimated[0], leftHashing ? "hash" : sortOn != null ? "sort" : "-"
        );
        Query query = JoinElement.toQuery(segmentWalker, left, sortOn, segmentSpec, context);
        if (leftHashing) {
          query = query.withOverriddenContext(HASHING, true);
        }

        if (leftHashing && element.isInnerJoin()
            && isUnderThreshold(leftEstimated[0], semiJoinThrehold)
            && DataSources.isDataNodeSourced(query)
            && DataSources.isFilterableOn(right, rightJoinColumns)) {
          ArrayOutputSupport<?> array = (ArrayOutputSupport) query;
          List<String> outputColumns = array.estimatedOutputColumns();
          if (outputColumns != null) {
            List<Object[]> values = Sequences.toList(QueryRunners.runArray(array, segmentWalker));
            query = MaterializedQuery.of(left, Sequences.simple(outputColumns, values), query.getContext());
            Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, leftJoinColumns));
            right = DataSources.applyFilter(right, SemiJoinFactory.from(rightJoinColumns, keys.iterator()));
            LOG.info("-- %s:%d (L) (hash) is applied to %s (R)", leftAlias, leftEstimated[0] = values.size(), rightAlias);
          }
        }
        queries.add(propagateCardinality(query, leftEstimated));
      }
      List<String> sortOn = rightHashing || (joinType != RO && leftHashing) ? null : rightJoinColumns;
      LOG.info(
          "-- %s:%d (R) (%s)", rightAlias, rightEstimated[0], rightHashing ? "hash" : sortOn != null ? "sort" : "-"
      );
      Query query = JoinElement.toQuery(segmentWalker, right, sortOn, segmentSpec, context);
      if (rightHashing) {
        query = query.withOverriddenContext(HASHING, true);
      }

      left = QueryDataSource.of(GuavaUtils.lastOf(queries));
      if (rightHashing && element.isInnerJoin()
          && isUnderThreshold(rightEstimated[0], semiJoinThrehold)
          && DataSources.isDataNodeSourced(query)
          && DataSources.isFilterableOn(left, leftJoinColumns)) {
        ArrayOutputSupport<?> array = (ArrayOutputSupport) query;
        List<String> outputColumns = array.estimatedOutputColumns();
        if (outputColumns != null) {
          List<Object[]> values = Sequences.toList(QueryRunners.runArray(array, segmentWalker));
          query = MaterializedQuery.of(right, Sequences.simple(outputColumns, values), query.getContext());
          Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, rightJoinColumns));
          left = DataSources.applyFilter(left, SemiJoinFactory.from(leftJoinColumns, keys.iterator()));
          LOG.info("-- %s:%d (R) (hash) is applied to %s (L) as filter", rightAlias, rightEstimated[0] = values.size(), leftAlias);
          GuavaUtils.setLastOf(queries, ((QueryDataSource) left).getQuery());   // overwrite
        }
      }
      queries.add(propagateCardinality(query, rightEstimated));

      if (queries.get(i) instanceof MaterializedQuery) {
        currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, LEFT_TO_RIGHT);
        if (currentEstimation[0] == Queries.UNKNOWN) {
          currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]);
        }
        continue;
      } else if (queries.get(i + 1) instanceof MaterializedQuery) {
        currentEstimation[0] = applySelectivity(leftEstimated, rightEstimated, RIGHT_TO_LEFT);
        if (currentEstimation[0] == Queries.UNKNOWN) {
          currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]);
        }
        continue;
      }

      boolean leftBloomed = false;
      boolean rightBloomed = false;
      // try bloom filter
      if (!element.isCrossJoin()) {
        Query query0 = queries.get(i);
        Query query1 = queries.get(i + 1);
        if (joinType.isLeftDrivable() && rightEstimated[0] > bloomFilterThreshold) {
          // left to right
          Factory bloom = bloom(query0, leftJoinColumns, leftEstimated[0], query1, rightJoinColumns);
          if (bloom != null) {
            LOG.info("-- applying bloom filter %s (L) to %s:%d (R)", bloom.getBloomSource(), rightAlias, rightEstimated[0]);
            queries.set(i + 1, DataSources.applyFilter(query1, bloom, rightJoinColumns));
            rightBloomed = true;
          }
        }
        if (joinType.isRightDrivable() && leftEstimated[0] > bloomFilterThreshold) {
          // right to left
          Factory bloom = bloom(query1, rightJoinColumns, rightEstimated[0], query0, leftJoinColumns);
          if (bloom != null) {
            LOG.info("-- applying bloom filter %s (R) to %s:%d (L)", bloom.getBloomSource(), leftAlias, leftEstimated[0]);
            queries.set(i, DataSources.applyFilter(query0, bloom, leftJoinColumns));
            leftBloomed = true;
          }
        }
      }
      if (element.isInnerJoin() && leftEstimated[0] >= 0 && rightEstimated[0] >= 0) {
        Query query0 = queries.get(i);
        Query query1 = queries.get(i + 1);
        if (!leftBloomed && leftEstimated[0] < forcedFilterTinyThreshold && rightEstimated[0] >= forcedFilterHugeThreshold) {
          Query.ArrayOutputSupport<?> converted = element.forceLeftToFilter(query0, query1);
          if (converted != null) {
            DimFilter filter = SemiJoinFactory.from(rightJoinColumns, QueryRunners.runArray(converted, segmentWalker));
            LOG.info("-- %s:%d (L) is applied to %s (R) as filter on %s", leftAlias, leftEstimated[0], rightAlias, rightJoinColumns);
            rightEstimated[0] = Math.max(1, rightEstimated[0] >>> 2);
            queries.set(i + 1, DimFilters.and((FilterSupport) query1, filter));
          }
        }
        if (!rightBloomed && leftEstimated[0] >= forcedFilterHugeThreshold && rightEstimated[0] < forcedFilterTinyThreshold) {
          Query.ArrayOutputSupport<?> converted = element.forceRightToFilter(query0, query1);
          if (converted != null) {
            DimFilter filter = SemiJoinFactory.from(leftJoinColumns, QueryRunners.runArray(converted, segmentWalker));
            LOG.info("-- %s:%d (R) is applied to %s (L) as filter on %s", rightAlias, rightEstimated[0], leftAlias, leftJoinColumns);
            leftEstimated[0] = Math.max(1, leftEstimated[0] >>> 2);
            queries.set(i, DimFilters.and((FilterSupport) query0, filter));
          }
        }
      }
      if (element.isInnerJoin() && rightEstimated[0] < rightEstimated[1]) {
        long estimation = applySelectivity(leftEstimated, rightEstimated, RIGHT_TO_LEFT);
        if (estimation == Queries.UNKNOWN && leftEstimated[0] >= 0) {
          estimation = leftEstimated[0] >>> 2;
        }
        leftEstimated[0] = estimation;
      }
      if (element.isInnerJoin() && leftEstimated[0] < leftEstimated[1]) {
        long estimation = applySelectivity(leftEstimated, rightEstimated, LEFT_TO_RIGHT);
        if (estimation == Queries.UNKNOWN && rightEstimated[0] >= 0) {
          estimation = rightEstimated[0] >>> 2;
        }
        rightEstimated[0] = estimation;
      }
      if (element.isInnerJoin() && (rightEstimated[0] < rightEstimated[1] || leftEstimated[0] < leftEstimated[1])) {
        currentEstimation[0] = Math.max(leftEstimated[0], rightEstimated[0]);
      } else {
        currentEstimation[0] = roughEstimation(element, leftEstimated[0], rightEstimated[0]);
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

    Map<String, Object> joinContext = BaseQuery.copyContextForMeta(context, CARDINALITY, currentEstimation[0]);
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
        new JoinPostProcessor(config.getJoin(), elements, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow)
    );
  }

  private Query propagateCardinality(Query query, long[] currentEstimation)
  {
    return currentEstimation[0] > 0 ? query.withOverriddenContext(CARDINALITY, currentEstimation[0]) : query;
  }

  private long applySelectivity(long[] leftEstimation, long[] rightEstimation, boolean leftToRight)
  {
    if (leftEstimation[0] < 0 || rightEstimation[0] < 0) {
      return Queries.UNKNOWN;
    }
    if (leftToRight && leftEstimation[1] > 0) {
      return (long) (rightEstimation[0] * Math.max(0.04f, (float) leftEstimation[0] / leftEstimation[1]));
    } else if (!leftToRight && rightEstimation[1] > 0) {
      return (long) (leftEstimation[0] * Math.max(0.04f, (float) rightEstimation[0] / rightEstimation[1]));
    }
    return Queries.UNKNOWN;
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
    return estimation >= 0 && threshold >= 0 && estimation <= threshold;
  }

  private long roughEstimation(JoinElement element, long leftEstimation, long rightEstimated)
  {
    if (leftEstimation < 0 || rightEstimated < 0) {
      return Queries.UNKNOWN;
    }
    if (element.isCrossJoin()) {
      return leftEstimation * rightEstimated;
    }
    switch (element.getJoinType()) {
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

  private static Factory bloom(
      Query source, List<String> sourceJoinOn, long sourceCardinality,
      Query target, List<String> targetJoinOn
  )
  {
    DimFilter filter = removeTrivials(removeFactory(BaseQuery.getDimFilter(source)), sourceJoinOn);
    if (filter != null &&
        DataSources.isDataNodeSourced(source) &&
        source.getContextValue(Query.LOCAL_POST_PROCESSING) == null) {
      // this is evaluated in UnionQueryRunner
      List<DimensionSpec> extracted = DataSources.findFilterableOn(target, targetJoinOn, q -> !Queries.isNestedQuery(q));
      if (extracted != null) {
        ViewDataSource view = BaseQuery.asView(source, filter, sourceJoinOn);
        return Factory.fields(extracted, view, Ints.checkedCast(sourceCardinality));
      }
    }
    return null;
  }

  private static DimFilter removeFactory(DimFilter filter)
  {
    return filter == null ? null : DimFilters.rewrite(filter, f -> f instanceof DimFilter.Factory ? null : f);
  }

  private static DimFilter removeTrivials(DimFilter filter, List<String> joinColumns)
  {
    if (joinColumns.size() == 1 && DimFilters.not(SelectorDimFilter.of(joinColumns.get(0), null)).equals(filter)) {
      return null;
    }
    if (filter instanceof AndDimFilter) {
      boolean changed = false;
      Set<DimFilter> filters = Sets.newLinkedHashSet(((AndDimFilter) filter).getFields());
      for (String joinColumn : joinColumns) {
        changed |= filters.remove(DimFilters.not(SelectorDimFilter.of(joinColumn, null)));
      }
      return changed ? DimFilters.and(Lists.newArrayList(filters)) : filter;
    }
    return filter;
  }

  public JoinQuery withPrefixAlias(boolean prefixAlias)
  {
    return new JoinQuery(
        getDataSources(),
        getQuerySegmentSpec(),
        getElements(),
        prefixAlias,
        asMap,
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
        asMap,
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
        asMap,
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
    if (asMap != that.asMap) {
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
           ", asMap=" + asMap +
           ", maxOutputRow=" + maxOutputRow +
           (outputColumns == null ? "" : ", outputColumns= " + outputColumns) +
           ", limit=" + limit +
           '}';
  }

  public static class JoinHolder extends UnionAllQuery<Object[]>
      implements ArrayOutputSupport<Object[]>, SchemaProvider, LastProjectionSupport<Object[]>
  {
    private final String alias;   // just for debug
    private final List<String> outputAlias;
    private final List<String> outputColumns;

    private final String timeColumnName;

    private final Execs.SettableFuture<List<List<OrderByColumnSpec>>> future = new Execs.SettableFuture<>();

    public JoinHolder(
        String alias,
        List<Query<Object[]>> queries,
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

    @Override
    public List<String> getOutputColumns()
    {
      return outputColumns;
    }

    @Override
    public JoinHolder withOutputColumns(List<String> outputColumns)
    {
      PostProcessingOperator rewritten = PostProcessingOperators.rewriteLast(
          getContextValue(Query.POST_PROCESSING),
          p -> ((CommonJoinProcessor) p).withOutputColumns(outputColumns)
      );
      return new JoinHolder(
          alias,
          getQueries(),
          timeColumnName,
          outputAlias,
          outputColumns,
          getLimit(),
          computeOverriddenContext(Query.POST_PROCESSING, rewritten)
      ).withSchema(schema);
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
      return !Preconditions.checkNotNull(getLastJoinProcessor(), "no join processor").isAsMap();
    }

    private static final IdentityFunction<PostProcessingOperator> AS_ARRAY = new IdentityFunction<PostProcessingOperator>()
    {
      @Override
      public PostProcessingOperator apply(PostProcessingOperator input)
      {
        if (input instanceof CommonJoinProcessor) {
          CommonJoinProcessor join = (CommonJoinProcessor) input;
          if (join.isAsMap()) {
            input = join.withAsMap(false);
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
        Query<Object[]> query0 = getQueries().get(0);
        Query<Object[]> query1 = getQueries().get(1);
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
        Query<Object[]> query,
        List<Query<Object[]>> queries,
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
