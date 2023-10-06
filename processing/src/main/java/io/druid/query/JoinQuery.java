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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.bloomfilter.BloomFilterAggregator;
import io.druid.query.aggregation.bloomfilter.BloomKFilter;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.BloomDimFilter.Factory;
import io.druid.query.filter.CompressedInFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilter.FilterFactory;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SemiJoinFactory;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("join")
public class JoinQuery extends BaseQuery<Object[]> implements Query.RewritingQuery<Object[]>, Query.SchemaHolder
{
  public static final String HASHING = "$hash";
  public static final String SORTING = "$sort";

  public static boolean isHashing(Query<?> query)
  {
    return query.getContextBoolean(HASHING, false);
  }

  public static boolean isSorting(Query<?> query)
  {
    return query.getContextBoolean(SORTING, false);
  }

  private static final Logger LOG = new Logger(JoinQuery.class);

  private final Map<String, DataSource> dataSources;
  private final JoinElement element;
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
      @JsonProperty("element") JoinElement element,
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
    super(UnionDataSource.of(element.getAliases()), querySegmentSpec, false, context);
    this.dataSources = dataSources;
    this.prefixAlias = prefixAlias;
    this.asMap = asMap;
    this.timeColumnName = timeColumnName;
    this.element = element;
    this.limit = limit;
    this.maxOutputRow = maxOutputRow;
    this.outputAlias = outputAlias;
    this.outputColumns = outputColumns;
  }

  static Map<String, DataSource> normalize(Map<String, DataSource> dataSources)
  {
    if (!Iterables.any(dataSources.values(), d -> d instanceof TableDataSource)) {
      return dataSources;
    }
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

  static JoinElement validateElement(Map<String, DataSource> dataSources, JoinElement element)
  {
    Preconditions.checkNotNull(element, "'element' should not be null");
    element = element.rewrite(dataSources.keySet());
    validateDataSource(dataSources, element.getLeftAlias(), element.getLeftJoinColumns());
    validateDataSource(dataSources, element.getRightAlias(), element.getRightJoinColumns());
    return element;
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
  public JoinElement getElement()
  {
    return element;
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
  public RowSignature schema()
  {
    return schema;
  }

  public JoinQuery withSchema(RowSignature schema)
  {
    this.schema = schema;
    return this;
  }

  @Override
  public JoinQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        dataSources,
        getQuerySegmentSpec(),
        element,
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

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker walker)
  {
    if (schema != null && outputColumns != null) {
      Preconditions.checkArgument(
          outputColumns.equals(schema.getColumnNames()),
          "Invalid schema %s, expected column names %s", schema, outputColumns
      );
    }
    final ObjectMapper mapper = walker.getMapper();

    final QueryConfig config = walker.getConfig();
    final int maxResult = config.getJoin().getMaxOutputRow(maxOutputRow);
    final int hashThreshold = config.getHashJoinThreshold(this);
    final int semiJoinThrehold = config.getSemiJoinThreshold(this);
    final int broadcastThreshold = config.getBroadcastJoinThreshold(this);
    final int bloomFilterThreshold = config.getBloomFilterThreshold(this);
    final int forcedFilterHugeThreshold = config.getForcedFilterHugeThreshold(this);
    final int forcedFilterTinyThreshold = config.getForcedFilterTinyThreshold(this);

    final Map<String, Object> context = getContext();
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();

    final JoinType joinType = element.getJoinType();
    final List<String> leftJoinColumns = element.getLeftJoinColumns();
    final List<String> rightJoinColumns = element.getRightJoinColumns();

    final String leftAlias = element.getLeftAlias();
    final String rightAlias = element.getRightAlias();

    final DataSource left = dataSources.get(leftAlias);
    final DataSource right = dataSources.get(rightAlias);

    final Estimation leftEstimated = Estimations.estimate(left, segmentSpec, context, walker);
    final Estimation rightEstimated = Estimations.estimate(right, segmentSpec, context, walker);

    final String typeString = element.getJoinTypeString();
    LOG.debug("-> %s (%s:%s + %s:%s)", typeString, leftAlias, leftEstimated, rightAlias, rightEstimated);

    if (!getContextBoolean(Query.OUTERMOST_JOIN, false)) {
      element.earlyCheckMaxJoin(leftEstimated.estimated, rightEstimated.estimated, maxResult);
    }

    // try convert semi-join to filter
    if (semiJoinThrehold > 0 && element.isInnerJoin() && outputColumns != null) {
      if (rightEstimated.lte(semiJoinThrehold) && element.isLeftSemiJoinable(left, right, outputColumns)) {
        Query query1 = JoinElement.toQuery(walker, right, segmentSpec, context);
        List<String> rightColumns = query1.estimatedOutputColumns();
        if (rightColumns != null && rightColumns.containsAll(rightJoinColumns)) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, walker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query1 instanceof MaterializedQuery)) {
            walker.register(right, MaterializedQuery.of(query1, sequence.columns(), values));
            LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          }
          rightEstimated.update(values.size());
          values = Lists.transform(values, GuavaUtils.mapper(rightColumns, rightJoinColumns));
          Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
              leftJoinColumns, values, JoinElement.allowDuplication(left, leftJoinColumns), outputColumns
          );
          DataSource filtered = DataSources.applyFilter(left, converted.lhs, rightEstimated, walker);
          if (converted.rhs == null) {
            filtered = DataSources.applyProjection(filtered, outputColumns);
          }
          LOG.debug("-- %s:%s (R) is merged into %s (L) as filter on %s", rightAlias, rightEstimated, leftAlias, leftJoinColumns);
          Query query = JoinElement.toQuery(walker, filtered, segmentSpec, context);
          if (converted.rhs != null) {
            query = PostProcessingOperators.appendLocal(query, converted.rhs);
          }
          Estimation estimation = leftEstimated.multiply(rightEstimated);
          out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
          return estimation.propagate(query);
        }
      }
      if (leftEstimated.lte(semiJoinThrehold) && element.isRightSemiJoinable(left, right, outputColumns)) {
        Query query0 = JoinElement.toQuery(walker, left, segmentSpec, context);
        List<String> leftColumns = query0.estimatedOutputColumns();
        if (leftColumns != null && leftColumns.containsAll(leftJoinColumns)) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, walker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query0 instanceof MaterializedQuery)) {
            walker.register(left, MaterializedQuery.of(query0, sequence.columns(), values));
            LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
          }
          leftEstimated.update(values.size());
          values = Lists.transform(values, GuavaUtils.mapper(leftColumns, leftJoinColumns));
          Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
              rightJoinColumns, values, JoinElement.allowDuplication(right, rightJoinColumns), outputColumns
          );
          DataSource filtered = DataSources.applyFilter(right, converted.lhs, leftEstimated, walker);
          if (converted.rhs == null) {
            filtered = DataSources.applyProjection(filtered, outputColumns);
          }
          LOG.debug("-- %s:%s (L) is merged into %s (R) as filter on %s", leftAlias, leftEstimated, rightAlias, rightJoinColumns);
          Query query = JoinElement.toQuery(walker, filtered, segmentSpec, context);
          if (converted.rhs != null) {
            query = PostProcessingOperators.appendLocal(query, converted.rhs);
          }
          Estimation estimation = rightEstimated.multiply(leftEstimated);
          out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
          return estimation.propagate(query);
        }
      }
    }

    // try broadcast join
    if (!element.isCrossJoin() && broadcastThreshold > 0) {
      boolean leftBroadcast = joinType.isRightDrivable() && DataSources.isDataNodeSourced(right) &&
                              leftEstimated.lte(rightEstimated.estimated << 2) &&
                              leftEstimated.lte(adjust(broadcastThreshold, left, right));
      boolean rightBroadcast = joinType.isLeftDrivable() && DataSources.isDataNodeSourced(left) &&
                               rightEstimated.lte(leftEstimated.estimated << 2) &&
                               rightEstimated.lte(adjust(broadcastThreshold, right, left));
      if (leftBroadcast && rightBroadcast) {
        if (leftExpensive(left, leftEstimated, right, rightEstimated)) {
          leftBroadcast = false;
        } else {
          rightBroadcast = false;
        }
      }
      if (leftBroadcast) {
        Query query0 = JoinElement.toQuery(walker, left, segmentSpec, context);
        Query query1 = JoinElement.toQuery(walker, right, segmentSpec, context);
        LOG.debug("-- %s:%s (L) will be broadcasted to %s (R)", leftAlias, leftEstimated, rightAlias);
        RowSignature signature = DataSources.schema(left, query0, walker);
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, walker);
        List<Object[]> values = Sequences.toList(sequence);
        if (!(query0 instanceof MaterializedQuery)) {
          walker.register(left, MaterializedQuery.of(query0, sequence.columns(), values).withSchema(signature));
          LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
        }
        query0 = leftEstimated.update(values.size()).propagate(query0);
        boolean applyfilter = false;
        if (element.isInnerJoin() && leftEstimated.moreSelective(rightEstimated)) {
          if (leftEstimated.lt(rightEstimated) && DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
            applyfilter = true;
            LOG.debug("-- %s:%s (L) will be applied as local filter to %s (R)", leftAlias, leftEstimated, rightAlias);
          } else if (rightEstimated.gt(bloomFilterThreshold) && query0.hasFilters() && DataSources.isFilterableOn(query1, rightJoinColumns)) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(resolver, leftJoinColumns, values.size(), values);
            query1 = DataSources.applyFilter(
                query1, BloomDimFilter.of(rightJoinColumns, bloom), leftEstimated.selectivity, walker
            );
            LOG.debug("-- .. with bloom filter %s(%s) to %s (R)", leftAlias, BaseQuery.getDimFilter(query0), rightAlias);
          }
          rightEstimated.update(leftEstimated.selectivity).propagate(query1);
        }
        byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
        BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
            mapper, config, element, true, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes, applyfilter
        );
        Estimation estimation = Estimations.join(element, leftEstimated, rightEstimated);
        out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
        query1 = PostProcessingOperators.appendLocal(query1, processor);
        query1 = ((LastProjectionSupport) query1).withOutputColumns(null);
        return estimation.propagate(query1);
      }
      if (rightBroadcast) {
        Query query0 = JoinElement.toQuery(walker, left, segmentSpec, context);
        Query query1 = JoinElement.toQuery(walker, right, segmentSpec, context);
        LOG.debug("-- %s:%s (R) will be broadcasted to %s (L)", rightAlias, rightEstimated, leftAlias);
        RowSignature signature = DataSources.schema(right, query1, walker);
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, walker);
        List<Object[]> values = Sequences.toList(sequence);
        if (!(query1 instanceof MaterializedQuery)) {
          walker.register(right, MaterializedQuery.of(query1, sequence.columns(), values).withSchema(signature));
          LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
        }
        rightEstimated.update(values.size()).propagate(query1);
        boolean applyfilter = false;
        if (element.isInnerJoin() && rightEstimated.moreSelective(leftEstimated)) {
          if (rightEstimated.lt(leftEstimated) && DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
            applyfilter = true;
            LOG.debug("-- %s:%s (R) will be applied as local filter to %s (L)", rightAlias, rightEstimated, leftAlias);
          } else if (leftEstimated.gt(bloomFilterThreshold) && query1.hasFilters() && DataSources.isFilterableOn(query0, leftJoinColumns)) {
            RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
            BloomKFilter bloom = BloomFilterAggregator.build(resolver, rightJoinColumns, values.size(), values);
            query0 = DataSources.applyFilter(
                query0, BloomDimFilter.of(leftJoinColumns, bloom), rightEstimated.selectivity, walker
            );
            LOG.debug("-- .. with bloom filter %s(%s) to %s (L)", rightAlias, BaseQuery.getDimFilter(query1), leftAlias);
          }
          leftEstimated.update(rightEstimated.selectivity).propagate(query0);
        }
        byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
        BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
            mapper, config, element, false, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes, applyfilter
        );
        Estimation estimation = Estimations.join(element, leftEstimated, rightEstimated);
        out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
        query0 = PostProcessingOperators.appendLocal(query0, processor);
        query0 = ((LastProjectionSupport) query0).withOutputColumns(null);
        return estimation.propagate(query0);
      }
    }

    // try hash-join
    boolean leftHashing = joinType.isRightDrivable() && leftEstimated.lte(hashThreshold);
    boolean rightHashing = joinType.isLeftDrivable() && rightEstimated.lte(hashThreshold);
    if (leftHashing && rightHashing) {
      if (leftExpensive(left, leftEstimated, right, rightEstimated)) {
        leftHashing = false;
      } else {
        rightHashing = false;
      }
    }
    List<String> leftSort = leftHashing || rightHashing ? null : leftJoinColumns;
    Query query0 = JoinElement.toQuery(walker, left, leftSort, segmentSpec, leftEstimated.propagate(context));
    if (leftHashing) {
      query0 = query0.withOverriddenContext(HASHING, true);
    }

    List<String> rightSort = leftHashing || rightHashing ? null : rightJoinColumns;
    Query query1 = JoinElement.toQuery(walker, right, rightSort, segmentSpec, rightEstimated.propagate(context));
    if (rightHashing) {
      query1 = query1.withOverriddenContext(HASHING, true);
    }

    LOG.debug("-- %s:%s (L) (%s)", leftAlias, leftEstimated, leftHashing ? "hash" : leftSort != null ? "sort" : "-");

    if (leftHashing && element.isInnerJoin() &&
        leftEstimated.lte(semiJoinThrehold) &&
        (query0 instanceof MaterializedQuery || DataSources.isDataNodeSourced(query0)) &&
        DataSources.isFilterableOn(right, rightJoinColumns)) {
      List<String> outputColumns = query0.estimatedOutputColumns();
      if (outputColumns != null) {
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, walker);
        List<Object[]> values = Sequences.toList(sequence);
        MaterializedQuery materialized = MaterializedQuery.of(query0, sequence.columns(), values);
        if (!(query0 instanceof MaterializedQuery)) {
          walker.register(left, materialized);
          leftEstimated.update(values.size());
        }
        LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
        Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, leftJoinColumns));
        DataSource applied = DataSources.applyFilter(
            QueryDataSource.of(query1), SemiJoinFactory.from(rightJoinColumns, keys.iterator()), leftEstimated, walker
        );
        LOG.debug("-- %s:%s (L) (hash) is applied as filter to %s (R)", leftAlias, leftEstimated, rightAlias);
        query0 = materialized;
        query1 = DataSources.nestedQuery(applied);
      }
    }

    LOG.debug("-- %s:%s (R) (%s)", rightAlias, rightEstimated.estimated, rightHashing ? "hash" : rightSort != null ? "sort" : "-");

    if (rightHashing && element.isInnerJoin() &&
        rightEstimated.lte(semiJoinThrehold) &&
        (query1 instanceof MaterializedQuery || DataSources.isDataNodeSourced(query1)) &&
        DataSources.isFilterableOn(left, leftJoinColumns)) {
      List<String> outputColumns = query1.estimatedOutputColumns();
      if (outputColumns != null) {
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, walker);
        List<Object[]> values = Sequences.toList(sequence);
        MaterializedQuery materialized = MaterializedQuery.of(query1, sequence.columns(), values);
        if (!(query1 instanceof MaterializedQuery)) {
          walker.register(right, materialized);
          rightEstimated.update(values.size());
        }
        LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
        Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, rightJoinColumns));
        DataSource applied = DataSources.applyFilter(
            QueryDataSource.of(query0), SemiJoinFactory.from(leftJoinColumns, keys.iterator()), rightEstimated, walker
        );
        LOG.debug("-- %s:%s (R) (hash) is applied as filter to %s (L)", rightAlias, rightEstimated, leftAlias);
        query0 = DataSources.nestedQuery(applied);
        query1 = materialized;
      }
    }
    if (!(query0 instanceof MaterializedQuery) && !(query1 instanceof MaterializedQuery)) {
      boolean leftBloomed = false;
      boolean rightBloomed = false;
      // try bloom filter
      if (joinType.isLeftDrivable() && rightEstimated.gt(bloomFilterThreshold) && leftEstimated.moreSelective(rightEstimated)) {
        // left to right
        Factory bloom = DataSources.bloom(query0, leftJoinColumns, leftEstimated, query1, rightJoinColumns);
        if (bloom != null) {
          LOG.debug("-- .. with bloom filter %s (L) to %s:%s (R)", bloom.getBloomSource(), rightAlias, rightEstimated);
          query1 = DataSources.applyFilter(query1, bloom, leftEstimated.degrade(), rightJoinColumns, walker);
          List<Query> queries = DataSources.filterMerged(element, Arrays.asList(query0, query1), 1, 0, walker);
          query0 = queries.get(0);
          query1 = queries.get(1);
          rightEstimated.updateFrom(query1);
          rightBloomed = true;
        }
      } else if (joinType.isRightDrivable() && leftEstimated.gt(bloomFilterThreshold) && rightEstimated.moreSelective(leftEstimated)) {
        // right to left
        Factory bloom = DataSources.bloom(query1, rightJoinColumns, rightEstimated, query0, leftJoinColumns);
        if (bloom != null) {
          LOG.debug("-- .. with bloom filter %s (R) to %s:%s (L)", bloom.getBloomSource(), leftAlias, leftEstimated);
          query0 = DataSources.applyFilter(query0, bloom, rightEstimated.degrade(), leftJoinColumns, walker);
          List<Query> queries = DataSources.filterMerged(element, Arrays.asList(query0, query1), 0, 1, walker);
          query0 = queries.get(0);
          query1 = queries.get(1);
          leftEstimated.updateFrom(query0);
          leftBloomed = true;
        }
      }
      if (!leftBloomed && joinType.isLeftDrivable() &&
          (leftHashing || leftEstimated.lte(forcedFilterTinyThreshold)) &&
          rightEstimated.gte(forcedFilterHugeThreshold) &&
          DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
        int[] indices = GuavaUtils.indexOf(query0.estimatedOutputColumns(), leftJoinColumns);
        if (indices != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, walker);
          List<Object[]> values = Sequences.toList(sequence);
          DimFilter filter = new ForcedFilter(rightJoinColumns, values, indices);
          LOG.debug("-- .. with forced filter from %s (L) to %s (R) + %s", leftAlias, rightAlias, filter);
          query0 = walker.register(left, MaterializedQuery.of(query0, sequence.columns(), values));
          query1 = Estimations.mergeSelectivity(DimFilters.and(query1, filter), leftEstimated.degrade());
          rightEstimated.updateFrom(query1);
        }
      }
      if (!rightBloomed && joinType.isRightDrivable() &&
          leftEstimated.gte(forcedFilterHugeThreshold) &&
          (rightHashing || rightEstimated.lte(forcedFilterTinyThreshold)) &&
          DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
        int[] indices = GuavaUtils.indexOf(query1.estimatedOutputColumns(), rightJoinColumns);
        if (indices != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, walker);
          List<Object[]> values = Sequences.toList(sequence);
          DimFilter filter = new ForcedFilter(leftJoinColumns, values, indices);
          LOG.debug("-- .. with forced filter from %s (R) to %s (L) + %s", rightAlias, leftAlias, filter);
          query0 = Estimations.mergeSelectivity(DimFilters.and(query0, filter), rightEstimated.degrade());
          query1 = walker.register(right, MaterializedQuery.of(query1, sequence.columns(), values));
          leftEstimated.updateFrom(query1);
        }
      }
    }
    Estimation estimation = Estimations.join(element, leftEstimated, rightEstimated);
    out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);

    List<String> aliases = element.getAliases();

    String timeColumn;
    if (prefixAlias) {
      timeColumn = timeColumnName == null ? aliases.get(0) + "." + Column.TIME_COLUMN_NAME : timeColumnName;
    } else {
      timeColumn = timeColumnName == null ? Column.TIME_COLUMN_NAME : timeColumnName;
    }

    String alias = StringUtils.concat("+", aliases);
    List<Query<Object[]>> queries = GuavaUtils.cast(Arrays.asList(query0, query1));
    JoinHolder query = new JoinHolder(
        alias, element, queries, timeColumn, outputAlias, outputColumns, limit, estimation.propagate(context)
    );
    Supplier<RowSignature> signature = schema == null ? null : Suppliers.ofInstance(schema);
    if (signature == null) {
      signature = Suppliers.memoize(() -> {
        List<String> columnNames = Lists.newArrayList();
        List<ValueDesc> columnTypes = Lists.newArrayList();
        Set<String> uniqueNames = Sets.newHashSet();
        for (int i = 0; i < queries.size(); i++) {
          final RowSignature element = Queries.relaySchema(queries.get(i), walker);
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
        new JoinPostProcessor(config.getJoin(), element, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow)
    );
  }

  private static void out(
      String typeString,
      String leftAlias,
      String rightAlias,
      Estimation leftEstimated,
      Estimation rightEstimated,
      Estimation estimation
  )
  {
    LOG.debug(
        "<- %s (%s:%s + %s:%s) : (%s+%s:%s)",
        typeString,
        leftAlias,
        leftEstimated,
        rightAlias,
        rightEstimated,
        leftAlias,
        rightAlias,
        estimation
    );
  }

  private static long adjust(long threshold, DataSource materializing, DataSource current)
  {
    return (long) (threshold * DataSources.roughCost(current) / DataSources.roughCost(materializing));
  }

  private static boolean leftExpensive(DataSource ds0, Estimation estimation0, DataSource ds1, Estimation estimation1)
  {
    if (Estimation.delta(estimation0, estimation1) < Estimation.max(estimation0, estimation1) * 0.1f) {
      return DataSources.roughCost(ds0) >= DataSources.roughCost(ds1);
    }
    return estimation0.gt(estimation1);
  }

  public JoinQuery withPrefixAlias(boolean prefixAlias)
  {
    return new JoinQuery(
        getDataSources(),
        getQuerySegmentSpec(),
        getElement(),
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
        element,
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
        element,
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

    if (!Objects.equals(element, that.element)) {
      return false;
    }
    String left = element.getLeftAlias();
    if (!Objects.equals(dataSources.get(left), that.dataSources.get(left))) {
      return false;
    }
    String right = element.getRightAlias();
    if (!Objects.equals(dataSources.get(right), that.dataSources.get(right))) {
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
           ", element=" + element +
           ", prefixAlias=" + prefixAlias +
           ", asMap=" + asMap +
           ", maxOutputRow=" + maxOutputRow +
           (outputColumns == null ? "" : ", outputColumns= " + outputColumns) +
           ", limit=" + limit +
           '}';
  }

  public static class JoinHolder extends UnionAllQuery<Object[]>
      implements ArrayOutputSupport<Object[]>, LastProjectionSupport<Object[]>
  {
    private final String alias;   // just for debug
    private final JoinElement element;
    private final List<String> outputAlias;
    private final List<String> outputColumns;

    private final String timeColumnName;

    private final Execs.SettableFuture<List<List<OrderByColumnSpec>>> future = new Execs.SettableFuture<>();

    public JoinHolder(
        String alias,
        JoinElement element,
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
      this.element = element;
      this.outputAlias = outputAlias;
      this.outputColumns = outputColumns;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
    }

    public String getAlias()
    {
      return alias;
    }

    public JoinElement getElement()
    {
      return element;
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
    public Query rewriteQuery(QuerySegmentWalker segmentWalker)
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
          element,
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
        Query<Object[]> query0 = getQuery(0);
        Query<Object[]> query1 = getQuery(1);
        boolean hashing0 = JoinQuery.isHashing(query0);
        boolean hashing1 = JoinQuery.isHashing(query1);
        if (hashing0 && hashing1) {
          return holder;    // todo use tree map?
        }
        final JoinType type = element.getJoinType();
        if (type.isLeftDrivable()) {
          if (hashing1 && query0 instanceof OrderingSupport) {
            OrderingSupport reordered = tryReordering((OrderingSupport) query0, sortColumns);
            if (reordered != null) {
              LOG.debug("--- reordered.. %s : %s", query0.getDataSource().getNames(), reordered.getResultOrdering());
              return (JoinHolder) holder.withQueries(
                  Arrays.asList(reordered.withOverriddenContext(SORTING, true), query1)
              );
            }
          }
        }
        if (type.isRightDrivable()) {
          if (hashing0 && query1 instanceof OrderingSupport) {
            OrderingSupport reordered = tryReordering((OrderingSupport) query1, sortColumns);
            if (reordered != null) {
              LOG.debug("--- reordered.. %s : %s", query1.getDataSource().getNames(), reordered.getResultOrdering());
              return (JoinHolder) holder.withQueries(
                  Arrays.asList(query0, reordered.withOverriddenContext(SORTING, true))
              );
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
      return isArrayOutput() ? sequence : Sequences.map(sequence, Rows.mapToArray(sequence.columns()));
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
          element,
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
    public String toString()
    {
      return "CommonJoin{" +
             "queries=" + getQueries() +
             (timeColumnName == null ? "" : ", timeColumnName=" + timeColumnName) +
             (getLimit() > 0 ? ", limit=" + getLimit() : "") +
             '}';
    }
  }

  private static class ForcedFilter extends FilterFactory implements DimFilter.Rewriting, DimFilter.Discardable
  {
    private final List<String> fieldNames;
    private final List<Object[]> values;
    private final int[] indices;

    private ForcedFilter(List<String> fieldNames, List<Object[]> values, int[] indices)
    {
      this.fieldNames = fieldNames;
      this.values = values;
      this.indices = indices;
    }

    @Override
    public DimFilter rewrite(QuerySegmentWalker walker, Query parent)
    {
      return SemiJoinFactory.from(fieldNames, Iterables.transform(values, GuavaUtils.mapper(indices)));
    }

    @Override
    public boolean discard(DimFilter filter)
    {
      return values.size() > CompressedInFilter.TRIVIAL_SIZE;
    }

    @Override
    public String toString()
    {
      return "ForcedFilter{" +
             "fieldNames=" + fieldNames +
             ", valuesLen=" + values.size() +
             '}';
    }
  }
}
