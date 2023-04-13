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
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.bloomfilter.BloomFilterAggregator;
import io.druid.query.aggregation.bloomfilter.BloomKFilter;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BloomDimFilter;
import io.druid.query.filter.BloomDimFilter.Factory;
import io.druid.query.filter.CompressedInFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilter.FilterFactory;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.SelectorDimFilter;
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
  public static final String CARDINALITY = "$cardinality";
  public static final String SELECTIVITY = "$selectivity";

  public static boolean isHashing(Query<?> query)
  {
    return query.getContextBoolean(HASHING, false);
  }

  public static boolean isSorting(Query<?> query)
  {
    return query.getContextBoolean(SORTING, false);
  }

  public static int getCardinality(Query<?> query)
  {
    return query.getContextInt(CARDINALITY, -1);
  }

  public static float getSelectivity(Query<?> query)
  {
    return query.getContextFloat(SELECTIVITY, 1f);
  }

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

  @Override
  @SuppressWarnings("unchecked")
  public Query rewriteQuery(QuerySegmentWalker segmentWalker)
  {
    if (schema != null && outputColumns != null) {
      Preconditions.checkArgument(
          outputColumns.equals(schema.getColumnNames()),
          "Invalid schema %s, expected column names %s", schema, outputColumns
      );
    }
    final ObjectMapper mapper = segmentWalker.getMapper();

    final QueryConfig config = segmentWalker.getConfig();
    final int maxResult = config.getJoin().getMaxOutputRow(maxOutputRow);
    final int hashThreshold = config.getHashJoinThreshold(this);
    final int semiJoinThrehold = config.getSemiJoinThreshold(this);
    final int broadcastThreshold = config.getBroadcastJoinThreshold(this);
    final int bloomFilterThreshold = config.getBloomFilterThreshold(this);
    final int forcedFilterHugeThreshold = config.getForcedFilterHugeThreshold(this);
    final int forcedFilterTinyThreshold = config.getForcedFilterTinyThreshold(this);

    final Map<String, Object> context = getContext();
    final QuerySegmentSpec segmentSpec = getQuerySegmentSpec();

    final List<Query<Object[]>> queries = Lists.newArrayList();

    long estimation = 0;
    float selectivity = 1f;

    for (int i = 0; i < elements.size(); i++) {
      final JoinElement element = elements.get(i);
      final JoinType joinType = element.getJoinType();
      final List<String> leftJoinColumns = element.getLeftJoinColumns();
      final List<String> rightJoinColumns = element.getRightJoinColumns();

      final String leftAlias = element.getLeftAlias();
      final String rightAlias = element.getRightAlias();

      DataSource left = i == 0 ? dataSources.get(leftAlias) : null;
      DataSource right = dataSources.get(rightAlias);

      Estimation leftEstimated = i > 0 ? Estimation.of(estimation, selectivity) :
                                 Estimations.estimate(left, segmentSpec, context, segmentWalker);
      Estimation rightEstimated = Estimations.estimate(right, segmentSpec, context, segmentWalker);

      LOG.debug("-> %s (%s:%s + %s:%s)", element.getJoinTypeString(), leftAlias, leftEstimated, rightAlias, rightEstimated);

      if (!getContextBoolean(Query.OUTERMOST_JOIN, false)) {
        element.earlyCheckMaxJoin(leftEstimated.estimated, rightEstimated.estimated, maxResult);
      }

      // try convert semi-join to filter
      if (semiJoinThrehold > 0 && element.isInnerJoin() && outputColumns != null) {
        if (i == 0 && rightEstimated.lte(semiJoinThrehold) && element.isLeftSemiJoinable(left, right, outputColumns)) {
          Map<String, Object> propagated = Estimations.propagate(context, leftEstimated);
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, right, segmentSpec, propagated);
          List<String> rightColumns = array.estimatedOutputColumns();
          if (rightColumns != null && rightColumns.containsAll(rightJoinColumns)) {
            Sequence<Object[]> sequence = QueryRunners.resolveAndRun(array, segmentWalker);
            List<Object[]> values = Sequences.toList(sequence);
            if (!(array instanceof MaterializedQuery)) {
              segmentWalker.register(right, MaterializedQuery.of(array, sequence.columns(), values));
            }
            values = Lists.transform(values, GuavaUtils.mapper(rightColumns, rightJoinColumns));
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                leftJoinColumns, values, JoinElement.allowDuplication(left, leftJoinColumns)
            );
            DataSource filtered = DataSources.applyFilterAndProjection(
                left, converted.lhs, rightEstimated.selectivity, outputColumns, converted.rhs == null, segmentWalker
            );
            LOG.debug("-- %s:%s (R) is merged into %s (L) as filter on %s", rightAlias, rightEstimated, leftAlias, leftJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, propagated);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            estimation = leftEstimated.multiply(rightEstimated.selectivity);
            selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
            queries.add(Estimations.propagate(query, estimation, selectivity));
            continue;
          }
        }
        if (leftEstimated.lte(semiJoinThrehold) && element.isRightSemiJoinable(left, right, outputColumns)) {
          Map<String, Object> propagated = Estimations.propagate(context, rightEstimated);
          ArrayOutputSupport array = JoinElement.toQuery(segmentWalker, left, segmentSpec, propagated);
          List<String> leftColumns = array.estimatedOutputColumns();
          if (leftColumns != null && leftColumns.containsAll(leftJoinColumns)) {
            Sequence<Object[]> sequence = QueryRunners.resolveAndRun(array, segmentWalker);
            List<Object[]> values = Sequences.toList(sequence);
            if (!(array instanceof MaterializedQuery)) {
              segmentWalker.register(left, MaterializedQuery.of(array, sequence.columns(), values));
            }
            values = Lists.transform(values, GuavaUtils.mapper(leftColumns, leftJoinColumns));
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                rightJoinColumns, values, JoinElement.allowDuplication(right, rightJoinColumns)
            );
            DataSource filtered = DataSources.applyFilterAndProjection(
                right, converted.lhs, leftEstimated.selectivity, outputColumns, converted.rhs == null, segmentWalker
            );
            LOG.debug("-- %s:%s (L) is merged into %s (R) as filter on %s", leftAlias, leftEstimated, rightAlias, rightJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, propagated);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            estimation = rightEstimated.multiply(leftEstimated.selectivity);
            selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
            queries.add(Estimations.propagate(query, estimation, selectivity));
            continue;
          }
        }
      }

      // try broadcast join
      if (i == 0 && broadcastThreshold > 0 && DataSources.isDataNodeSourced(left) && DataSources.isDataNodeSourced(right)) {
        boolean leftBroadcast = joinType.isRightDrivable() && leftEstimated.lte(broadcastThreshold) && !segmentWalker.cached(right);
        boolean rightBroadcast = joinType.isLeftDrivable() && rightEstimated.lte(broadcastThreshold) && !segmentWalker.cached(left);
        if (leftBroadcast && rightBroadcast) {
          if (leftEstimated.gt(rightEstimated)) {
            leftBroadcast = false;
          } else {
            rightBroadcast = false;
          }
        }
        if (leftBroadcast) {
          Map<String, Object> context0 = Estimations.propagate(context, leftEstimated);
          Map<String, Object> context1 = Estimations.propagate(context, rightEstimated);
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context0);
          Query query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context1);
          LOG.debug("-- %s:%s (L) will be broadcasted to %s (R)", leftAlias, leftEstimated, rightAlias);
          RowSignature signature = Queries.relaySchema(query0, segmentWalker);
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query0 instanceof MaterializedQuery)) {
            segmentWalker.register(left, MaterializedQuery.of(query0, sequence.columns(), values).withSchema(signature));
          }
          LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
          if (leftEstimated.moreSelective(rightEstimated)) {
            // todo: should be done in BroadcastJoinProcessor
            if (values.size() < semiJoinThrehold && DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
              DimFilter filter = SemiJoinFactory.from(
                  rightJoinColumns, values.iterator(), GuavaUtils.indexOf(signature.columnNames, leftJoinColumns, true)
              );
              query1 = DimFilters.and((FilterSupport) query1, filter);
              LOG.debug("-- %s:%s (L) is applied as filter to %s (R)", leftAlias, leftEstimated, rightAlias);
            } else if (rightEstimated.gt(bloomFilterThreshold) && query0.hasFilters() && DataSources.isFilterableOn(query1, rightJoinColumns) && !element.isCrossJoin()) {
              RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
              BloomKFilter bloom = BloomFilterAggregator.build(resolver, leftJoinColumns, values.size(), values);
              query1 = DataSources.applyFilter(
                  query1, BloomDimFilter.of(rightJoinColumns, bloom), leftEstimated.selectivity, segmentWalker
              );
              LOG.debug("-- .. with bloom filter %s(%s) to %s (R)", leftAlias, BaseQuery.getDimFilter(query0), rightAlias);
            }
          }
          byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, true, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query1 = PostProcessingOperators.appendLocal(query1, processor);
          query1 = ((LastProjectionSupport) query1).withOutputColumns(null);
          estimation = Estimations.joinEstimation(element, leftEstimated, rightEstimated);
          selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
          queries.add(Estimations.propagate(query1, estimation, selectivity));
          continue;
        }
        if (rightBroadcast) {
          Map<String, Object> context0 = Estimations.propagate(context, leftEstimated);
          Map<String, Object> context1 = Estimations.propagate(context, rightEstimated);
          Query query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context0);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context1);
          LOG.debug("-- %s:%s (R) will be broadcasted to %s (L)", rightAlias, rightEstimated, leftAlias);
          RowSignature signature = Queries.relaySchema(query1, segmentWalker);
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query1 instanceof MaterializedQuery)) {
            segmentWalker.register(right, MaterializedQuery.of(query1, sequence.columns(), values).withSchema(signature));
          }
          LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          if (rightEstimated.moreSelective(leftEstimated)) {
            // todo: should be done in BroadcastJoinProcessor
            if (values.size() < semiJoinThrehold && DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
              DimFilter filter = SemiJoinFactory.from(
                  leftJoinColumns, values.iterator(), GuavaUtils.indexOf(signature.columnNames, rightJoinColumns, true)
              );
              query0 = DimFilters.and((FilterSupport) query0, filter);
              LOG.debug("-- %s:%s (R) is applied as filter to %s (L)", rightAlias, rightEstimated, leftAlias);
            } else if (leftEstimated.gt(bloomFilterThreshold) && query1.hasFilters() && DataSources.isFilterableOn(query0, leftJoinColumns)) {
              RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
              BloomKFilter bloom = BloomFilterAggregator.build(resolver, rightJoinColumns, values.size(), values);
              query0 = DataSources.applyFilter(
                  query0, BloomDimFilter.of(leftJoinColumns, bloom), rightEstimated.selectivity, segmentWalker
              );
              LOG.debug("-- .. with bloom filter %s(%s) to %s (L)", rightAlias, BaseQuery.getDimFilter(query1), leftAlias);
            }
          }
          byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, false, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes
          );
          query0 = PostProcessingOperators.appendLocal(query0, processor);
          query0 = ((LastProjectionSupport) query0).withOutputColumns(null);
          estimation = Estimations.joinEstimation(element, leftEstimated, rightEstimated);
          selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
          queries.add(Estimations.propagate(query0, estimation, selectivity));
          continue;
        }
      }

      // try hash-join
      boolean leftHashing = i == 0 && joinType.isRightDrivable() && leftEstimated.lte(hashThreshold);
      boolean rightHashing = joinType.isLeftDrivable() && rightEstimated.lte(hashThreshold);
      if (leftHashing && rightHashing) {
        if (Estimation.delta(leftEstimated, rightEstimated) < Estimation.max(leftEstimated, rightEstimated) * 0.1f) {
          boolean ldn = DataSources.isDataNodeSourced(left);
          boolean rdn = DataSources.isDataNodeSourced(right);
          if (!ldn && rdn) {
            leftHashing = false;
          } else if (ldn && !rdn) {
            rightHashing = false;
          }
        }
        if (rightHashing && leftEstimated.gt(rightEstimated)) {
          leftHashing = false;
        } else if (leftHashing) {
          rightHashing = false;
        }
      }
      if (i == 0) {
        List<String> sortOn = leftHashing || rightHashing ? null : leftJoinColumns;
        LOG.debug("-- %s:%s (L) (%s)", leftAlias, leftEstimated, leftHashing ? "hash" : sortOn != null ? "sort" : "-");
        Map<String, Object> propagated = Estimations.propagate(context, leftEstimated);
        Query query = JoinElement.toQuery(segmentWalker, left, sortOn, segmentSpec, propagated);
        if (leftHashing) {
          query = query.withOverriddenContext(HASHING, true);
        }

        if (leftHashing && element.isInnerJoin()
            && leftEstimated.lte(semiJoinThrehold)
            && (query instanceof MaterializedQuery || DataSources.isDataNodeSourced(query))
            && DataSources.isFilterableOn(right, rightJoinColumns)) {
          List<String> outputColumns = query.estimatedOutputColumns();
          if (outputColumns != null) {
            Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query, segmentWalker);
            List<Object[]> values = Sequences.toList(sequence);
            Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, leftJoinColumns));
            right = DataSources.applyFilter(
                right, SemiJoinFactory.from(rightJoinColumns, keys.iterator()), leftEstimated.selectivity, segmentWalker
            );
            LOG.debug("-- %s:%s (L) (hash) is applied as filter to %s (R)", leftAlias, leftEstimated, rightAlias);
            MaterializedQuery materialized = MaterializedQuery.of(query, sequence.columns(), values);
            if (!(query instanceof MaterializedQuery)) {
              segmentWalker.register(left, materialized);
            }
            query = materialized;
          }
        }
        queries.add(query);
      }
      List<String> sortOn = leftHashing || rightHashing ? null : rightJoinColumns;
      LOG.debug(
          "-- %s:%s (R) (%s)", rightAlias, rightEstimated.estimated, rightHashing ? "hash" : sortOn != null ? "sort" : "-"
      );
      Map<String, Object> propagated = Estimations.propagate(context, rightEstimated);
      Query query = JoinElement.toQuery(segmentWalker, right, sortOn, segmentSpec, propagated);
      if (rightHashing) {
        query = query.withOverriddenContext(HASHING, true);
      }

      left = QueryDataSource.of(GuavaUtils.lastOf(queries));
      if (rightHashing && element.isInnerJoin()
          && rightEstimated.lte(semiJoinThrehold)
          && (query instanceof MaterializedQuery || DataSources.isDataNodeSourced(query))
          && DataSources.isFilterableOn(left, leftJoinColumns)) {
        List<String> outputColumns = query.estimatedOutputColumns();
        if (outputColumns != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, rightJoinColumns));
          left = DataSources.applyFilter(
              left, SemiJoinFactory.from(leftJoinColumns, keys.iterator()), rightEstimated.selectivity, segmentWalker
          );
          LOG.debug("-- %s:%s (R) (hash) is applied as filter to %s (L)", rightAlias, rightEstimated, leftAlias);
          MaterializedQuery materialized = MaterializedQuery.of(query, sequence.columns(), values);
          if (!(query instanceof MaterializedQuery)) {
            segmentWalker.register(right, materialized);
          }
          GuavaUtils.setLastOf(queries, ((QueryDataSource) left).getQuery());   // overwrite
          query = materialized;
        }
      }
      queries.add(query);

      if (queries.get(i) instanceof MaterializedQuery || queries.get(i + 1) instanceof MaterializedQuery) {
        estimation = Estimations.joinEstimation(element, leftEstimated, rightEstimated);
        selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
        continue;
      }

      boolean leftBloomed = false;
      boolean rightBloomed = false;
      // try bloom filter
      if (!element.isCrossJoin()) {
        Query query0 = queries.get(i);
        Query query1 = queries.get(i + 1);
        if (joinType.isLeftDrivable() && rightEstimated.gt(bloomFilterThreshold) && leftEstimated.moreSelective(rightEstimated)) {
          // left to right
          Factory bloom = bloom(query0, leftJoinColumns, leftEstimated.estimated, query1, rightJoinColumns);
          if (bloom != null) {
            LOG.debug("-- .. with bloom filter %s (L) to %s:%s (R)", bloom.getBloomSource(), rightAlias, rightEstimated);
            queries.set(i + 1, DataSources.applyFilter(query1, bloom, leftEstimated.selectivity, rightJoinColumns, segmentWalker));
            rightBloomed = true;
          }
        }
        if (joinType.isRightDrivable() && leftEstimated.gt(bloomFilterThreshold) && rightEstimated.moreSelective(leftEstimated)) {
          // right to left
          Factory bloom = bloom(query1, rightJoinColumns, rightEstimated.estimated, query0, leftJoinColumns);
          if (bloom != null) {
            LOG.debug("-- .. with bloom filter %s (R) to %s:%s (L)", bloom.getBloomSource(), leftAlias, leftEstimated);
            queries.set(i, DataSources.applyFilter(query0, bloom, rightEstimated.selectivity, leftJoinColumns, segmentWalker));
            leftBloomed = true;
          }
        }
      }
      if (element.isInnerJoin()) {
        Query query0 = queries.get(i);
        Query query1 = queries.get(i + 1);
        if (!leftBloomed &&
            leftEstimated.lt(forcedFilterTinyThreshold) && rightEstimated.gte(forcedFilterHugeThreshold) &&
            DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
          List<String> outputColumns = query0.estimatedOutputColumns();
          if (outputColumns != null) {
            Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query0, segmentWalker);
            List<Object[]> values = Sequences.toList(sequence);
            DimFilter filter = new ForcedFilter(rightJoinColumns, values, GuavaUtils.indexOf(outputColumns, leftJoinColumns));
            LOG.debug("-- .. with forced filter from %s (L) to %s (R) + %s", leftAlias, rightAlias, filter);
            MaterializedQuery materialized = MaterializedQuery.of(query0, sequence.columns(), values);
            queries.set(i, segmentWalker.register(left, materialized));
            queries.set(i + 1, DimFilters.and((FilterSupport) query1, filter));
          }
        }
        if (!rightBloomed &&
            leftEstimated.gte(forcedFilterHugeThreshold) && rightEstimated.lt(forcedFilterTinyThreshold) &&
            DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
          List<String> outputColumns = query1.estimatedOutputColumns();
          if (outputColumns != null) {
            Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query1, segmentWalker);
            List<Object[]> values = Sequences.toList(sequence);
            DimFilter filter = new ForcedFilter(leftJoinColumns, values, GuavaUtils.indexOf(outputColumns, rightJoinColumns));
            LOG.debug("-- .. with forced filter from %s (R) to %s (L) + %s", rightAlias, leftAlias, filter);
            MaterializedQuery materialized = MaterializedQuery.of(query1, sequence.columns(), values);
            queries.set(i, DimFilters.and((FilterSupport) query0, filter));
            queries.set(i + 1, query = segmentWalker.register(right, materialized));
          }
        }
      }
      estimation = Estimations.joinEstimation(element, leftEstimated, rightEstimated);
      selectivity *= Math.min(leftEstimated.selectivity, rightEstimated.selectivity);
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

    Map<String, Object> joinContext = Estimations.propagate(context, estimation, selectivity);
    JoinHolder query = new JoinHolder(
        StringUtils.concat("+", aliases), elements, queries, timeColumn, outputAlias, outputColumns, limit, joinContext
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


  // todo convert back to JoinQuery and rewrite
  public static List<Query> changeHashing(
      List<JoinElement> elements,
      List<Query> queries,
      int ix,
      QuerySegmentWalker segmentWalker
  )
  {
    Query<?> query0 = queries.get(ix);
    if (JoinQuery.isHashing(query0)) {
      return queries;
    }
    int current = JoinQuery.getCardinality(query0);
    int iy = Iterables.indexOf(queries, q -> JoinQuery.isHashing(q) && JoinQuery.getCardinality(q) > current);
    if (iy < 0 || Math.abs(ix - iy) != 1 || !elements.get(Math.min(ix, iy)).isInnerJoin()) {
      return queries;
    }
    JoinElement element = elements.get(Math.min(ix, iy));
    Query<?> query1 = queries.get(iy);
    LOG.debug(
        "--- reassigned 'hashing' to %s:%d (was %s:%d)", query0.alias(), current, query1.alias(), JoinQuery.getCardinality(query1)
    );
    int semiJoinThrehold = segmentWalker.getConfig().getSemiJoinThreshold(query0);
    int cardinality = JoinQuery.getCardinality(query0);
    if (semiJoinThrehold > 0 && cardinality < semiJoinThrehold) {
      List<String> joinColumn0 = ix < iy ? element.getLeftJoinColumns() : element.getRightJoinColumns();
      List<String> joinColumn1 = ix < iy ? element.getRightJoinColumns() : element.getLeftJoinColumns();
      if (DataSources.isDataNodeSourced(query0) && DataSources.isFilterableOn(query1, joinColumn1)) {
        List<String> outputColumns = query0.estimatedOutputColumns();
        if (outputColumns != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query0, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          query0 = MaterializedQuery.of(query0, sequence.columns(), values);
          Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, joinColumn0));
          query1 = DataSources.applyFilter(query1, SemiJoinFactory.from(joinColumn1, keys.iterator()), -1, segmentWalker);
          LOG.debug("--- %s:%d (hash) is applied as filter to %s", query0.alias(), values.size(), query1.alias());
        }
      }
    }
    queries.set(ix, query0.withOverriddenContext(JoinQuery.HASHING, true));
    queries.set(iy, query1.withOverriddenContext(JoinQuery.HASHING, null));
    return queries;
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
    return filter == null ? null : DimFilters.rewrite(filter, f -> f instanceof FilterFactory ? null : f);
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

    if (!Objects.equals(elements, that.elements)) {
      return false;
    }
    String left = elements.get(0).getLeftAlias();
    if (!Objects.equals(dataSources.get(left), that.dataSources.get(left))) {
      return false;
    }
    for (int i = 0; i < elements.size(); i++) {
      String right = elements.get(i).getRightAlias();
      if (!Objects.equals(dataSources.get(right), that.dataSources.get(right))) {
        return false;
      }
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
      implements ArrayOutputSupport<Object[]>, LastProjectionSupport<Object[]>
  {
    private final String alias;   // just for debug
    private final List<JoinElement> elements;
    private final List<String> outputAlias;
    private final List<String> outputColumns;

    private final String timeColumnName;

    private final Execs.SettableFuture<List<List<OrderByColumnSpec>>> future = new Execs.SettableFuture<>();

    public JoinHolder(
        String alias,
        List<JoinElement> elements,
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
      this.elements = elements;
      this.outputAlias = outputAlias;
      this.outputColumns = outputColumns;
      this.timeColumnName = Preconditions.checkNotNull(timeColumnName, "'timeColumnName' is null");
    }

    public String getAlias()
    {
      return alias;
    }

    public List<JoinElement> getElements()
    {
      return elements;
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
          elements,
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
          elements,
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
