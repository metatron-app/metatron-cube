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
import io.druid.query.select.StreamQuery;
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

    final List<Query> queries = Lists.newArrayList();

    Estimation estimation = null;

    for (int i = 0; i < elements.size(); i++) {
      final JoinElement element = elements.get(i);
      final JoinType joinType = element.getJoinType();
      final List<String> leftJoinColumns = element.getLeftJoinColumns();
      final List<String> rightJoinColumns = element.getRightJoinColumns();

      final String leftAlias = element.getLeftAlias();
      final String rightAlias = element.getRightAlias();

      DataSource left = i == 0 ? dataSources.get(leftAlias) : null;
      DataSource right = dataSources.get(rightAlias);

      Estimation leftEstimated = i > 0 ? estimation : Estimations.estimate(left, segmentSpec, context, segmentWalker);
      Estimation rightEstimated = Estimations.estimate(right, segmentSpec, context, segmentWalker);

      String typeString = element.getJoinTypeString();
      LOG.debug("-> %s (%s:%s + %s:%s)", typeString, leftAlias, leftEstimated, rightAlias, rightEstimated);

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
              LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
            }
            values = Lists.transform(values, GuavaUtils.mapper(rightColumns, rightJoinColumns));
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                leftJoinColumns, values, JoinElement.allowDuplication(left, leftJoinColumns), outputColumns
            );
            DataSource filtered = DataSources.applyFilter(left, converted.lhs, rightEstimated.selectivity, segmentWalker);
            if (converted.rhs == null) {
              filtered = DataSources.applyProjection(filtered, outputColumns);
            }
            LOG.debug("-- %s:%s (R) is merged into %s (L) as filter on %s", rightAlias, rightEstimated, leftAlias, leftJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, propagated);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            queries.add(Estimations.propagate(query, estimation = leftEstimated.multiply(rightEstimated.update(values.size()))));
            out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
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
              LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
            }
            values = Lists.transform(values, GuavaUtils.mapper(leftColumns, leftJoinColumns));
            Pair<DimFilter, RowExploder> converted = SemiJoinFactory.extract(
                rightJoinColumns, values, JoinElement.allowDuplication(right, rightJoinColumns), outputColumns
            );
            DataSource filtered = DataSources.applyFilter(right, converted.lhs, leftEstimated.selectivity, segmentWalker);
            if (converted.rhs == null) {
              filtered = DataSources.applyProjection(filtered, outputColumns);
            }
            LOG.debug("-- %s:%s (L) is merged into %s (R) as filter on %s", leftAlias, leftEstimated, rightAlias, rightJoinColumns);
            Query query = JoinElement.toQuery(segmentWalker, filtered, segmentSpec, propagated);
            if (converted.rhs != null) {
              query = PostProcessingOperators.appendLocal(query, converted.rhs);
            }
            queries.add(Estimations.propagate(query, estimation = rightEstimated.multiply(leftEstimated.update(values.size()))));
            out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
            continue;
          }
        }
      }

      // try broadcast join
      if (i == 0 && !element.isCrossJoin() && broadcastThreshold > 0) {
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
          Map<String, Object> context0 = Estimations.propagate(context, leftEstimated);
          Map<String, Object> context1 = Estimations.propagate(context, rightEstimated);
          Query.ArrayOutputSupport query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context0);
          Query query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context1);
          LOG.debug("-- %s:%s (L) will be broadcasted to %s (R)", leftAlias, leftEstimated, rightAlias);
          RowSignature signature = DataSources.schema(left, query0, segmentWalker);
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query0, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query0 instanceof MaterializedQuery)) {
            segmentWalker.register(left, MaterializedQuery.of(query0, sequence.columns(), values).withSchema(signature));
            LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
          }
          boolean applyfilter = false;
          if (element.isInnerJoin() && leftEstimated.moreSelective(rightEstimated)) {
            if (leftEstimated.lt(rightEstimated) && DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
              applyfilter = true;
              LOG.debug("-- %s:%s (L) will be applied as local filter to %s (R)", leftAlias, leftEstimated, rightAlias);
            } else if (rightEstimated.gt(bloomFilterThreshold) && query0.hasFilters() && DataSources.isFilterableOn(query1, rightJoinColumns)) {
              RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
              BloomKFilter bloom = BloomFilterAggregator.build(resolver, leftJoinColumns, values.size(), values);
              query1 = DataSources.applyFilter(
                  query1, BloomDimFilter.of(rightJoinColumns, bloom), leftEstimated.selectivity, segmentWalker
              );
              LOG.debug("-- .. with bloom filter %s(%s) to %s (R)", leftAlias, BaseQuery.getDimFilter(query0), rightAlias);
            }
            rightEstimated.update(leftEstimated.selectivity);
          }
          byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, true, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes, applyfilter
          );
          query1 = PostProcessingOperators.appendLocal(query1, processor);
          query1 = ((LastProjectionSupport) query1).withOutputColumns(null);
          queries.add(Estimations.propagate(query1, estimation = Estimations.join(element, leftEstimated.update(values.size()), rightEstimated)));
          out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
          continue;
        }
        if (rightBroadcast) {
          Map<String, Object> context0 = Estimations.propagate(context, leftEstimated);
          Map<String, Object> context1 = Estimations.propagate(context, rightEstimated);
          Query query0 = JoinElement.toQuery(segmentWalker, left, segmentSpec, context0);
          Query.ArrayOutputSupport query1 = JoinElement.toQuery(segmentWalker, right, segmentSpec, context1);
          LOG.debug("-- %s:%s (R) will be broadcasted to %s (L)", rightAlias, rightEstimated, leftAlias);
          RowSignature signature = DataSources.schema(right, query1, segmentWalker);
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun(query1, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          if (!(query1 instanceof MaterializedQuery)) {
            segmentWalker.register(right, MaterializedQuery.of(query1, sequence.columns(), values).withSchema(signature));
            LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          }
          boolean applyfilter = false;
          if (element.isInnerJoin() && rightEstimated.moreSelective(leftEstimated)) {
            if (rightEstimated.lt(leftEstimated) && DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
              applyfilter = true;
              LOG.debug("-- %s:%s (R) will be applied as local filter to %s (L)", rightAlias, rightEstimated, leftAlias);
            } else if (leftEstimated.gt(bloomFilterThreshold) && query1.hasFilters() && DataSources.isFilterableOn(query0, leftJoinColumns)) {
              RowResolver resolver = RowResolver.of(signature, ImmutableList.of());
              BloomKFilter bloom = BloomFilterAggregator.build(resolver, rightJoinColumns, values.size(), values);
              query0 = DataSources.applyFilter(
                  query0, BloomDimFilter.of(leftJoinColumns, bloom), rightEstimated.selectivity, segmentWalker
              );
              LOG.debug("-- .. with bloom filter %s(%s) to %s (L)", rightAlias, BaseQuery.getDimFilter(query1), leftAlias);
            }
            leftEstimated.update(rightEstimated.selectivity);
          }
          byte[] bytes = ObjectMappers.writeBytes(mapper, BulkSequence.fromArray(Sequences.simple(values), signature));
          BroadcastJoinProcessor processor = new BroadcastJoinProcessor(
              mapper, config, element, false, signature, prefixAlias, asMap, outputAlias, outputColumns, maxOutputRow, bytes, applyfilter
          );
          query0 = PostProcessingOperators.appendLocal(query0, processor);
          query0 = ((LastProjectionSupport) query0).withOutputColumns(null);
          queries.add(Estimations.propagate(query0, estimation = Estimations.join(element, leftEstimated, rightEstimated.update(values.size()))));
          out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
          continue;
        }
      }

      // try hash-join
      boolean leftHashing = i == 0 && joinType.isRightDrivable() && leftEstimated.lte(hashThreshold);
      boolean rightHashing = joinType.isLeftDrivable() && rightEstimated.lte(hashThreshold);
      if (leftHashing && rightHashing) {
        if (leftExpensive(left, leftEstimated, right, rightEstimated)) {
          leftHashing = false;
        } else {
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
            LOG.debug("-- %s (L) is materialized (%d rows)", leftAlias, values.size());
            Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, leftJoinColumns));
            right = DataSources.applyFilter(
                right, SemiJoinFactory.from(rightJoinColumns, keys.iterator()), leftEstimated.selectivity, segmentWalker
            );
            LOG.debug("-- %s:%s (L) (hash) is applied as filter to %s (R)", leftAlias, leftEstimated, rightAlias);
            MaterializedQuery materialized = MaterializedQuery.of(query, sequence.columns(), values);
            if (!(query instanceof MaterializedQuery)) {
              segmentWalker.register(left, materialized);
              leftEstimated.update(values.size());
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
          LOG.debug("-- %s (R) is materialized (%d rows)", rightAlias, values.size());
          Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, rightJoinColumns));
          left = DataSources.applyFilter(
              left, SemiJoinFactory.from(leftJoinColumns, keys.iterator()), rightEstimated.selectivity, segmentWalker
          );
          LOG.debug("-- %s:%s (R) (hash) is applied as filter to %s (L)", rightAlias, rightEstimated, leftAlias);
          MaterializedQuery materialized = MaterializedQuery.of(query, sequence.columns(), values);
          if (!(query instanceof MaterializedQuery)) {
            segmentWalker.register(right, materialized);
            rightEstimated.update(values.size());
          }
          GuavaUtils.setLastOf(queries, ((QueryDataSource) left).getQuery());   // overwrite
          query = materialized;
        }
      }
      queries.add(query);

      if (queries.get(i) instanceof MaterializedQuery || queries.get(i + 1) instanceof MaterializedQuery) {
        estimation = Estimations.join(element, leftEstimated, rightEstimated);
        out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
        continue;
      }

      boolean leftBloomed = false;
      boolean rightBloomed = false;
      // try bloom filter
      Query query0 = queries.get(i);
      Query query1 = queries.get(i + 1);
      if (joinType.isLeftDrivable() && rightEstimated.gt(bloomFilterThreshold) && leftEstimated.moreSelective(rightEstimated)) {
        // left to right
        Factory bloom = bloom(query0, leftJoinColumns, leftEstimated.estimated, query1, rightJoinColumns);
        if (bloom != null) {
          LOG.debug("-- .. with bloom filter %s (L) to %s:%s (R)", bloom.getBloomSource(), rightAlias, rightEstimated);
          query1 = DataSources.applyFilter(query1, bloom, leftEstimated.degrade(), rightJoinColumns, segmentWalker);
          queries.set(i + 1, query1);
          filterMerged(element, queries, i + 1, i, segmentWalker);
          rightEstimated = Estimation.from(query1);
          rightBloomed = true;
        }
      } else if (joinType.isRightDrivable() && leftEstimated.gt(bloomFilterThreshold) && rightEstimated.moreSelective(leftEstimated)) {
        // right to left
        Factory bloom = bloom(query1, rightJoinColumns, rightEstimated.estimated, query0, leftJoinColumns);
        if (bloom != null) {
          LOG.debug("-- .. with bloom filter %s (R) to %s:%s (L)", bloom.getBloomSource(), leftAlias, leftEstimated);
          query0 = DataSources.applyFilter(query0, bloom, rightEstimated.degrade(), leftJoinColumns, segmentWalker);
          queries.set(i, query0);
          filterMerged(element, queries, i, i + 1, segmentWalker);
          leftEstimated = Estimation.from(query0);
          leftBloomed = true;
        }
      }
      if (!leftBloomed && joinType.isLeftDrivable() &&
          (leftHashing || leftEstimated.lte(forcedFilterTinyThreshold)) &&
          rightEstimated.gte(forcedFilterHugeThreshold) &&
          DataSources.isDataLocalFilterable(query1, rightJoinColumns)) {
        List<String> outputColumns = query0.estimatedOutputColumns();
        if (outputColumns != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query0, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          DimFilter filter = new ForcedFilter(rightJoinColumns, values, GuavaUtils.indexOf(outputColumns, leftJoinColumns));
          LOG.debug("-- .. with forced filter from %s (L) to %s (R) + %s", leftAlias, rightAlias, filter);
          MaterializedQuery materialized = MaterializedQuery.of(query0, sequence.columns(), values);
          queries.set(i, segmentWalker.register(left, materialized));
          queries.set(i + 1, Estimations.mergeSelectivity(DimFilters.and(query1, filter), leftEstimated.degrade()));
          rightEstimated = Estimation.from(queries.get(i + 1));
        }
      }
      if (!rightBloomed && joinType.isRightDrivable() &&
          leftEstimated.gte(forcedFilterHugeThreshold) &&
          (rightHashing || rightEstimated.lte(forcedFilterTinyThreshold)) &&
          DataSources.isDataLocalFilterable(query0, leftJoinColumns)) {
        List<String> outputColumns = query1.estimatedOutputColumns();
        if (outputColumns != null) {
          Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query1, segmentWalker);
          List<Object[]> values = Sequences.toList(sequence);
          DimFilter filter = new ForcedFilter(leftJoinColumns, values, GuavaUtils.indexOf(outputColumns, rightJoinColumns));
          LOG.debug("-- .. with forced filter from %s (R) to %s (L) + %s", rightAlias, leftAlias, filter);
          MaterializedQuery materialized = MaterializedQuery.of(query1, sequence.columns(), values);
          queries.set(i, Estimations.mergeSelectivity(DimFilters.and(query0, filter), rightEstimated.degrade()));
          queries.set(i + 1, query = segmentWalker.register(right, materialized));
          leftEstimated = Estimation.from(queries.get(i));
        }
      }
      estimation = Estimations.join(element, leftEstimated, rightEstimated);
      out(typeString, leftAlias, rightAlias, leftEstimated, rightEstimated, estimation);
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

    String alias = StringUtils.concat("+", aliases);
    Map<String, Object> joinContext = Estimations.propagate(context, estimation);
    JoinHolder query = new JoinHolder(
        alias, elements, GuavaUtils.cast(queries), timeColumn, outputAlias, outputColumns, limit, joinContext
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

  // todo convert back to JoinQuery and rewrite
  static List<Query> filterMerged(JoinElement element, List<Query> queries, int ix, int iy, QuerySegmentWalker walker)
  {
    if (!element.isInnerJoin()) {
      return queries;   // todo
    }
    Query<Object[]> query0 = queries.get(ix);
    Query<Object[]> query1 = queries.get(iy);
    if (JoinQuery.isHashing(query0)) {
      return queries;
    }

    int rc0 = Estimation.getRowCount(query0);
    int rc1 = Estimation.getRowCount(query1);
    if (rc0 < 0 || rc1 < 0 || rc0 >= rc1) {
      return queries;
    }

    QueryConfig config = walker.getConfig();
    if (rc0 > config.getHashJoinThreshold(query0)) {
      return queries;
    }

    if (JoinQuery.isHashing(query1)) {
      LOG.debug("--- reassigning 'hashing' to %s:%d (was %s:%d)", query0.alias(), rc0, query1.alias(), rc1);
    } else {
      LOG.debug("--- assigning 'hashing' to %s:%d.. overriding sort-merge join", query0.alias(), rc0);
    }
    List<String> joinColumn0 = ix < iy ? element.getLeftJoinColumns() : element.getRightJoinColumns();
    List<String> joinColumn1 = ix < iy ? element.getRightJoinColumns() : element.getLeftJoinColumns();

    query0 = disableSort(query0, joinColumn0);
    query1 = disableSort(query1, joinColumn1);

    int semiJoinThrehold = config.getSemiJoinThreshold(query0);
    if (semiJoinThrehold > 0 && rc0 <= semiJoinThrehold &&
        DataSources.isDataNodeSourced(query0) && DataSources.isFilterableOn(query1, joinColumn1)) {
      DimFilter filter = removeTrivials(BaseQuery.getDimFilter(query0), joinColumn0);
      List<String> outputColumns = query0.estimatedOutputColumns();
      if (filter != null && outputColumns != null) {
        Sequence<Object[]> sequence = QueryRunners.resolveAndRun((ArrayOutputSupport) query0, walker);
        List<Object[]> values = Sequences.toList(sequence);
        query0 = MaterializedQuery.of(query0, sequence.columns(), values);
        Iterable<Object[]> keys = Iterables.transform(values, GuavaUtils.mapper(outputColumns, joinColumn0));
        query1 = DataSources.applyFilter(query1, SemiJoinFactory.from(joinColumn1, keys.iterator()), -1, walker);
        LOG.debug("--- %s:%d (hash) is applied as filter to %s", query0.alias(), values.size(), query1.alias());
      }
    }
    queries.set(ix, query0.withOverriddenContext(JoinQuery.HASHING, true));
    queries.set(iy, query1.withOverriddenContext(JoinQuery.HASHING, null));
    return queries;
  }

  private static Query<Object[]> disableSort(Query<Object[]> query, List<String> joinKey)
  {
    if (!JoinQuery.isSorting(query)) {
      return query;
    }
    if (query instanceof StreamQuery) {
      StreamQuery stream = (StreamQuery) query;
      if (OrderByColumnSpec.ascending(joinKey).equals(stream.getOrderingSpecs())) {
        query = stream.withOrderingSpec(null);
      }
    } else if (query instanceof OrderingSupport) {
      OrderingSupport ordering = (OrderingSupport) query;
      if (OrderByColumnSpec.ascending(joinKey).equals(ordering.getResultOrdering())) {
        query = ordering.withResultOrdering(null);    // todo: seemed not working
      }
    }
    return query.withOverriddenContext(JoinQuery.SORTING, null);
  }

  private static Factory bloom(
      Query source, List<String> sourceJoinOn, long sourceCardinality,
      Query target, List<String> targetJoinOn
  )
  {
    DimFilter filter = removeTrivials(removeFactory(BaseQuery.getDimFilter(source)), sourceJoinOn);
    if (filter == null || !DataSources.isDataNodeSourced(source)) {
      return null;
    }
    if (source.getContextValue(Query.LOCAL_POST_PROCESSING) != null) {
      Query view = source.withOverriddenContext(Query.LOCAL_POST_PROCESSING, null);
      List<String> outputColumns = view.estimatedOutputColumns();
      if (outputColumns == null || !outputColumns.containsAll(sourceJoinOn)) {
        return null;
      }
    }
    List<DimensionSpec> extracted = DataSources.findFilterableOn(target, targetJoinOn, q -> !Queries.isNestedQuery(q));
    if (extracted != null) {
      // bloom factory will be evaluated in UnionQueryRunner
      ViewDataSource view = BaseQuery.asView(source, filter, sourceJoinOn);
      return Factory.fields(extracted, view, Ints.checkedCast(sourceCardinality));
    }
    return null;
  }

  private static DimFilter removeFactory(DimFilter filter)
  {
    return filter == null ? null : DimFilters.rewrite(filter, f -> f instanceof FilterFactory ? null : f);
  }

  private static DimFilter removeTrivials(DimFilter filter, List<String> joinColumns)
  {
    if (filter == null) {
      return null;
    }
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

    public JoinElement getElement(int alias)
    {
      return alias == 0 ? elements.get(0) : elements.get(alias - 1);
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
