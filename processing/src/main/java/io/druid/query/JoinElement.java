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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query.ArrayOutputSupport;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SemiJoinFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.druid.query.JoinQuery.SORTING;

/**
 */
public class JoinElement
{
  private static final Logger LOG = new Logger(JoinQuery.class);

  public static JoinElement inner(String expression)
  {
    return new JoinElement(JoinType.INNER, expression);
  }

  public static JoinElement of(JoinType type, String expression)
  {
    return new JoinElement(type, expression);
  }

  public static List<String> getAliases(List<JoinElement> elements)
  {
    List<String> aliases = Lists.newArrayList();
    aliases.add(elements.get(0).getLeftAlias());
    for (JoinElement element : elements) {
      aliases.add(element.getRightAlias());
    }
    return aliases;
  }

  private final JoinType joinType;

  private final String leftAlias;
  private final List<String> leftJoinColumns;

  private final String rightAlias;
  private final List<String> rightJoinColumns;

  private final String expression;

  @JsonCreator
  public JoinElement(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("leftAlias") String leftAlias,
      @JsonProperty("leftJoinColumns") List<String> leftJoinColumns,
      @JsonProperty("rightAlias") String rightAlias,
      @JsonProperty("rightJoinColumns") List<String> rightJoinColumns,
      @JsonProperty("expression") String expression
  )
  {
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.expression = expression;
    if (expression == null) {
      this.leftAlias = leftAlias;   // can be null.. ignored when i > 0
      this.leftJoinColumns = Preconditions.checkNotNull(leftJoinColumns);
      this.rightAlias = Preconditions.checkNotNull(rightAlias);
      this.rightJoinColumns = Preconditions.checkNotNull(rightJoinColumns);
      Preconditions.checkArgument(leftJoinColumns.size() == rightJoinColumns.size());
    } else {
      this.leftAlias = null;
      this.leftJoinColumns = null;
      this.rightAlias = null;
      this.rightJoinColumns = null;
    }
  }

  public JoinElement(
      JoinType joinType,
      String leftAlias,
      List<String> leftJoinColumns,
      String rightAlias,
      List<String> rightJoinColumns
  )
  {
    this(joinType, leftAlias, leftJoinColumns, rightAlias, rightJoinColumns, null);
  }

  public JoinElement(JoinType joinType, String expression)
  {
    this(joinType, null, null, null, null, expression);
  }

  public JoinElement rewrite(Set<String> dataSources)
  {
    if (expression == null) {
      return this;
    }
    String leftAlias = null;
    List<String> leftJoinColumns = Lists.newArrayList();
    String rightAlias = null;
    List<String> rightJoinColumns = Lists.newArrayList();

    for (String clause : expression.split("&&")) {
      String[] split = clause.trim().split("=");
      Preconditions.checkArgument(split.length == 2);
      String part1 = split[0].trim();
      String part2 = split[1].trim();
      String alias0 = findAlias(part1, dataSources);
      String alias1 = findAlias(part2, dataSources);
      if (leftAlias == null || (leftAlias.equals(alias0) && rightAlias.equals(alias1))) {
        leftAlias = alias0;
        rightAlias = alias1;
        leftJoinColumns.add(part1.substring(alias0.length() + 1));
        rightJoinColumns.add(part2.substring(alias1.length() + 1));
      } else if (leftAlias.equals(alias1) && rightAlias.equals(alias0)) {
        leftJoinColumns.add(part2.substring(alias1.length() + 1));
        rightJoinColumns.add(part1.substring(alias0.length() + 1));
      } else {
        throw new IllegalArgumentException("invalid expression " + expression);
      }
    }
    return new JoinElement(joinType, leftAlias, leftJoinColumns, rightAlias, rightJoinColumns);
  }

  private String findAlias(String expression, Set<String> aliases)
  {
    if (aliases == null) {
      int index = expression.indexOf('.');
      if (index < 0) {
        throw new IllegalArgumentException("cannot find alias from " + expression);
      }
      if (expression.indexOf('.', index + 1) > 0) {
        throw new IllegalArgumentException("ambiguous alias.. contains more than one dot in " + expression);
      }
      return expression.substring(0, index);
    }
    String found = null;
    for (String alias : aliases) {
      if (expression.startsWith(alias + ".")) {
        if (found != null) {
          throw new IllegalArgumentException("ambiguous alias " + found + " and " + alias);
        }
        found = alias;
      }
    }
    return Preconditions.checkNotNull(found, "cannot find alias from %s", expression);
  }

  @JsonIgnore
  public String getJoinTypeString()
  {
    return isCrossJoin() ? "CROSS" : joinType.getName();
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @JsonProperty
  public String getLeftAlias()
  {
    return leftAlias;
  }

  @JsonProperty
  public List<String> getLeftJoinColumns()
  {
    return leftJoinColumns;
  }

  @JsonProperty
  public String getRightAlias()
  {
    return rightAlias;
  }

  @JsonProperty
  public List<String> getRightJoinColumns()
  {
    return rightJoinColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExpression()
  {
    return expression;
  }

  @JsonIgnore
  public List<String> getAliases()
  {
    return Arrays.asList(leftAlias, rightAlias);
  }

  @JsonIgnore
  public boolean isCrossJoin()
  {
    return leftJoinColumns.isEmpty();
  }

  @JsonIgnore
  public boolean isInnerJoin()
  {
    return joinType == JoinType.INNER && !leftJoinColumns.isEmpty();
  }

  public void earlyCheckMaxJoin(long leftEstimated, long rightEstimated, int maxResult)
  {
    if (maxResult <= 0) {
      return;
    }
    if (isCrossJoin() && leftEstimated > 0 && rightEstimated > 0 && leftEstimated * rightEstimated > maxResult) {
      throw _exceeding(maxResult, leftEstimated * rightEstimated);
    }
    if (joinType == JoinType.LO && leftEstimated > 0 && leftEstimated > maxResult) {
      throw _exceeding(maxResult, leftEstimated);
    }
    if (joinType == JoinType.RO && rightEstimated > 0 && rightEstimated > maxResult) {
      throw _exceeding(maxResult, rightEstimated);
    }
  }

  private ISE _exceeding(int maxResult, long estimated)
  {
    return new ISE("Exceeding maxOutputRow of %d in %s + %s (%d)", maxResult, leftAlias, rightAlias, estimated);
  }

  public boolean isLeftSemiJoinable(DataSource left, DataSource right, List<String> outputColumns)
  {
    if (!GuavaUtils.isNullOrEmpty(outputColumns) && joinType.isLeftDrivable()) {
      List<String> rightOutputColumns = DataSources.getOutputColumns(right);
      if (rightOutputColumns == null || GuavaUtils.containsAny(outputColumns, rightOutputColumns)) {
        return false;
      }
      return DataSources.isFilterableOn(left, leftJoinColumns);
    }
    return false;
  }

  public boolean isRightSemiJoinable(DataSource left, DataSource right, List<String> outputColumns)
  {
    if (!GuavaUtils.isNullOrEmpty(outputColumns) && joinType.isRightDrivable()) {
      List<String> leftOutputColumns = DataSources.getOutputColumns(left);
      if (leftOutputColumns == null || GuavaUtils.containsAny(outputColumns, leftOutputColumns)) {
        return false;
      }
      return DataSources.isFilterableOn(right, rightJoinColumns);
    }
    return false;
  }

  public static boolean allowDuplication(DataSource dataSource, List<String> keys)
  {
    return dataSource instanceof QueryDataSource && allowDuplication(((QueryDataSource) dataSource).getQuery(), keys);
  }

  public static boolean allowDuplication(Query<?> query, List<String> keys)
  {
    if (query instanceof Query.AggregationsSupport) {
      List<DimensionSpec> dimensions = BaseQuery.getDimensions(query);
      if (!dimensions.isEmpty() && DimensionSpecs.isAllDefault(dimensions)) {
        return Sets.newHashSet(DimensionSpecs.toOutputNames(dimensions)).containsAll(keys);
      }
    }
    return false;
  }

  public static ArrayOutputSupport toQuery(
      QuerySegmentWalker segmentWalker,
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context
  )
  {
    return toQuery(segmentWalker, dataSource, null, segmentSpec, context);
  }

  public static ArrayOutputSupport toQuery(
      QuerySegmentWalker segmentWalker,
      DataSource dataSource,
      List<String> sortColumns,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context
  )
  {
    Preconditions.checkNotNull(dataSource);
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      if (query instanceof JoinQuery.JoinHolder) {
        return ((JoinQuery.JoinHolder) query).toArrayJoin(sortColumns);  // keep array for output
      }
      if (!(query instanceof ArrayOutputSupport)) {
        throw new UnsupportedOperationException("todo: cannot resolve output column names on " + query.getType());
      }
      if (!GuavaUtils.isNullOrEmpty(sortColumns) && query instanceof Query.OrderingSupport) {
        if (DataSources.isFilterSupport(dataSource) && DataSources.isFilterableOn(dataSource, sortColumns)) {
          query = ((Query.OrderingSupport<?>) query).withResultOrdering(OrderByColumnSpec.ascending(sortColumns))
                                                    .withOverriddenContext(SORTING, true);
        }
      }
      return (ArrayOutputSupport) query;
    }
    if (dataSource instanceof TableDataSource) {
      dataSource = ViewDataSource.of(((TableDataSource) dataSource).getName());
    }
    if (dataSource instanceof ViewDataSource) {
      ViewDataSource view = (ViewDataSource) dataSource;
      StreamQuery query = new Druids.SelectQueryBuilder()
          .dataSource(view.getName())
          .intervals(segmentSpec)
          .filters(view.getFilter())
          .columns(view.getColumns())
          .virtualColumns(view.getVirtualColumns())
          .context(BaseQuery.copyContextForMeta(context))
          .streaming(sortColumns);
      if (GuavaUtils.isNullOrEmpty(query.getColumns())) {
        query = (StreamQuery) QueryUtils.resolve(query, segmentWalker);
      }
      return query;
    }
    throw new ISE("todo: cannot join on %s(%s)", dataSource, dataSource.getClass());
  }

  private static final int TRIVIAL_SIZE = 100;
  private static final int ACCEPTABLE_MAX_LIMIT = 10000;

  private static long[] applyLimit(Query<?> query, long[] estimated)
  {
    LimitSpec limitSpec = BaseQuery.getLimitSpec(query);
    if (limitSpec != null && limitSpec.hasLimit()) {
      if (estimated[0] > 0) {
        estimated[0] = Math.min(limitSpec.getLimit(), estimated[0]);
      } else if (limitSpec.getLimit() < ACCEPTABLE_MAX_LIMIT) {
        estimated[0] = limitSpec.getLimit();
      }
    }
    return estimated;
  }

  private static final float MIN_SAMPLING = 4000;

  public static long[] estimatedNumRows(
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context,
      QuerySegmentWalker segmentWalker
  )
  {
    long[] estimated = JoinQuery.estimatedCardinality(dataSource);
    if (estimated[0] != Queries.NOT_EVALUATED) {
      return estimated;
    }
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      LimitSpec limitSpec = BaseQuery.getLimitSpec(query);
      if (limitSpec != null && limitSpec.hasLimit() && limitSpec.getLimit() < TRIVIAL_SIZE) {
        return new long[] {limitSpec.getLimit(), limitSpec.getLimit()};
      }
      if (query.getDataSource() instanceof QueryDataSource) {
        if (query instanceof StreamQuery) {
          StreamQuery stream = (StreamQuery) query;
          // ignore simple projections
          estimated = estimatedNumRows(query.getDataSource(), segmentSpec, context, segmentWalker);
          if (estimated[0] > 0 && stream.getFilter() != null) {
            estimated[0] = Math.max(1, estimated[0] >>> 1);
          }
          return applyLimit(stream, estimated);
        } else if (query instanceof TimeseriesQuery && Granularities.isAll(query.getGranularity())) {
          return new long[]{1, 1};
        }
        return new long[]{Queries.NOT_EVALUATED, Queries.NOT_EVALUATED};  // see later
      }
      if (query instanceof TimeseriesQuery) {
        TimeseriesQuery timeseries = (TimeseriesQuery) query;
        return Queries.estimateCardinality(timeseries.withHavingSpec(null), segmentWalker);
      } else if (query instanceof GroupByQuery) {
        GroupByQuery groupBy = (GroupByQuery) query;
        long[] selectivity = Queries.estimateSelectivity(groupBy, segmentWalker);
        if (selectivity[0] <= TRIVIAL_SIZE) {
          return selectivity;
        }
        long start = System.currentTimeMillis();
        List<DimensionSpec> dimensions = groupBy.getDimensions();
        DimensionSamplingQuery sampling = groupBy.toSampling(Math.min(0.05f, MIN_SAMPLING / selectivity[1]));
        IntTagged<Object2IntSortedMap<?>> mapping = SemiJoinFactory.toMap(
            dimensions.size(), Sequences.toIterator(sampling.run(segmentWalker, null))
        );
        estimated[0] = Math.min(selectivity[0], estimateBySample(mapping, selectivity[0]));
//        estimated = Queries.estimateCardinality(groupBy.withHavingSpec(null), segmentWalker);
        if (selectivity[0] > 0 && selectivity[1] > 0) {
          estimated[1] = estimated[0] * selectivity[1] / selectivity[0];
        } else {
          estimated[1] = estimated[0];
        }
        if (groupBy.getHavingSpec() != null) {
          long threshold = segmentWalker.getJoinConfig().anyMinThreshold();
          if (threshold > 0 && estimated[0] > threshold << 1 && DimensionSpecs.isAllDefault(dimensions)) {
            DimFilter filter = SemiJoinFactory.toFilter(DimensionSpecs.toInputNames(dimensions), mapping.value());
            Query<Row> sampler = groupBy.prepend(filter)
                                        .withOverriddenContext(Query.GBY_LOCAL_SPLIT_CARDINALITY, -1)
                                        .withOverriddenContext("$skip", true);   // for test hook
            int size = SemiJoinFactory.sizeOf(filter);
            int passed = Sequences.size(QueryUtils.resolve(sampler, segmentWalker).run(segmentWalker, null));
            estimated[0] = Math.max(1, estimated[0] * passed / size);
            LOG.debug("--- 'having' selectivity by sampling: %f", (float) passed / size);
          } else {
            estimated[0] = Math.max(1, estimated[0] >>> 1);    // half
          }
        }
        LOG.debug(
            "--- %s is estimated to %d rows by sampling %d rows from %d rows in %,d msec",
            groupBy.getDataSource(), estimated[0], mapping.tag, selectivity[0], System.currentTimeMillis() - start
        );
        return applyLimit(query, estimated);
      } else if (query instanceof StreamQuery) {
        return applyLimit(query, Queries.estimateSelectivity(query, segmentWalker));
      }
      return new long[]{Queries.UNKNOWN, Queries.UNKNOWN};  // see later
    }
    return Queries.estimateSelectivity(
        dataSource,
        segmentSpec,
        null,
        BaseQuery.copyContextForMeta(context),
        segmentWalker
    );
  }

  private static long estimateBySample(IntTagged<Object2IntSortedMap<?>> mapping, long N)
  {
    final int n = mapping.tag;
    final Object2IntMap<?> samples = mapping.value;

    float q = (float) n / N;
    final float d = samples.size();

    float f1 = samples.object2IntEntrySet().stream().filter(e -> e.getIntValue() == 1).count();

//    final IntIterator counts = samples.values().iterator();
//    final Int2IntOpenHashMap fn = new Int2IntOpenHashMap();
//    while (counts.hasNext()) {
//      fn.addTo(counts.nextInt(), 1);
//    }
//    float f1 = fn.get(1);
//
//    float sum = 0;
//    float numerator = 0f;
//    float denominator = 0f;
//    for (Int2IntMap.Entry entry : fn.int2IntEntrySet()) {
//      int i = entry.getIntKey();
//      int fi = entry.getIntValue();
//      numerator += Math.pow(1 - q, i) * fi;
//      denominator += i * q * Math.pow(1 - q, i - 1) * fi;
//      sum += i * (i - 1) * fi;
//    }
//    double Dsh = d + f1 * numerator / denominator;
//
//    float f1n = 1 - f1 / n;
//    double Dcl = d + f1 * d * sum / (Math.pow(n, 2) - n - 1) / f1n / f1n;
//    double Dchar = d + f1 * (Math.sqrt(1 / q) - 1);
//    double Dchao = d + 0.5 * Math.pow(f1, 2) / (d - f1);
//    LOG.info("Dsh = %.2f, Dcl = %.2f, Dchar = %.2f, Dchao = %.2f", Dsh, Dcl, Dchar, Dchao);

    if (f1 == d) {
      return (long) (d * Math.sqrt(1 / q));   // Dchar
    } else {
      return (long) (d + 0.5 * Math.pow(f1, 2) / (d - f1));   // Dchao
    }
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

    JoinElement element = (JoinElement) o;

    if (joinType != element.joinType) {
      return false;
    }
    if (!Objects.equals(expression, element.expression)) {
      return false;
    }
    if (!Objects.equals(leftAlias, element.leftAlias)) {
      return false;
    }
    if (!Objects.equals(leftJoinColumns, element.leftJoinColumns)) {
      return false;
    }
    if (!Objects.equals(rightAlias, element.rightAlias)) {
      return false;
    }
    if (!Objects.equals(rightJoinColumns, element.rightJoinColumns)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(joinType, leftAlias, leftJoinColumns, rightAlias, rightJoinColumns, expression);
  }

  @Override
  public String toString()
  {
    return "JoinElement{" +
           "joinType=" + joinType +
           ", leftAlias=" + leftAlias +
           ", leftJoinColumns=" + leftJoinColumns +
           ", rightAlias=" + rightAlias +
           ", rightJoinColumns=" + rightJoinColumns +
           '}';
  }
}
