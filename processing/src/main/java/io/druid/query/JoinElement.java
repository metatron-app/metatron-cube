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
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.ISE;
import io.druid.query.Query.ArrayOutputSupport;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;

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
  public static JoinElement inner(String expression)
  {
    return new JoinElement(JoinType.INNER, expression);
  }

  public static JoinElement of(JoinType type, String expression)
  {
    return new JoinElement(type, expression);
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
    return new JoinElement(joinType, leftAlias, leftJoinColumns, rightAlias, rightJoinColumns, null);
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
  public int groupKeys()
  {
    return leftJoinColumns.size();
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

  public static Query toQuery(
      QuerySegmentWalker segmentWalker,
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context
  )
  {
    return toQuery(segmentWalker, dataSource, null, segmentSpec, context);
  }

  @SuppressWarnings("unchecked")
  public static Query toQuery(
      QuerySegmentWalker segmentWalker,
      DataSource dataSource,
      List<String> sortColumns,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context
  ) {
    return toQuery(segmentWalker, dataSource, sortColumns, segmentSpec).withOverriddenContext(context);
  }

  private static Query toQuery(
      QuerySegmentWalker segmentWalker,
      DataSource dataSource,
      List<String> sortColumns,
      QuerySegmentSpec segmentSpec
  )
  {
    Preconditions.checkNotNull(dataSource);
    MaterializedQuery materialized = segmentWalker.getMaterialized(dataSource);
    if (materialized != null) {
      if (!GuavaUtils.isNullOrEmpty(sortColumns)) {
        materialized = (MaterializedQuery) materialized.withResultOrdering(OrderByColumnSpec.ascending(sortColumns))
                                                       .withOverriddenContext(SORTING, true);
      }
      return materialized;
    }
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
      return query;
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
          .streaming(sortColumns);
      if (GuavaUtils.isNullOrEmpty(query.getColumns())) {
        query = (StreamQuery) QueryUtils.resolve(query, segmentWalker);
      }
      return query;
    }
    throw new ISE("todo: cannot join on %s(%s)", dataSource, dataSource.getClass());
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
