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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaResultValue;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    return Preconditions.checkNotNull(found, "cannot find alias from " + expression);
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

  public static Query toQuery(
      DataSource dataSource,
      List<String> sortColumns,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context
  )
  {
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      if (query instanceof JoinQuery.JoinDelegate) {
        return ((JoinQuery.JoinDelegate) query).toArrayJoin();  // keep array for output
      }
      if (!(query instanceof Query.ArrayOutputSupport)) {
        throw new UnsupportedOperationException("todo: cannot resolve output column names on " + query.getType());
      }
      if (GuavaUtils.isNullOrEmpty(((Query.ArrayOutputSupport) query).estimatedOutputColumns())) {
        throw new UnsupportedOperationException("todo: cannot resolve output column names..");
      }
      if (!GuavaUtils.isNullOrEmpty(sortColumns) && query instanceof Query.OrderingSupport) {
        if (!(query.getDataSource() instanceof QueryDataSource)) {
          query = ((Query.OrderingSupport) query).withResultOrdering(OrderByColumnSpec.ascending(sortColumns));
        }
      }
      return query;
    }
    // even for hash join, sorting will help building hash efficiently, I guess
    return new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .intervals(segmentSpec)
        .context(BaseQuery.copyContextForMeta(context))
        .streaming(sortColumns);
  }

  public static long estimatedNumRows(
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      Map<String, Object> context,
      QuerySegmentWalker segmentWalker,
      QueryConfig config
  )
  {
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      if (query.getDataSource() instanceof QueryDataSource) {
        if (query instanceof StreamQuery) {
          StreamQuery stream = (StreamQuery) query;
          // ignore simple projections
          long estimated = estimatedNumRows(query.getDataSource(), segmentSpec, context, segmentWalker, config);
          if (estimated > 0 && stream.getFilter() != null) {
            estimated >>= 1;
          }
          LimitSpec limitSpec = stream.getLimitSpec();
          if (estimated > 0 && limitSpec.hasLimit()) {
            estimated = Math.min(limitSpec.getLimit(), estimated);
          }
          return estimated;
        }
        return -1;  // see later
      }
      if (query instanceof JoinQuery.JoinDelegate) {
        return ((JoinQuery.JoinDelegate) query).getEstimatedCardinality();
      }
      if (query instanceof BaseAggregationQuery) {
        BaseAggregationQuery aggregation = (BaseAggregationQuery) query;
        long estimated = Queries.estimateCardinality(aggregation.withHavingSpec(null), segmentWalker, config);
        if (estimated > 0 && aggregation.getHavingSpec() != null) {
          estimated >>= 1;
        }
        return estimated;
      }
      if (query instanceof StreamQuery) {
        StreamQuery stream = (StreamQuery) query;
        return runSelectMetaQuery(
            stream.getDataSource(),
            stream.getQuerySegmentSpec(),
            stream.getFilter(),
            BaseQuery.copyContextForMeta(stream),
            segmentWalker
        );
      }
      return -1;
    }
    return runSelectMetaQuery(dataSource, segmentSpec, null, BaseQuery.copyContextForMeta(context), segmentWalker);
  }

  public static long runSelectMetaQuery(
      DataSource dataSource,
      QuerySegmentSpec segmentSpec,
      DimFilter filter,
      Map<String, Object> context,
      QuerySegmentWalker segmentWalker
  )
  {
    SelectMetaQuery query = SelectMetaQuery.of(dataSource, segmentSpec, filter, context);
    Result<SelectMetaResultValue> result = Sequences.only(query.run(segmentWalker, null), null);
    return result == null ? -1 : result.getValue().getTotalCount();
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

    if (expression != null ? !expression.equals(element.expression) : element.expression != null) {
      return false;
    }
    if (joinType != element.joinType) {
      return false;
    }
    if (leftAlias != null ? !leftAlias.equals(element.leftAlias) : element.leftAlias != null) {
      return false;
    }
    if (leftJoinColumns != null ? !leftJoinColumns.equals(element.leftJoinColumns) : element.leftJoinColumns != null) {
      return false;
    }
    if (rightAlias != null ? !rightAlias.equals(element.rightAlias) : element.rightAlias != null) {
      return false;
    }
    if (rightJoinColumns != null
        ? !rightJoinColumns.equals(element.rightJoinColumns)
        : element.rightJoinColumns != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = joinType != null ? joinType.hashCode() : 0;
    result = 31 * result + (leftAlias != null ? leftAlias.hashCode() : 0);
    result = 31 * result + (leftJoinColumns != null ? leftJoinColumns.hashCode() : 0);
    result = 31 * result + (rightAlias != null ? rightAlias.hashCode() : 0);
    result = 31 * result + (rightJoinColumns != null ? rightJoinColumns.hashCode() : 0);
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    return result;
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
