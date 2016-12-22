/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class JoinQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  private final JoinType joinType;
  private final List<JoinElement> elements;
  private final JoinPartitionSpec partitionSpec;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("partitionSpec") JoinPartitionSpec partitionSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(validateDataSource(elements), validateSegmentSpec(elements, querySegmentSpec), false, context);
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.elements = validateJoinCondition(elements);
    this.partitionSpec = partitionSpec;
  }

  private static DataSource validateDataSource(List<JoinElement> elements)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    return elements.get(0).getDataSource();
  }

  private static QuerySegmentSpec validateSegmentSpec(List<JoinElement> elements, QuerySegmentSpec segmentSpec)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    if (lhs.getDataSource() instanceof QueryDataSource) {
      QuerySegmentSpec segmentSpec1 = ((QueryDataSource) lhs.getDataSource()).getQuery().getQuerySegmentSpec();
      Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec1));
      segmentSpec = segmentSpec1;
    }
    if (rhs.getDataSource() instanceof QueryDataSource) {
      QuerySegmentSpec segmentSpec2 = ((QueryDataSource) rhs.getDataSource()).getQuery().getQuerySegmentSpec();
      Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec2));
      segmentSpec = segmentSpec2;
    }
    return segmentSpec;
  }

  private List<JoinElement> validateJoinCondition(List<JoinElement> elements)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    Preconditions.checkArgument(lhs.getJoinExpressions().size() == rhs.getJoinExpressions().size());
    return elements;
  }

  @JsonProperty
  public List<JoinElement> getElements()
  {
    return elements;
  }

  @Override
  public boolean hasFilters()
  {
    for (JoinElement element : elements) {
      if (element.hasFilter()) {
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
  public Query<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    Map<String, Object> overridden = computeOverridenContext(contextOverride);
    return new JoinQuery(joinType, elements, getQuerySegmentSpec(), partitionSpec, overridden);
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    throw new IllegalStateException();
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    throw new IllegalStateException();
  }

  @SuppressWarnings("unchecked")
  public Query rewriteQuery()
  {
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    Map<String, Object> joinProcessor = Maps.newHashMap();

    joinProcessor.put(
        "postProcessing",
        ImmutableMap.builder()
                    .put("type", "join")
                    .put("joinType", joinType.name())
                    .put("leftAliases", lhs.getDataSource().getNames())
                    .put("leftJoinExpressions", lhs.getJoinExpressions())
                    .put("rightAliases", rhs.getDataSource().getNames())
                    .put("rightJoinExpressions", rhs.getJoinExpressions())
                    .build()
    );
    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    Map<String, Object> context = computeOverridenContext(joinProcessor);
    return new Delegate(Arrays.asList(lhs.toQuery(segmentSpec), rhs.toQuery(segmentSpec)), context);
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "joinType=" + joinType +
           ", elements=" + elements +
           ", partitionSpec=" + partitionSpec +
           '}';
  }

  @SuppressWarnings("unchecked")
  public static class Delegate extends UnionAllQuery
  {
    public Delegate(List<Query> list, Map<String, Object> context)
    {
      super(null, list, false, context);
    }

    @Override
    public Query withQueries(List queries)
    {
      return new Delegate(queries, getContext());
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }
  }
}
