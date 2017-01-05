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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.math.expr.Parser;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class JoinQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  private static final Logger log = new Logger(JoinQuery.class);

  private final JoinType joinType;
  private final List<JoinElement> elements;
  private final int numPartition;
  private final int scannerLen;
  private final int limit;

  private final String leftAlias;
  private final String rightAlias;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("elements") List<JoinElement> elements,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("numPartition") int numPartition,
      @JsonProperty("scannerLen") int scannerLen,
      @JsonProperty("limit") int limit,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(validateDataSource(elements), validateSegmentSpec(elements, querySegmentSpec), false, context);
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.elements = validateJoinCondition(elements);
    this.numPartition = numPartition == 0 && scannerLen == 0 ? 1 : numPartition;
    this.scannerLen = scannerLen;
    this.limit = limit;
    this.leftAlias = Iterables.getOnlyElement(elements.get(0).getDataSource().getNames());
    this.rightAlias = Iterables.getOnlyElement(elements.get(1).getDataSource().getNames());
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
      Query query1 = ((QueryDataSource) lhs.getDataSource()).getQuery();
      QuerySegmentSpec segmentSpec1 = query1.getQuerySegmentSpec();
      if (segmentSpec != null && segmentSpec1.getIntervals().isEmpty()) {
        elements.set(0, lhs.withDataSource(new QueryDataSource(query1.withQuerySegmentSpec(segmentSpec))));
      } else {
        Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec1));
      }
      segmentSpec = segmentSpec1;
    }
    if (rhs.getDataSource() instanceof QueryDataSource) {
      Query query2 = ((QueryDataSource) rhs.getDataSource()).getQuery();
      QuerySegmentSpec segmentSpec2 = query2.getQuerySegmentSpec();
      if (segmentSpec != null && segmentSpec2.getIntervals().isEmpty()) {
        elements.set(1, lhs.withDataSource(new QueryDataSource(query2.withQuerySegmentSpec(segmentSpec))));
      } else {
        Preconditions.checkArgument(segmentSpec == null || segmentSpec.equals(segmentSpec2));
      }
      segmentSpec = segmentSpec2;
    }
    return segmentSpec;
  }

  private List<JoinElement> validateJoinCondition(List<JoinElement> elements)
  {
    Preconditions.checkArgument(elements.size() == 2, "For now");
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    Preconditions.checkArgument(lhs.getJoinExpressions().size() > 0);
    Preconditions.checkArgument(rhs.getJoinExpressions().size() > 0);
    Preconditions.checkArgument(lhs.getJoinExpressions().size() == rhs.getJoinExpressions().size());

    for (String expression : lhs.getJoinExpressions()) {
      Preconditions.checkArgument(Parser.findRequiredBindings(expression).size() == 1);
    }
    for (String expression : rhs.getJoinExpressions()) {
      Preconditions.checkArgument(Parser.findRequiredBindings(expression).size() == 1);
    }

    // todo: nested element with multiple datasources (union, etc.)
    Preconditions.checkArgument(lhs.getDataSource().getNames().size() == 1);
    Preconditions.checkArgument(rhs.getDataSource().getNames().size() == 1);

    return elements;
  }

  @JsonProperty
  public List<JoinElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public int getNumPartition()
  {
    return numPartition;
  }

  @JsonProperty
  public int getScannerLen()
  {
    return scannerLen;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
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
    return new JoinQuery(joinType, elements, getQuerySegmentSpec(), numPartition, scannerLen, limit, overridden);
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
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    JoinPartitionSpec partitions = partition(segmentWalker, jsonMapper);
    Map<String, Object> joinProcessor = Maps.newHashMap();

    joinProcessor.put(
        "postProcessing",
        ImmutableMap.builder()
                    .put("type", "join")
                    .put("joinType", joinType.name())
                    .put("leftAlias", leftAlias)
                    .put("leftJoinExpressions", lhs.getJoinExpressions())
                    .put("rightAlias", rightAlias)
                    .put("rightJoinExpressions", rhs.getJoinExpressions())
                    .build()
    );
    Map<String, Object> context = getContext();
    Map<String, Object> joinContext = computeOverridenContext(joinProcessor);

    QuerySegmentSpec segmentSpec = getQuerySegmentSpec();
    if (partitions == null) {
      return new JoinDelegate(Arrays.asList(lhs.toQuery(segmentSpec), rhs.toQuery(segmentSpec)), limit, joinContext);
    }
    List<Query> queries = Lists.newArrayList();
    for (Pair<DimFilter, DimFilter> filters : partitions) {
      List<Query> list = Arrays.asList(lhs.toQuery(segmentSpec, filters.lhs), rhs.toQuery(segmentSpec, filters.rhs));
      queries.add(new JoinDelegate(list, -1, joinContext));
    }
    return new JoinDelegate(queries, limit, context);
  }

  private JoinPartitionSpec partition(QuerySegmentWalker segmentWalker, ObjectMapper jsonMapper)
  {
    if (numPartition <= 1 && scannerLen <= 0) {
      return null;
    }
    JoinElement lhs = elements.get(0);
    JoinElement rhs = elements.get(1);

    String left = lhs.getJoinExpressions().get(0);
    String right = rhs.getJoinExpressions().get(0);

    List<String> partitions;
    if (joinType != JoinType.RO) {
      partitions = runSketchQuery(segmentWalker, lhs.getFilter(), jsonMapper, leftAlias, left);
    } else {
      partitions = runSketchQuery(segmentWalker, rhs.getFilter(), jsonMapper, rightAlias, right);
    }
    if (partitions != null && partitions.size() > 2) {
      return new JoinPartitionSpec(left, right, partitions);
    }
    return null;
  }

  private List<String> runSketchQuery(
      QuerySegmentWalker segmentWalker,
      DimFilter filter,
      ObjectMapper jsonMapper,
      String table,
      String expression
  )
  {
    // default.. regard skewed
    Object postProc = ImmutableMap.<String, Object>of(
        "type", "sketch.quantiles",
        "op", "QUANTILES",
        "evenSpaced", numPartition > 0 ? numPartition + 1 : -1,
        "evenCounted", scannerLen > 0 ? scannerLen : -1
    );

    Query.DimFilterSupport query = (DimFilterSupport) Queries.toQuery(
        ImmutableMap.<String, Object>builder()
                    .put("queryType", "sketch")
                    .put("dataSource", table)
                    .put("intervals", getQuerySegmentSpec())
                    .put("dimensions", Arrays.asList(expression))
                    .put("sketchOp", "QUANTILE")
                    .put("context", ImmutableMap.of("postProcessing", postProc))
                    .build(), jsonMapper);

    if (query == null) {
      return null;
    }
    if (filter != null) {
      query = query.withDimFilter(filter);
    }
    log.info("Running sketch query on join partition key %s.%s", table, expression);
    log.debug("Running.. %s", query);

    @SuppressWarnings("unchecked")
    final List<Result<Map<String, Object>>> res = Sequences.toList(
        query.run(segmentWalker, Maps.newHashMap()), Lists.<Result<Map<String, Object>>>newArrayList()
    );
    if (!res.isEmpty()) {
      String prev = null;
      String[] splits = (String[])res.get(0).getValue().get(expression);
      log.info("Partition keys.. %s", Arrays.toString(splits));
      List<String> partitions = Lists.newArrayList();
      for (String split : splits) {
        if (prev == null || !prev.equals(split)) {
          partitions.add(split);
        }
        prev = split;
      }
      return partitions;
    }
    return null;
  }

  @Override
  public String toString()
  {
    return "JoinQuery{" +
           "joinType=" + joinType +
           ", elements=" + elements +
           ", numPartition=" + numPartition +
           ", scannerLen=" + scannerLen +
           ", limit=" + limit +
           '}';
  }

  private static class JoinPartitionSpec implements Iterable<Pair<DimFilter, DimFilter>>
  {
    final String leftExpression;
    final String rightExpression;
    final List<String> partitions;

    private JoinPartitionSpec(
        String leftExpression,
        String rightExpression,
        List<String> partitions
    )
    {
      this.leftExpression = Preconditions.checkNotNull(leftExpression);
      this.rightExpression = Preconditions.checkNotNull(rightExpression);
      this.partitions = Preconditions.checkNotNull(partitions);
    }

    @Override
    public Iterator<Pair<DimFilter, DimFilter>> iterator()
    {
      return new Iterator<Pair<DimFilter, DimFilter>>()
      {
        private int index = 1;

        @Override
        public boolean hasNext()
        {
          return index < partitions.size();
        }

        @Override
        public Pair<DimFilter, DimFilter> next()
        {
          DimFilter left;
          DimFilter right;
          if (index == 1) {
            left = BoundDimFilter.lt(leftExpression, partitions.get(index));
            right = BoundDimFilter.lt(rightExpression, partitions.get(index));
          } else if (index == partitions.size() - 1) {
            left = BoundDimFilter.gte(leftExpression, partitions.get(index - 1));
            right = BoundDimFilter.gte(rightExpression, partitions.get(index - 1));
          } else {
            left = BoundDimFilter.between(leftExpression, partitions.get(index - 1), partitions.get(index));
            right = BoundDimFilter.between(rightExpression, partitions.get(index - 1), partitions.get(index));
          }
          index++;
          return Pair.of(left, right);
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  public static class JoinDelegate extends UnionAllQuery
  {
    public JoinDelegate(List<Query> list, int limit, Map<String, Object> context)
    {
      super(null, list, false, limit, context);
    }

    @Override
    public Query withQueries(List queries)
    {
      return new JoinDelegate(queries, getLimit(), getContext());
    }

    @Override
    public Query withQuery(Query query)
    {
      throw new IllegalStateException();
    }

    @Override
    public String toString()
    {
      return "JoinDelegate{" +
             "queries=" + getQueries() +
             ", limit=" + getLimit() +
             '}';
    }
  }
}
