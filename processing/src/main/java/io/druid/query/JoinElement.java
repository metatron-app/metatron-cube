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
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;

/**
 */
public class JoinElement
{
  private final JoinType joinType;

  private final String leftAlias;
  private final List<String> leftJoinColumns;

  private final String rightAlias;
  private final List<String> rightJoinColumns;

  @JsonCreator
  public JoinElement(
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("leftAlias") String leftAlias,
      @JsonProperty("leftJoinColumns") List<String> leftJoinColumns,
      @JsonProperty("rightAlias") String rightAlias,
      @JsonProperty("rightJoinColumns") List<String> rightJoinColumns
  )
  {
    this.joinType = joinType == null ? JoinType.INNER : joinType;
    this.leftAlias = leftAlias;   // can be null.. ignored when i > 0
    this.leftJoinColumns = Preconditions.checkNotNull(leftJoinColumns);
    this.rightAlias = Preconditions.checkNotNull(rightAlias);
    this.rightJoinColumns = Preconditions.checkNotNull(rightJoinColumns);
    Preconditions.checkArgument(leftJoinColumns.size() > 0);
    Preconditions.checkArgument(leftJoinColumns.size() == rightJoinColumns.size());
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

  public int keyLength()
  {
    return leftJoinColumns.size();
  }

  public String[] getFirstKeys()
  {
    return new String[]{leftJoinColumns.get(0), rightJoinColumns.get(0)};
  }

  public static Query toQuery(DataSource dataSource, QuerySegmentSpec segmentSpec)
  {
    return toQuery(dataSource, segmentSpec, null);
  }

  public static Query toQuery(DataSource dataSource, QuerySegmentSpec segmentSpec, DimFilter filter)
  {
    if (dataSource instanceof QueryDataSource) {
      Query query = ((QueryDataSource) dataSource).getQuery();
      if (filter != null) {
        Query.DimFilterSupport filterSupport = (Query.DimFilterSupport) query;
        if (filterSupport.getDimFilter() != null) {
          filter = AndDimFilter.of(filterSupport.getDimFilter(), filter);
          query = filterSupport.withDimFilter(filter);
        }
      }
      return query;
    }
    return new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .intervals(segmentSpec)
        .filters(filter)
        .streaming();
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
