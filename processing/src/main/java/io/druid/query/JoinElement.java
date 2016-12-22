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
import io.druid.query.select.PagingSpec;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.List;

/**
 */
public class JoinElement
{
  private final DataSource dataSource;
  private final List<String> joinExpressions;

  @JsonCreator
  public JoinElement(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("joinExpressions") List<String> joinExpressions
  )
  {
    this.dataSource = dataSource;
    this.joinExpressions = joinExpressions;
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<String> getJoinExpressions()
  {
    return joinExpressions;
  }

  public boolean hasFilter()
  {
    return dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getQuery().hasFilters();
  }

  public Query toQuery(QuerySegmentSpec segmentSpec)
  {
    if (dataSource instanceof QueryDataSource) {
      return (((QueryDataSource) dataSource).getQuery());
    }
    // should be replaced with streaming query
    return new Druids.SelectQueryBuilder()
        .dataSource(dataSource)
        .intervals(segmentSpec)
        .pagingSpec(PagingSpec.newSpec(Integer.MAX_VALUE))
        .build();
  }

  @Override
  public String toString()
  {
    return "JoinElement{" +
           "dataSource=" + dataSource +
           ", joinExpressions=" + joinExpressions +
           '}';
  }
}
