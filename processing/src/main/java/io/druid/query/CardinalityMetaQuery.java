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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 *
 */
@JsonTypeName(Query.CARDINALITY_META)
public class CardinalityMetaQuery extends BaseQuery<CardinalityMeta>
{
  public static Query<CardinalityMeta> of(TableDataSource ds, List<String> columns)
  {
    return new CardinalityMetaQuery(ds, null, columns, columns.size() > 1, null).withRandomId();
  }

  private final List<String> columns;
  private final boolean complex;

  @JsonCreator
  public CardinalityMetaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("complex") boolean complex,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.columns = columns;
    this.complex = complex;
  }

  @Override
  public String getType()
  {
    return CARDINALITY_META;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public boolean isComplex()
  {
    return complex;
  }

  @Override
  public Comparator<CardinalityMeta> getMergeOrdering(List<String> columns)
  {
    return (c1, c2) -> 0;
  }

  @Override
  public CardinalityMetaQuery withDataSource(DataSource dataSource)
  {
    return new CardinalityMetaQuery(dataSource, getQuerySegmentSpec(), columns, complex, getContext());
  }

  @Override
  public CardinalityMetaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new CardinalityMetaQuery(getDataSource(), spec, columns, complex, getContext());
  }

  @Override
  public CardinalityMetaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new CardinalityMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        columns,
        complex,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return String.format(
        "CardinalityMetaQuery{dataSource='%s', columns=%s, complex=%s}",
        getDataSource(),
        columns,
        complex
    );
  }
}