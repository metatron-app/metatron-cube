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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName(Query.SELECT_STREAM)
public class StreamQuery extends AbstractStreamQuery<StreamQueryRow>
{
  @JsonCreator
  public StreamQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("descending") boolean descending,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("concatString") String concatString,
      @JsonProperty("limit") int limit,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        descending,
        dimFilter,
        columns,
        virtualColumns,
        concatString,
        limit,
        context
    );
  }

  @Override
  public String getType()
  {
    return Query.SELECT_STREAM;
  }

  @Override
  public StreamQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new StreamQuery(
        getDataSource(),
        querySegmentSpec,
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamQuery withDataSource(DataSource dataSource)
  {
    return new StreamQuery(
        dataSource,
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getLimit(),
        computeOverriddenContext(contextOverrides)
    );
  }

  @Override
  public StreamQuery withDimFilter(DimFilter dimFilter)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        dimFilter,
        getColumns(),
        getVirtualColumns(),
        getConcatString(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamQuery withVirtualColumns(List<VirtualColumn> virtualColumns)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        getColumns(),
        virtualColumns,
        getConcatString(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public StreamQuery withColumns(List<String> columns)
  {
    return new StreamQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        getDimFilter(),
        columns,
        getVirtualColumns(),
        getConcatString(),
        getLimit(),
        getContext()
    );
  }

  @Override
  public Sequence<Object[]> array(Sequence<StreamQueryRow> sequence)
  {
    final String[] columns = getColumns().toArray(new String[0]);
    return Sequences.map(
        sequence, new Function<StreamQueryRow, Object[]>()
        {
          @Override
          public Object[] apply(StreamQueryRow input)
          {
            final Object[] array = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
              array[i] = input.get(columns[i]);
            }
            return array;
          }
        }
    );
  }
}
