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

package io.druid.query.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.query.BaseQuery;
import io.druid.query.BrokerFilteringQuery;
import io.druid.query.DataSource;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 *
 */
@JsonTypeName("deleteColumns")
public class DeleteColumnsQuery extends BaseQuery<DDLResult> implements BrokerFilteringQuery<DDLResult>
{
  private final List<String> columns;
  private final long assertTimeout;
  private final boolean assertLoaded;

  public DeleteColumnsQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("assertTimeout") long assertTimeout,
      @JsonProperty("assertLoaded") boolean assertLoaded,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.columns = columns;
    this.assertTimeout = assertTimeout;
    this.assertLoaded = assertLoaded;
  }

  @Override
  public String getType()
  {
    return "deleteColumns";
  }

  @Override
  public Comparator<DDLResult> getMergeOrdering(List<String> columns)
  {
    return null;
  }

  @Override
  public DeleteColumnsQuery withDataSource(DataSource dataSource)
  {
    return new DeleteColumnsQuery(
        dataSource,
        querySegmentSpec,
        columns,
        assertTimeout,
        assertLoaded,
        context
    );
  }

  @Override
  public DeleteColumnsQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new DeleteColumnsQuery(
        dataSource,
        querySegmentSpec,
        columns,
        assertTimeout,
        assertLoaded,
        context
    );
  }

  @Override
  public DeleteColumnsQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new DeleteColumnsQuery(
        dataSource,
        querySegmentSpec,
        columns,
        assertTimeout,
        assertLoaded,
        computeOverriddenContext(contextOverride)
    );
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public long getAssertTimeout()
  {
    return assertTimeout;
  }

  @JsonProperty
  public boolean isAssertLoaded()
  {
    return assertLoaded;
  }

  @Override
  public String toString()
  {
    return "DeleteColumnsQuery{" +
           "dataSource='" + dataSource + '\'' +
           (querySegmentSpec == null ? "" : ", querySegmentSpec=" + querySegmentSpec) +
           ", columns=" + columns +
           ", assertTimeout=" + assertTimeout +
           ", assertLoaded=" + assertLoaded +
           '}';
  }

  @Override
  public Predicate<QueryableDruidServer> filter()
  {
    return server -> "historical".equals(server.getServer().getType());
  }

  public DataSegment rewrite(DataSegment descriptor)
  {
    String newVersion = new DateTime(descriptor.getVersion()).plus(1).toString();
    List<String> dimensions = Lists.newArrayList(descriptor.getDimensions());
    dimensions.removeAll(columns);
    List<String> metrics = Lists.newArrayList(descriptor.getMetrics());
    metrics.removeAll(columns);
    return descriptor.withVersion(newVersion).withDimensionsMetrics(dimensions, metrics);
  }
}
