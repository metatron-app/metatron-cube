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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.Granularities;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class SchemaQuery extends BaseQuery<Result<SelectMetaResultValue>>
    implements Query.RewritingQuery<Result<SelectMetaResultValue>>
{
  public static SchemaQuery of(String dataSource)
  {
    return new SchemaQuery(
        TableDataSource.of(dataSource),
        new MultipleIntervalSegmentSpec(Arrays.asList(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT))),
        ImmutableMap.<String, Object>of("allDimensionsForEmpty", false, "allMetricsForEmpty", false)
    );
  }

  @JsonCreator
  public SchemaQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
  }

  @Override
  public String getType()
  {
    return Query.SCHEMA;
  }

  @Override
  public SelectMetaQuery rewriteQuery(
      QuerySegmentWalker segmentWalker, QueryConfig queryConfig, ObjectMapper jsonMapper
  )
  {
    return new SelectMetaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        null,
        Granularities.ALL,
        null,
        null,
        null,
        true,
        null,
        getContext()
    );
  }

  @Override
  public SchemaQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SchemaQuery(getDataSource(), spec, getContext());
  }

  @Override
  public SchemaQuery withDataSource(DataSource dataSource)
  {
    return new SchemaQuery(
        dataSource,
        getQuerySegmentSpec(),
        getContext()
    );
  }

  @Override
  public SchemaQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SchemaQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return "SchemaQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           '}';
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
    return super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }
}
