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

package io.druid.query.jmx;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.common.Intervals;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 */
@JsonTypeName("jmx")
public class JMXQuery extends BaseQuery<Map<String, Object>> implements Query.ManagementQuery
{
  public JMXQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("jmx") : dataSource,
        querySegmentSpec == null ? new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY) : querySegmentSpec,
        false,
        context
    );
  }

  @Override
  public String getType()
  {
    return "jmx";
  }

  @Override
  public JMXQuery withDataSource(DataSource dataSource)
  {
    return new JMXQuery(
        dataSource,
        getQuerySegmentSpec(),
        getContext()
    );
  }

  @Override
  public JMXQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new JMXQuery(
        getDataSource(),
        spec,
        getContext()
    );
  }

  @Override
  public JMXQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JMXQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return getType();
  }
}
