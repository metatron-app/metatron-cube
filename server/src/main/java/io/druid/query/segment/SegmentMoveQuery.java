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

package io.druid.query.segment;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.timeline.DataSegment;

import java.util.Map;

@JsonTypeName("move")
public class SegmentMoveQuery extends BaseQuery<SegmentLocation>
{
  private final DataSegment segment;

  public SegmentMoveQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        dataSource == null ? TableDataSource.of("move") : dataSource,
        new SpecificSegmentSpec(segment.toDescriptor()),
        false,
        context
    );
    this.segment = Preconditions.checkNotNull(segment, "'segment' is missing");
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public String getType()
  {
    return "move";
  }

  @Override
  public SegmentMoveQuery withDataSource(DataSource dataSource)
  {
    return new SegmentMoveQuery(dataSource, segment, getContext());
  }

  @Override
  public SegmentMoveQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return this;
  }

  @Override
  public SegmentMoveQuery withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SegmentMoveQuery(
        getDataSource(),
        segment,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public String toString()
  {
    return "MoveQuery{segment=" + segment + '}';
  }
}
