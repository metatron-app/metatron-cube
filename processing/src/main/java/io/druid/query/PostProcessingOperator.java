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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.aggregation.model.HoltWintersPostProcessor;
import io.druid.query.groupby.LimitingPostProcessor;
import io.druid.segment.incremental.IncrementalIndexSchema;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = PostAggregationsPostProcessor.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "timewarp", value = TimewarpOperator.class),
    @JsonSubTypes.Type(name = "join", value = XJoinPostProcessor.class),
    @JsonSubTypes.Type(name = "tabular", value = TabularPostProcessor.class),
    @JsonSubTypes.Type(name = "holtWinters", value = HoltWintersPostProcessor.class),
    @JsonSubTypes.Type(name = "rowToMap", value = RowToMap.class),
    @JsonSubTypes.Type(name = "rowToArray", value = RowToArray.class),
    @JsonSubTypes.Type(name = "selectToRow", value = SelectToRow.class),
    @JsonSubTypes.Type(name = "topNToRow", value = TopNToRow.class),
    @JsonSubTypes.Type(name = "list", value = ListPostProcessingOperator.class),
    @JsonSubTypes.Type(name = "limit", value = LimitingPostProcessor.class),
    @JsonSubTypes.Type(name = "postAggregations", value = PostAggregationsPostProcessor.class),
    @JsonSubTypes.Type(name = "rowMapping", value = RowMappingPostProcessor.class),
})
public interface PostProcessingOperator<T>
{
  public QueryRunner postProcess(QueryRunner<T> baseQueryRunner);

  public boolean supportsUnionProcessing();

  // means output is Map<String, Object> (replace with Object[] ?)
  public boolean hasTabularOutput();

  public abstract class UnionSupport<T> implements PostProcessingOperator<T>
  {
    @Override
    public boolean supportsUnionProcessing() { return true;}

    public abstract QueryRunner postProcess(UnionAllQueryRunner<?> baseQueryRunner);

    @Override
    public String toString()
    {
      return getClass().getSimpleName();
    }
  }

  public abstract class Abstract<T> implements PostProcessingOperator<T>
  {
    @Override
    public boolean supportsUnionProcessing() { return false;}

    @Override
    public boolean hasTabularOutput() { return false; }

    @Override
    public String toString()
    {
      return getClass().getSimpleName();
    }
  }

  // this is needed to be implemented by all post processors, but let's do it step by step
  interface SchemaResolving
  {
    IncrementalIndexSchema resolve(Query query, IncrementalIndexSchema input, ObjectMapper mapper);
  }

  // marker for not-serializable post processors
  interface Local
  {
  }
}
