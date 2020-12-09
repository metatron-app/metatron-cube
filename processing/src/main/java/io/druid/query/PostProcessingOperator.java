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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.data.input.Row;
import io.druid.query.aggregation.model.HoltWintersPostProcessor;
import io.druid.query.groupby.LimitingPostProcessor;

import java.util.Map;
import java.util.concurrent.ExecutorService;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = PostAggregationsPostProcessor.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "timewarp", value = TimewarpOperator.class),
    @JsonSubTypes.Type(name = "join", value = JoinPostProcessor.class),
    @JsonSubTypes.Type(name = "broadcastJoin", value = BroadcastJoinProcessor.class),
    @JsonSubTypes.Type(name = "toMap", value = ToMapPostProcessor.class),
    @JsonSubTypes.Type(name = "holtWinters", value = HoltWintersPostProcessor.class),
    @JsonSubTypes.Type(name = "rowToMap", value = RowToMap.class),
    @JsonSubTypes.Type(name = "rowToArray", value = RowToArray.class),
    @JsonSubTypes.Type(name = "arrayToMap", value = ArrayToMap.class),
    @JsonSubTypes.Type(name = "arrayToRow", value = ArrayToRow.class),
    @JsonSubTypes.Type(name = "selectToRow", value = SelectToRow.class),
    @JsonSubTypes.Type(name = "topNToRow", value = TopNToRow.class),
    @JsonSubTypes.Type(name = "list", value = ListPostProcessingOperator.class),
    @JsonSubTypes.Type(name = "limit", value = LimitingPostProcessor.class),
    @JsonSubTypes.Type(name = "postAggregations", value = PostAggregationsPostProcessor.class),
    @JsonSubTypes.Type(name = "rowMapping", value = RowMappingPostProcessor.class),
    @JsonSubTypes.Type(name = "dbScan", value = DBScanPostProcessor.class),
    @JsonSubTypes.Type(name = "fft", value = FFTPostProcessor.class),
    @JsonSubTypes.Type(name = "classify", value = ClassifyPostProcessor.class),
})
public interface PostProcessingOperator<T>
{
  TypeReference<PostProcessingOperator> TYPE_REF = new TypeReference<PostProcessingOperator>() {};

  @JsonProperty
  default String getType()
  {
    return getClass().getAnnotation(JsonTypeName.class).value();
  }

  QueryRunner postProcess(QueryRunner<T> baseQueryRunner);

  default boolean supportsUnionProcessing()
  {
    return false;
  }

  public interface UnionSupport<T> extends PostProcessingOperator<T>
  {
    @Override
    default boolean supportsUnionProcessing() { return true;}

    QueryRunner postProcess(UnionAllQueryRunner<?> baseQueryRunner, ExecutorService exec);
  }

  public abstract class Abstract<T> implements PostProcessingOperator<T>
  {
    @Override
    public String toString()
    {
      return getClass().getSimpleName();
    }
  }

  public interface ReturnRowAs
  {
    Class rowClass();
  }

  public abstract class ReturnsMap<T> extends Abstract<T> implements ReturnRowAs
  {
    public abstract QueryRunner<Map<String, Object>> postProcess(QueryRunner<T> baseQueryRunner);

    @Override
    public Class rowClass()
    {
      return Map.class;
    }
  }

  public abstract class ReturnsRow<T> extends Abstract<T> implements ReturnRowAs
  {
    public abstract QueryRunner<Row> postProcess(QueryRunner<T> baseQueryRunner);

    @Override
    public Class rowClass()
    {
      return Row.class;
    }
  }

  public abstract class ReturnsArray<T> extends Abstract<T> implements ReturnRowAs
  {
    public abstract QueryRunner<Object[]> postProcess(QueryRunner<T> baseQueryRunner);

    @Override
    public Class rowClass()
    {
      return Object[].class;
    }
  }

  interface LogProvider<T> extends PostProcessingOperator<T>
  {
    PostProcessingOperator<T> forLog();
  }

  // marker for not-serializable post processors
  interface Local
  {
  }
}
