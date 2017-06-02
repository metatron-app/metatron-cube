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
import io.druid.query.aggregation.model.HoltWintersPostProcessor;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "timewarp", value = TimewarpOperator.class),
    @JsonSubTypes.Type(name = "join", value = JoinPostProcessor.class),
    @JsonSubTypes.Type(name = "tabular", value = TabularPostProcessor.class),
    @JsonSubTypes.Type(name = "holtWinters", value = HoltWintersPostProcessor.class)
})
public interface PostProcessingOperator<T>
{
  public QueryRunner<T> postProcess(QueryRunner<T> baseQueryRunner);

  public boolean supportsUnionProcessing();

  public boolean hasTabularOutput();

  public abstract class UnionSupport<T> implements PostProcessingOperator<T>
  {
    @Override
    public boolean supportsUnionProcessing() { return true;}

    public abstract QueryRunner<T> postProcess(UnionAllQueryRunner<T> baseQueryRunner);
  }

  public abstract class Abstract<T> implements PostProcessingOperator<T>
  {
    @Override
    public boolean supportsUnionProcessing() { return false;}

    @Override
    public boolean hasTabularOutput() { return false; }
  }
}
