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
import com.google.common.base.Preconditions;

import java.util.List;

/**
 */
public class ListPostProcessingOperator<T> extends PostProcessingOperator.UnionSupport<T>
{
  private final List<PostProcessingOperator> processors;
  private final boolean supportsUnion;

  @JsonCreator
  public ListPostProcessingOperator(
      @JsonProperty("processors") List<PostProcessingOperator> processors
  )
  {
    Preconditions.checkArgument(processors != null && !processors.isEmpty());
    this.processors = processors;
    this.supportsUnion = processors.get(0).supportsUnionProcessing();
    for (int i = 1; i < processors.size(); i++) {
      Preconditions.checkArgument(!processors.get(i).supportsUnionProcessing());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<T> postProcess(QueryRunner<T> baseQueryRunner)
  {
    for (PostProcessingOperator processor : processors) {
      baseQueryRunner = processor.postProcess(baseQueryRunner);
    }
    return baseQueryRunner;
  }

  @Override
  public boolean supportsUnionProcessing()
  {
    return supportsUnion;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<T> postProcess(UnionAllQueryRunner<T> baseQueryRunner)
  {
    QueryRunner<T> queryRunner = ((PostProcessingOperator.UnionSupport)processors.get(0)).postProcess(baseQueryRunner);
    for (PostProcessingOperator processor : processors) {
      queryRunner = processor.postProcess(queryRunner);
    }
    return queryRunner;
  }

  @Override
  public boolean hasTabularOutput()
  {
    return processors.get(processors.size() - 1).hasTabularOutput();
  }
}
