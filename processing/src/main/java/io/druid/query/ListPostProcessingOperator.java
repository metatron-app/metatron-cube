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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.query.PostProcessingOperator.UnionSupport;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 */
@JsonTypeName("list")
public class ListPostProcessingOperator<T> implements RowSignature.Evolving, UnionSupport<T>
{
  private final List<PostProcessingOperator> processors;
  private final boolean supportsUnion;

  @JsonCreator
  public ListPostProcessingOperator(
      @JsonProperty("processors") List<PostProcessingOperator> processors
  )
  {
    Preconditions.checkArgument(processors != null && !processors.isEmpty());
    this.processors = Lists.newArrayList(processors);
    this.supportsUnion = processors.get(0).supportsUnionProcessing();
    for (int i = 1; i < processors.size(); i++) {
      Preconditions.checkArgument(!processors.get(i).supportsUnionProcessing());
    }
  }

  @JsonProperty
  public List<PostProcessingOperator> getProcessors()
  {
    return processors;
  }

  @JsonIgnore
  public List<PostProcessingOperator> getProcessorsInReverse()
  {
    return Lists.reverse(processors);
  }

  public PostProcessingOperator getLast()
  {
    return processors.get(processors.size() - 1);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(QueryRunner<T> baseQueryRunner)
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
  public QueryRunner<T> postProcess(UnionAllQueryRunner baseQueryRunner, ExecutorService exec)
  {
    QueryRunner<T> queryRunner = ((UnionSupport) processors.get(0)).postProcess(baseQueryRunner, exec);
    for (PostProcessingOperator processor : processors.subList(1, processors.size())) {
      queryRunner = processor.postProcess(queryRunner);
    }
    return queryRunner;
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    for (PostProcessingOperator child : processors) {
      if (child instanceof RowSignature.Evolving) {
        schema = ((RowSignature.Evolving) child).evolve(schema);
      }
    }
    return schema;
  }

  @Override
  public RowSignature evolve(Query query, RowSignature schema)
  {
    for (PostProcessingOperator child : processors) {
      if (child instanceof RowSignature.Evolving) {
        schema = ((RowSignature.Evolving) child).evolve(query, schema);
      }
    }
    return schema;
  }

  @Override
  public String toString()
  {
    return "ListPostProcessingOperator" + processors;
  }
}
