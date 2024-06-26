/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;

import io.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@JsonTypeName("classify")
public class ClassifyPostProcessor implements PostProcessingOperator.UnionSupport, RowSignature.Evolving
{
  private static final Logger LOG = new Logger(ClassifyPostProcessor.class);

  private final String tagColumn;

  @JsonCreator
  public ClassifyPostProcessor(@JsonProperty("tagColumn") String tagColumn) {this.tagColumn = tagColumn;}

  @Override
  public QueryRunner postProcess(QueryRunner baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner, final ExecutorService exec)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        List<Pair<Query, Sequence>> sequences = Sequences.toList(baseRunner.run(query, responseContext));
        Preconditions.checkArgument(!sequences.isEmpty(), "should not be empty");
        Pair<Query, Sequence> first = sequences.remove(0);
        Preconditions.checkArgument(first.lhs instanceof Query.ClassifierFactory, "first should be classifier factory");
        Classifier classifier = ((Query.ClassifierFactory) first.lhs).toClassifier(first.rhs, tagColumn);

        List<Sequence<Object>> tagged = Lists.newArrayList();
        for (Pair<Query, Sequence> pair : sequences) {
          Sequence sequence = pair.rhs;
          List<String> outputColumns = sequence.columns();
          List<String> finalColumns = outputColumns == null ? null : GuavaUtils.concat(outputColumns, tagColumn);
          if (pair.lhs instanceof Query.ArrayOutputSupport) {
            Query.ArrayOutputSupport stream = (Query.ArrayOutputSupport) pair.lhs;
            if (outputColumns != null) {
              sequence = Sequences.map(finalColumns, stream.array(sequence), classifier.init(outputColumns));
            }
          } else {
            sequence = Sequences.map(finalColumns, sequence, classifier);
          }
          tagged.add(sequence);
        }
        return Sequences.concat(tagged);
      }
    };
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    return schema == null ? null : GuavaUtils.concat(schema, tagColumn);
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    return schema == null ? null : schema.append(tagColumn, ValueDesc.LONG);
  }

  @Override
  public String toString()
  {
    return "ClassifyPostProcessor{" +
           "tagColumn='" + tagColumn + '\'' +
           '}';
  }
}
