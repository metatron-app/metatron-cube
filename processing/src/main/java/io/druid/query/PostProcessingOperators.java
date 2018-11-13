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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Sequence;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class PostProcessingOperators
{
  public static <T> QueryRunner<T> wrap(final QueryRunner<T> baseRunner, final ObjectMapper mapper)
  {
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        PostProcessingOperator<T> processor = load(query, mapper);
        if (processor != null) {
          return processor.postProcess(baseRunner).run(query, responseContext);
        }
        return baseRunner.run(query, responseContext);
      }
    };
  }

  public static Map<String, Object> tabular(final ObjectMapper mapper, final String timestampColumn)
  {
    return ImmutableMap.of(
        Query.POST_PROCESSING,
        mapper.convertValue(
            ImmutableMap.of("type", "tabular", "timestampColumn", timestampColumn),
            new TypeReference<PostProcessingOperator>()
            {
            }
        )
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> PostProcessingOperator<T> load(Query<T> query, ObjectMapper mapper)
  {
    Object value = query.getContextValue(QueryContextKeys.POST_PROCESSING);
    if (value instanceof PostProcessingOperator) {
      return (PostProcessingOperator<T>) value;
    }
    return mapper.convertValue(
        value,
        new TypeReference<PostProcessingOperator<T>>()
        {
        }
    );
  }

  public static <T> boolean isTabularOutput(Query<T> query, ObjectMapper mapper)
  {
    PostProcessingOperator<T> processor = load(query, mapper);
    return processor != null && processor.hasTabularOutput();
  }

  @SuppressWarnings("unchecked")
  public static <T> Query append(Query<T> query, ObjectMapper mapper, PostProcessingOperator processor)
  {
    PostProcessingOperator<T> existing = load(query, mapper);
    if (existing != null) {
      if (existing instanceof ListPostProcessingOperator) {
        ((ListPostProcessingOperator) existing).getProcessors().add(processor);
        return query;
      }
      processor = new ListPostProcessingOperator(Arrays.asList(existing, processor));
    }
    return query.withOverriddenContext(Query.POST_PROCESSING, processor);
  }

  @SuppressWarnings("unchecked")
  public static <T> Query prepend(Query<T> query, ObjectMapper mapper, PostProcessingOperator processor)
  {
    PostProcessingOperator<T> existing = load(query, mapper);
    if (existing != null) {
      if (existing instanceof ListPostProcessingOperator) {
        ((ListPostProcessingOperator) existing).getProcessors().add(0, processor);
        return query;
      }
      processor = new ListPostProcessingOperator(Arrays.asList(processor, existing));
    }
    return query.withOverriddenContext(Query.POST_PROCESSING, processor);
  }
}
