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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;

import java.util.Arrays;
import java.util.List;
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
        PostProcessingOperator processor = load(query, mapper);
        if (processor != null) {
          return processor.postProcess(baseRunner).run(query, responseContext);
        }
        return baseRunner.run(query, responseContext);
      }
    };
  }

  public static Map<String, Object> toMap(final ObjectMapper mapper, final String timestampColumn)
  {
    return ImmutableMap.of(
        Query.POST_PROCESSING,
        mapper.convertValue(
            ImmutableMap.of("type", "toMap", "timestampColumn", timestampColumn),
            new TypeReference<PostProcessingOperator>()
            {
            }
        )
    );
  }

  @SuppressWarnings("unchecked")
  public static PostProcessingOperator load(Query query, ObjectMapper mapper)
  {
    Object value = query.getContextValue(QueryContextKeys.POST_PROCESSING);
    if (value instanceof PostProcessingOperator) {
      return (PostProcessingOperator) value;
    }
    return mapper.convertValue(
        value,
        new TypeReference<PostProcessingOperator>() {}
    );
  }

  public static <T> boolean isMapOutput(Query<T> query, ObjectMapper mapper)
  {
    return Map.class == returns(query, mapper);
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> append(Query<T> query, ObjectMapper mapper, PostProcessingOperator processor)
  {
    PostProcessingOperator existing = load(query, mapper);
    if (existing != null) {
      if (existing instanceof ListPostProcessingOperator) {
        processor = list(GuavaUtils.concat(((ListPostProcessingOperator) existing).getProcessors(), processor));
      } else {
        processor = list(Arrays.asList(existing, processor));
      }
    }
    return query.withOverriddenContext(Query.POST_PROCESSING, processor);
  }

  @SuppressWarnings("unchecked")
  public static <T> Query prepend(Query<T> query, ObjectMapper mapper, PostProcessingOperator processor)
  {
    PostProcessingOperator existing = load(query, mapper);
    if (existing != null) {
      if (existing instanceof ListPostProcessingOperator) {
        processor = list(GuavaUtils.concat(processor, ((ListPostProcessingOperator) existing).getProcessors()));
      } else {
        processor = list(Arrays.asList(processor, existing));
      }
    }
    return query.withOverriddenContext(Query.POST_PROCESSING, processor);
  }

  public static PostProcessingOperator list(List<PostProcessingOperator> processors)
  {
    return processors.size() == 1 ? processors.get(0) : new ListPostProcessingOperator(processors);
  }

  @SuppressWarnings("unchecked")
  public static Class<?> returns(Query query, ObjectMapper mapper)
  {
    return returns(load(query, mapper));
  }

  public static Class<?> returns(PostProcessingOperator processing)
  {
    if (processing instanceof PostProcessingOperator.ReturnsArray) {
      return Object[].class;
    } else if (processing instanceof PostProcessingOperator.ReturnsRow) {
      return Row.class;
    } else if (processing instanceof PostProcessingOperator.ReturnsMap) {
      return Map.class;
    } else if (processing instanceof ListPostProcessingOperator) {
      for (PostProcessingOperator element : ((ListPostProcessingOperator<?>) processing).getProcessorsInReverse()) {
        Class<?> returns = returns(element);
        if (returns != null) {
          return returns;
        }
      }
    }
    return null;
  }
}
