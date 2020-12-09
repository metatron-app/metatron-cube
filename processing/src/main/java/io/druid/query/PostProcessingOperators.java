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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.IdentityFunction;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.Pair;
import io.druid.query.PostProcessingOperator.UnionSupport;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class PostProcessingOperators
{
  public static <T> QueryRunner<T> wrap(final QueryRunner<T> baseRunner)
  {
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        PostProcessingOperator processor = load(query);
        if (processor != null) {
          String contextKey = BaseQuery.isBrokerSide(query) ? Query.POST_PROCESSING : Query.LOCAL_POST_PROCESSING;
          return processor.postProcess(baseRunner).run(query.withOverriddenContext(contextKey, null), responseContext);
        }
        return baseRunner.run(query, responseContext);
      }
    };
  }

  public static <T> QueryRunner<T> wrap(final UnionAllQueryRunner<T> baseRunner, final QuerySegmentWalker walker)
  {
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        final UnionAllQuery unionAll = (UnionAllQuery) query;
        final PostProcessingOperator postProcessing = load(query);

        final QueryRunner<T> runner;
        if (postProcessing != null && postProcessing.supportsUnionProcessing()) {
          runner = ((UnionSupport<T>) postProcessing).postProcess(baseRunner, walker.getExecutor());
        } else {
          QueryRunner<T> merged = new QueryRunner<T>()
          {
            @Override
            public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
            {
              Sequence<Sequence<T>> sequences = Sequences.map(
                  baseRunner.run(query, responseContext), Pair.<Query<T>, Sequence<T>>rhsFn()
              );
              if (unionAll.isSortOnUnion()) {
                return QueryUtils.mergeSort(query, sequences);
              }
              return Sequences.concat(sequences);
            }
          };
          runner = postProcessing == null ? merged : postProcessing.postProcess(merged);
        }
        Sequence<T> sequence = runner.run(query, responseContext);
        final int limit = unionAll.getLimit();
        if (limit > 0 && limit < Integer.MAX_VALUE) {
          sequence = Sequences.limit(sequence, limit);
        }
        return sequence;
      }
    };
  }

  public static PostProcessingOperator convert(ObjectMapper mapper, Object value)
  {
    return mapper.convertValue(value, PostProcessingOperator.TYPE_REF);
  }

  public static RowSignature resove(Query<?> query, RowSignature source)
  {
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query);
    if (postProcessor instanceof RowSignature.Evolving) {
      source = ((RowSignature.Evolving) postProcessor).evolve(query, source);
    }
    return source;
  }

  public static List<String> resove(Query<?> query, List<String> source)
  {
    PostProcessingOperator postProcessor = PostProcessingOperators.load(query);
    if (postProcessor instanceof RowSignature.Evolving) {
      source = ((RowSignature.Evolving) postProcessor).evolve(source);
    }
    return source;
  }

  private static PostProcessingOperator load(Query<?> query)
  {
    Map<String, Object> context = query.getContext();
    String contextKey = BaseQuery.isBrokerSide(query) ? Query.POST_PROCESSING : Query.LOCAL_POST_PROCESSING;
    return context == null ? null : (PostProcessingOperator) context.get(contextKey);
  }

  @SuppressWarnings("unchecked")
  public static <Q extends Query<T>, T> Q append(Q query, PostProcessingOperator processor)
  {
    return (Q) query.withOverriddenContext(append(query.getContext(), processor));
  }

  public static Map<String, Object> append(Map<String, Object> context, PostProcessingOperator processor)
  {
    return append(context, Query.POST_PROCESSING, processor);
  }

  public static Map<String, Object> appendLocal(Map<String, Object> context, PostProcessingOperator processor)
  {
    return append(context, Query.LOCAL_POST_PROCESSING, processor);
  }

  private static Map<String, Object> append(
      Map<String, Object> context,
      String key,
      PostProcessingOperator processor
  )
  {
    context = context == null ? Maps.newHashMap() : Maps.newHashMap(context);
    PostProcessingOperator existing = context == null ? null : (PostProcessingOperator) context.get(key);
    if (existing != null) {
      if (existing instanceof ListPostProcessingOperator) {
        processor = list(GuavaUtils.concat(((ListPostProcessingOperator<?>) existing).getProcessors(), processor));
      } else {
        processor = list(Arrays.asList(existing, processor));
      }
    }
    context.put(key, processor);
    return context;
  }

  @SuppressWarnings("unchecked")
  public static <T> Query prepend(Query<T> query, PostProcessingOperator processor)
  {
    PostProcessingOperator existing = load(query);
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

  public static Class<?> returns(Query query)
  {
    return returns(load(query));
  }

  private static Class<?> returns(PostProcessingOperator processing)
  {
    if (processing instanceof PostProcessingOperator.ReturnRowAs) {
      return ((PostProcessingOperator.ReturnRowAs) processing).rowClass();
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

  public static PostProcessingOperator rewrite(
      PostProcessingOperator processor,
      IdentityFunction<PostProcessingOperator> converter
  )
  {
    if (processor instanceof ListPostProcessingOperator) {
      List<PostProcessingOperator> rewritten = Lists.newArrayList();
      for (PostProcessingOperator element : ((ListPostProcessingOperator<?>) processor).getProcessors()) {
        rewritten.add(converter.apply(element));
      }
      return PostProcessingOperators.list(rewritten);
    } else {
      return converter.apply(processor);
    }
  }
}
