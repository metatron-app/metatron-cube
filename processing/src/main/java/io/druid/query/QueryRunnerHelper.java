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
import com.google.common.base.Function;
import io.druid.cache.Cache;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.segment.Cursor;
import io.druid.segment.CursorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 */
public class QueryRunnerHelper
{
  public static <T> Sequence<T> makeCursorBasedQuery(
      final CursorFactory factory,
      final Query<?> query,
      final Cache cache,
      final Function<Cursor, T> mapFn
  )
  {
    return Sequences.filterNull(
        Sequences.map(query.estimatedInitialColumns(), factory.makeCursors(query, cache), mapFn)
    );
  }

  public static <T> Sequence<T> makeCursorBasedQueryConcat(
      final CursorFactory factory,
      final Query<?> query,
      final Cache cache,
      final Function<Cursor, Sequence<T>> mapFn
  )
  {
    List<String> columns = query.estimatedInitialColumns();
    return Sequences.concat(
        columns, Sequences.filterNull(Sequences.map(columns, factory.makeCursors(query, cache), mapFn))
    );
  }

  public static <T> QueryRunner<T> toManagementRunner(
      Query<T> query,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService exec,
      ObjectMapper mapper
  )
  {
    QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    exec = exec == null ? Execs.newDirectExecutorService() : exec;
    return QueryRunners.finalizeAndPostProcessing(
        toolChest.mergeResults(
            factory.mergeRunners(query, exec, Arrays.asList(factory.createRunner(null, null)), null)
        ),
        toolChest,
        mapper
    );
  }
}
