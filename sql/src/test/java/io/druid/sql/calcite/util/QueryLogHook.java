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

package io.druid.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.sql.calcite.CalciteQueryTestHelper;
import org.apache.calcite.runtime.Hook;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;
import java.util.function.Consumer;

/**
 * JUnit Rule that adds a Calcite hook to log and remember Druid queries.
 */
public class QueryLogHook implements TestRule, Consumer<Object>
{
  private static final Logger log = new Logger(QueryLogHook.class);

  private final List<Query> recordedQueries = Lists.newCopyOnWriteArrayList();

  public static QueryLogHook create()
  {
    return new QueryLogHook();
  }

  public void clearRecordedQueries()
  {
    recordedQueries.clear();
  }

  public List<Query> getRecordedQueries()
  {
    return ImmutableList.copyOf(recordedQueries);
  }

  @Override
  public Statement apply(final Statement base, final Description description)
  {
    return new Statement()
    {
      @Override
      public void evaluate() throws Throwable
      {
        clearRecordedQueries();
        try (final Hook.Closeable unhook = Hook.QUERY_PLAN.add(QueryLogHook.this)) {
          base.evaluate();
        }
      }
    };
  }

  @Override
  public void accept(Object query)
  {
    recordedQueries.add(
        Queries.iterate((Query) query, q -> q.withOverriddenContext(CalciteQueryTestHelper.REMOVER))
    );
  }
}
