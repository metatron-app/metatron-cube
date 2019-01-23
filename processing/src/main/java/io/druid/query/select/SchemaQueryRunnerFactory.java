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

package io.druid.query.select;

import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class SchemaQueryRunnerFactory extends QueryRunnerFactory.Abstract<Schema, SchemaQuery>
{
  @Inject
  public SchemaQueryRunnerFactory(SchemaQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public QueryRunner<Schema> createRunner(final Segment segment, Future<Object> optimizer)
  {
    return new QueryRunner<Schema>()
    {
      @Override
      public Sequence<Schema> run(Query<Schema> query, Map<String, Object> responseContext)
      {
        return Sequences.of(segment.asSchema(true));
      }
    };
  }
}