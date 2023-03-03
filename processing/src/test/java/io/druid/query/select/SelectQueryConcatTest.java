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

package io.druid.query.select;

import com.google.common.io.CharSource;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class SelectQueryConcatTest
{
  QueryRunnerFactory<Result<SelectResultValue>> factory = TestHelper.factoryFor(SelectQuery.class);

  private final CharSource input = CharSource.wrap(
      "2011-01-12T00:00:00.000Z\tspot\tAutoMotive\tPREFERRED\ta\u0001preferred\t100.000000\n" +
          "2011-01-12T00:00:00.000Z\tSPot\tbusiness\tpreferred\tb\u0001Preferred\t100.000000\n" +
          "2011-01-12T00:00:00.000Z\tspot\tentertainment\tPREFERRed\te\u0001preferred\t100.000000\n" +
          "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\ta\u0001preferred\t94.874713"
  );

  private final IncrementalIndex index = TestIndex.makeRealtimeIndex(input, true);

  private final QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunner(factory, new IncrementalIndexSegment(
      index, DataSegment.asKey("index")));

  private Druids.SelectQueryBuilder testBuilder()
  {
    return Druids.newSelectQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .pagingSpec(PagingSpec.newSpec(5));
  }

  @Test
  public void testSelect()
  {
    Druids.SelectQueryBuilder builder = testBuilder();
    SelectQuery selectQuery;
    HashMap<String, List> context = new HashMap<>();
    String concatString = ":";

    selectQuery = builder.build();
    List<Result<SelectResultValue>> results = Sequences.toList(
        queryRunner.run(selectQuery, context)
    );

    Assert.assertTrue(results.get(0).getValue().getEvents().get(0).getEvent().get(QueryRunnerTestHelper.placementishDimension) instanceof List);

    selectQuery = builder.concatString(concatString).build();

    List<Result<SelectResultValue>> concatResults = Sequences.toList(
        queryRunner.run(selectQuery, context)
    );

    Assert.assertTrue(concatResults.get(0).getValue().getEvents().get(0).getEvent().get(QueryRunnerTestHelper.placementishDimension) instanceof String);
    Assert.assertTrue(((String)concatResults.get(0).getValue().getEvents().get(0).getEvent().get(QueryRunnerTestHelper.placementishDimension)).contains(concatString));
  }
}
