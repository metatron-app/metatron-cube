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

package io.druid.query.sketch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.query.aggregation.datasketches.theta.SketchOperations;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SketchQueryRunnerTest
{
  private static final SketchQueryQueryToolChest toolChest = new SketchQueryQueryToolChest(
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SketchQueryRunnerFactory(
                toolChest,
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private final QueryRunner<Result<Map<String, Object>>> runner;

  @SuppressWarnings("unchecked")
  public SketchQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testSketchResultSerDe() throws Exception
  {
    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    for (Module module : new SketchModule().getJacksonModules()) {
      mapper = mapper.registerModule(module);
    }
    int nomEntries = 16;
    Union union1 = (Union) SetOperation.builder().build(nomEntries, Family.UNION);
    union1.update("automotive");
    union1.update("business");
    union1.update("entertainment");
    union1.update("health");
    union1.update("mezzanine");
    union1.update("news");
    Sketch sketch1 = union1.getResult(true, null);
    Union union2 = (Union) SetOperation.builder().build(nomEntries, Family.UNION);
    union2.update("automotive1");
    union2.update("automotive2");
    union2.update("automotive3");
    union2.update("business1");
    union2.update("business2");
    union2.update("business3");
    union2.update("entertainment1");
    union2.update("entertainment2");
    union2.update("entertainment3");
    union2.update("health1");
    union2.update("health2");
    union2.update("health3");
    union2.update("mezzanine1");
    union2.update("mezzanine2");
    union2.update("mezzanine3");
    union2.update("news1");
    union2.update("news2");
    union2.update("news3");
    union2.update("premium1");
    union2.update("premium2");
    union2.update("premium3");
    Sketch sketch2 = union2.getResult(true, null);

    Map<String, Object> sketches = ImmutableMap.<String, Object>of("quality1", sketch1, "quality2", sketch2);
    Result<Map<String, Object>> result = new Result<>(new DateTime("2016-12-14T16:08:00"), sketches);

    String serialized = mapper.writeValueAsString(result);
    Assert.assertEquals(
        "{\"timestamp\":\"2016-12-14T16:08:00.000Z\","
        + "\"result\":{"
        + "\"quality1\":\"AgMDAAAazJMGAAAAAACAPwKobNnmhVcIRjb8GcPp4QosSoEtTHTHELg4+/gHt6lvG9qOWCPXz3UtwWAW4Pebew==\","
        + "\"quality2\":\"AwMDAAAazJMQAAAAAACAP9b33ETElj9iJamXpuMfvwZELjTL3JWJEG/31isLqBQS+UTkQnu0wSPfbj6za7/1JAbAyn8cTkMmevQWEYNNzSrz7fgK+wwuK7lGzxKorcQ3i4l4XrfKujlQQMiQkLixP1xsYb8rqeNCGmbo70PGAEXTqJyjf4Z4TxdkRE2hslxSbc0dsIVOylg=\"}}",
        serialized
    );
    Result<Map<String, Object>> deserialized = mapper.readValue(
        serialized,
        new TypeReference<Result<Map<String, Object>>>()
        {
        }
    );
    assertEqual(sketch1, SketchOperations.deserialize(deserialized.getValue().get("quality1")));
    assertEqual(sketch2, SketchOperations.deserialize(deserialized.getValue().get("quality2")));
  }

  @Test
  public void testSketchMergeFunction() throws Exception
  {
    int nomEntries = 16;
    Union union1 = (Union) SetOperation.builder().build(nomEntries, Family.UNION);
    union1.update("automotive");
    union1.update("business");
    union1.update("entertainment");
    union1.update("health");
    union1.update("mezzanine");
    union1.update("news");
    Sketch sketch1 = union1.getResult(true, null);
    Assert.assertEquals(6, sketch1.getEstimate(), 0.001);
    Union union2 = (Union) SetOperation.builder().build(nomEntries, Family.UNION);
    union2.update("automotive");
    union2.update("health");
    union2.update("premium");
    Sketch sketch2 = union2.getResult(true, null);
    Assert.assertEquals(3, sketch2.getEstimate(), 0.001);

    Result<Map<String, Object>> merged =
        new SketchBinaryFn(nomEntries).apply(
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch1)
            ),
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch2)
            )
        );

    Sketch sketch = (Sketch) merged.getValue().get("quality");
    Assert.assertEquals(7, sketch.getEstimate(), 0.001);
  }

  @Test
  public void testSketchQuery() throws Exception
  {
    SketchQuery query = new SketchQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        Arrays.asList("market", "quality"), null, 16, null
    );

    List<Result<Map<String, Object>>> result = Sequences.toList(
        runner.run(query, null),
        Lists.<Result<Map<String, Object>>>newArrayList()
    );
    Assert.assertEquals(1, result.size());
    Map<String, Object> values = result.get(0).getValue();
    Assert.assertEquals(3, ((Sketch) values.get("market")).getEstimate(), 0.001);
    Assert.assertEquals(9, ((Sketch) values.get("quality")).getEstimate(), 0.001);
  }

  private void assertEqual(Sketch expected, Sketch result)
  {
    Assert.assertEquals(expected.toString(false, true, 8, true), result.toString(false, true, 8, true));
  }
}
