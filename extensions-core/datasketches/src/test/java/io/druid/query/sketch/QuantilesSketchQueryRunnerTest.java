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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.TestIndex;
import io.druid.segment.VirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.Interval;
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
@SuppressWarnings("unchecked")
public class QuantilesSketchQueryRunnerTest extends SketchQueryRunnerTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public QuantilesSketchQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testSketchResultSerDe() throws Exception
  {
    SketchHandler handler = SketchOp.QUANTILE.handler();
    int nomEntries = 16;
    TypedSketch union1 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union1, "automotive");
    handler.updateWithValue(union1, "business");
    handler.updateWithValue(union1, "entertainment");
    handler.updateWithValue(union1, "health");
    handler.updateWithValue(union1, "mezzanine");
    handler.updateWithValue(union1, "news");
    ItemsSketch sketch1 = (ItemsSketch) handler.toSketch(union1).value();
    TypedSketch union2 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union2, "automotive1");
    handler.updateWithValue(union2, "automotive2");
    handler.updateWithValue(union2, "automotive3");
    handler.updateWithValue(union2, "business1");
    handler.updateWithValue(union2, "business2");
    handler.updateWithValue(union2, "business3");
    handler.updateWithValue(union2, "entertainment1");
    handler.updateWithValue(union2, "entertainment2");
    handler.updateWithValue(union2, "entertainment3");
    handler.updateWithValue(union2, "health1");
    handler.updateWithValue(union2, "health2");
    handler.updateWithValue(union2, "health3");
    handler.updateWithValue(union2, "mezzanine1");
    handler.updateWithValue(union2, "mezzanine2");
    handler.updateWithValue(union2, "mezzanine3");
    handler.updateWithValue(union2, "news1");
    handler.updateWithValue(union2, "news2");
    handler.updateWithValue(union2, "news3");
    handler.updateWithValue(union2, "premium1");
    handler.updateWithValue(union2, "premium2");
    handler.updateWithValue(union2, "premium3");
    ItemsSketch sketch2 = (ItemsSketch) handler.toSketch(union2).value();

    Map<String, Object> sketches = ImmutableMap.<String, Object>of("quality1", sketch1, "quality2", sketch2);
    Result<Map<String, Object>> result = new Result<>(new DateTime("2016-12-14T16:08:00"), sketches);

    String serialized = JSON_MAPPER.writeValueAsString(result);
    Assert.assertEquals(
        "{\"timestamp\":\"2016-12-14T16:08:00.000Z\","
        + "\"result\":{"
        + "\"quality1\":\"AgMICBAAAAAGAAAAAAAAAAoAAABhdXRvbW90aXZlBAAAAG5ld3MKAAAAYXV0b21vdGl2ZQgAAABidXNpbmVzcw0AAABlbnRlcnRhaW5tZW50BgAAAGhlYWx0aAkAAABtZXp6YW5pbmUEAAAAbmV3cw==\","
        + "\"quality2\":\"AgMICBAAAAAVAAAAAAAAAAsAAABhdXRvbW90aXZlMQgAAABwcmVtaXVtMwsAAABhdXRvbW90aXZlMQsAAABhdXRvbW90aXZlMgsAAABhdXRvbW90aXZlMwkAAABidXNpbmVzczEJAAAAYnVzaW5lc3MyCQAAAGJ1c2luZXNzMw4AAABlbnRlcnRhaW5tZW50MQ4AAABlbnRlcnRhaW5tZW50Mg4AAABlbnRlcnRhaW5tZW50MwcAAABoZWFsdGgxBwAAAGhlYWx0aDIHAAAAaGVhbHRoMwoAAABtZXp6YW5pbmUxCgAAAG1lenphbmluZTIKAAAAbWV6emFuaW5lMwUAAABuZXdzMQUAAABuZXdzMgUAAABuZXdzMwgAAABwcmVtaXVtMQgAAABwcmVtaXVtMggAAABwcmVtaXVtMw==\"}}",
        serialized
    );
    Result<Map<String, Object>> deserialized = JSON_MAPPER.readValue(
        serialized,
        new TypeReference<Result<Map<String, Object>>>()
        {
        }
    );
    assertEqual(sketch1, ThetaOperations.deserializeQuantile(deserialized.getValue().get("quality1"), ValueDesc.STRING));
    assertEqual(sketch2, ThetaOperations.deserializeQuantile(deserialized.getValue().get("quality2"), ValueDesc.STRING));

    Map<String, Object> object = ImmutableMap.<String, Object>builder()
                .put("queryType", "sketch")
                .put("dataSource", "ds")
                .put("intervals", new Interval("2000-01-01/2010-01-01"))
                .put("dimensions", Arrays.asList("partCol"))
                .put("sketchOp", "QUANTILE")
                .put(
                    "context",
                    ImmutableMap.of(
                        "postProcessing",
                        ImmutableMap.of(
                            "type", "sketch.quantiles",
                            "op", "QUANTILES",
                            "evenSpaced", 10
                        )
                    )
                )
                .build();

    System.out.println(JSON_MAPPER.convertValue(object, Query.class));
  }

  @Test
  public void testSketchMergeFunction() throws Exception
  {
    SketchHandler handler = SketchOp.QUANTILE.handler();
    int nomEntries = 16;
    TypedSketch union1 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union1, "automotive");
    handler.updateWithValue(union1, "business");
    handler.updateWithValue(union1, "entertainment");
    handler.updateWithValue(union1, "health");
    handler.updateWithValue(union1, "mezzanine");
    handler.updateWithValue(union1, "news");
    TypedSketch<ItemsSketch> sketch1 = handler.toSketch(union1);
    Assert.assertEquals("health", sketch1.value().getQuantile(0.5f));
    TypedSketch union2 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "mezzanine");
    handler.updateWithValue(union2, "mezzanine");
    TypedSketch<ItemsSketch> sketch2 = handler.toSketch(union2);
    Assert.assertEquals("premium", sketch2.value().getQuantile(0.5f));

    Result<Map<String, Object>> merged =
        new SketchBinaryFn(nomEntries, handler).apply(
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch1)
            ),
            new Result<Map<String, Object>>(
                new DateTime(0),
                    ImmutableMap.<String, Object>of("quality", sketch2)
            )
        );

    TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) merged.getValue().get("quality");
    Assert.assertEquals("mezzanine", sketch.value().getQuantile(0.5f));
  }

  @Test
  public void testSketchQuery() throws Exception
  {
    SketchQuery baseQuery = new SketchQuery(
        TableDataSource.of(dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null, null, DefaultDimensionSpec.toSpec("market", "quality"), null, 16, SketchOp.QUANTILE, null
    );

    for (boolean includeMetric : new boolean[] {false, true}) {
      SketchQuery query = baseQuery.withOverriddenContext(
          ImmutableMap.<String, Object>of(Query.ALL_METRICS_FOR_EMPTY, includeMetric)
      );
      List<Result<Map<String, Object>>> result = Sequences.toList(
          query.run(segmentWalker, Maps.<String, Object>newHashMap())
      );
      Assert.assertEquals(1, result.size());
      Map<String, Object> values = result.get(0).getValue();
      TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values.get("market");
      TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values.get("quality");
      Assert.assertEquals("spot", sketch1.value().getQuantile(0.5d));
      final Object quantile = sketch2.value().getQuantile(0.5d);
      Assert.assertTrue(Sets.<Object>newHashSet("mezzanine", "news").contains(quantile));
    }
  }

  @Test
  public void testSketchQueryWithFilter() throws Exception
  {
    SketchQuery query = new SketchQuery(
        TableDataSource.of(dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        DimFilters.and(
            BoundDimFilter.gt("market", "spot"),
            BoundDimFilter.lte("quality", "premium")
        ),
        null,
        DefaultDimensionSpec.toSpec("market", "quality"),
        null,
        16,
        SketchOp.QUANTILE,
        ImmutableMap.<String, Object>of(Query.ALL_METRICS_FOR_EMPTY, false)
    );

    List<Result<Map<String, Object>>> result = Sequences.toList(
        query.run(segmentWalker, Maps.<String, Object>newHashMap())
    );
    Assert.assertEquals(1, result.size());
    Map<String, Object> values = result.get(0).getValue();
    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values.get("market");
    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values.get("quality");
    Assert.assertEquals("total_market", sketch1.value().getQuantile(0.3d));
    Assert.assertEquals("upfront", sketch1.value().getQuantile(0.8d));
    Assert.assertEquals("mezzanine", sketch2.value().getQuantile(0.4d));

    SketchQuery postProcessing = query.withOverriddenContext(
        ImmutableMap.<String, Object>of(
            Query.POST_PROCESSING,
            new SketchQuantilesProcessor(QuantileOperation.QUANTILES, null, 2, null, null, null)
        )
    );

    result = Sequences.toList(postProcessing.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(1, result.size());
    values = result.get(0).getValue();
    Assert.assertArrayEquals(new String[]{"total_market", "upfront"}, (String[]) values.get("market"));
    Assert.assertArrayEquals(new String[]{"mezzanine", "premium"}, (String[]) values.get("quality"));
  }

  @Test
  public void testSketchQueryWithVirtualColumnAndMetric() throws Exception
  {
    SketchQuery query = new SketchQuery(
        new TableDataSource(dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        DimFilters.and(
            BoundDimFilter.gt("market_aa", "spot_aa"),
            BoundDimFilter.lte("quality", "premium")
        ),
        Arrays.<VirtualColumn>asList(new ExprVirtualColumn("market + '_aa'", "market_aa")),
        DefaultDimensionSpec.toSpec("market_aa", "quality"),
        Arrays.asList("index"),
        16,
        SketchOp.QUANTILE,
        null
    );

    List<Result<Map<String, Object>>> result = Sequences.toList(
        query.run(segmentWalker, Maps.<String, Object>newHashMap())
    );
    Assert.assertEquals(1, result.size());
    Map<String, Object> values = result.get(0).getValue();

    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values.get("market_aa");
    Assert.assertEquals("total_market_aa", sketch1.value().getQuantile(0.3d));
    Assert.assertEquals("upfront_aa", sketch1.value().getQuantile(0.8d));

    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values.get("quality");
    Assert.assertEquals("mezzanine", sketch2.value().getQuantile(0.4d));

    TypedSketch<ItemsSketch> sketch3 = (TypedSketch<ItemsSketch>) values.get("index");
    Assert.assertEquals(545, ((Number) sketch3.value().getMinValue()).doubleValue(), 100);
    Assert.assertEquals(1044, ((Number) sketch3.value().getQuantile(0.4d)).doubleValue(), 100);
    Assert.assertEquals(1251, ((Number) sketch3.value().getQuantile(0.8d)).doubleValue(), 100);
    Assert.assertEquals(1870, ((Number) sketch3.value().getMaxValue()).doubleValue(), 100);

    ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new SketchModule().getJacksonModules()) {
      mapper = mapper.registerModule(module);
    }
    byte[] serialized = mapper.writeValueAsBytes(result.get(0));
    Result<Map<String, Object>> deserialized = toolChest.makePreComputeManipulatorFn(query, null).apply(
        mapper.<Result<Map<String, Object>>>readValue(serialized, toolChest.getResultTypeReference())
    );
    Assert.assertNotNull("never", deserialized);
    TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) deserialized.getValue().get("index");
    Assert.assertEquals(545, ((Number) sketch.value().getMinValue()).doubleValue(), 100);
    Assert.assertEquals(1044, ((Number) sketch.value().getQuantile(0.4d)).doubleValue(), 100);
    Assert.assertEquals(1251, ((Number) sketch.value().getQuantile(0.8d)).doubleValue(), 100);
    Assert.assertEquals(1870, ((Number) sketch.value().getMaxValue()).doubleValue(), 100);
  }

  private void assertEqual(ItemsSketch expected, ItemsSketch result)
  {
    System.out.println(expected.toString(true, true));
    Assert.assertEquals(expected.toString(true, true), result.toString(true, true));
  }
}
