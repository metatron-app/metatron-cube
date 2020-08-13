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

package io.druid.query.sketch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.yahoo.sketches.frequencies.ItemsSketch;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.TestIndex;
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
public class FrequencySketchQueryRunnerTest extends SketchQueryRunnerTestHelper
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public FrequencySketchQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testSketchResultSerDe() throws Exception
  {
    SketchHandler handler = SketchOp.FREQUENCY.handler();
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

    Object[] result = new Object[] {new DateTime("2016-12-14T16:08:00").getMillis(), sketch1, sketch2};

    String serialized = JSON_MAPPER.writeValueAsString(result);
    Assert.assertEquals(
        "[1481731680000," +
        "\"BAEKBAMAAAAGAAAAAAAAAAYAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAANAAAAZW50ZXJ0YWlubWVudAkAAABtZXp6YW5pbmUEAAAAbmV3cwgAAABidXNpbmVzcwoAAABhdXRvbW90aXZlBgAAAGhlYWx0aA==\"," +
        "\"BAEKBAQAAAAIAAAAAAAAABUAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAAAAAACAAAAHByZW1pdW0xCgAAAG1lenphbmluZTIFAAAAbmV3czMFAAAAbmV3czIIAAAAcHJlbWl1bTIKAAAAbWV6emFuaW5lMwUAAABuZXdzMQgAAABwcmVtaXVtMw==\"]",
        serialized
    );
    Object[] deserialized = JSON_MAPPER.readValue(serialized, new TypeReference<Object[]>() {});
    assertEqual(sketch1, ThetaOperations.deserializeFrequency(deserialized[1], ValueDesc.STRING));
    assertEqual(sketch2, ThetaOperations.deserializeFrequency(deserialized[2], ValueDesc.STRING));

    Map<String, Object> object = ImmutableMap.<String, Object>builder()
                .put("queryType", "sketch")
                .put("dataSource", "ds")
                .put("intervals", new Interval("2000-01-01/2010-01-01"))
                .put("dimensions", Arrays.asList("partCol"))
                .put("sketchOp", "FREQUENCY")
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
    SketchHandler handler = SketchOp.FREQUENCY.handler();
    int nomEntries = 16;
    TypedSketch union1 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union1, "automotive");
    handler.updateWithValue(union1, "business");
    handler.updateWithValue(union1, "entertainment");
    handler.updateWithValue(union1, "health");
    handler.updateWithValue(union1, "mezzanine");
    handler.updateWithValue(union1, "news");
    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) handler.toSketch(union1);
    Assert.assertEquals(1L, sketch1.value().getEstimate("mezzanine"));
    TypedSketch union2 = handler.newUnion(nomEntries, ValueDesc.STRING, null);
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "premium");
    handler.updateWithValue(union2, "mezzanine");
    handler.updateWithValue(union2, "mezzanine");
    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) handler.toSketch(union2);
    Assert.assertEquals(2L, sketch2.value().getEstimate("mezzanine"));

    Object[] merged = new SketchBinaryFn(nomEntries, handler).apply(new Object[]{0, sketch1}, new Object[]{0, sketch2});

    TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) merged[1];
    Assert.assertEquals(3L, sketch.value().getEstimate("mezzanine"));
  }

  @Test
  public void testSketchQuery() throws Exception
  {
    SketchQuery baseQuery = new SketchQuery(
        new TableDataSource(dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        null,
        null,
        DefaultDimensionSpec.toSpec("market", "quality"),
        null, 16, SketchOp.FREQUENCY, null
    );

    for (boolean includeMetric : new boolean[] {false, true}) {
      SketchQuery query = baseQuery.withOverriddenContext(
          ImmutableMap.<String, Object>of(Query.ALL_METRICS_FOR_EMPTY, includeMetric)
      );
      List<Object[]> result = Sequences.toList(
          query.run(segmentWalker, Maps.<String, Object>newHashMap())
      );
      Assert.assertEquals(1, result.size());
      Object[] values = result.get(0);
      TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values[1];
      sketch1 = TypedSketch.deserialize(
          baseQuery.getSketchOp(), sketch1.toByteArray(), GuavaUtils.nullFirstNatural()
      );
      System.out.println(sketch1);
      Assert.assertEquals(187L, sketch1.value().getEstimate("upfront"), 5);
      Assert.assertEquals(838L, sketch1.value().getEstimate("spot"), 5);
      Assert.assertEquals(187L, sketch1.value().getEstimate("total_market"), 5);

      TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values[2];
      System.out.println(sketch2);
      sketch2 = TypedSketch.deserialize(
          baseQuery.getSketchOp(), sketch2.toByteArray(), GuavaUtils.nullFirstNatural()
      );
      Assert.assertEquals(94L, sketch2.value().getEstimate("entertainment"), 5);
      Assert.assertEquals(280L, sketch2.value().getEstimate("mezzanine"), 5);
      Assert.assertEquals(280L, sketch2.value().getEstimate("premium"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("business"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("news"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("technology"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("automotive"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("travel"), 5);
      Assert.assertEquals(94L, sketch2.value().getEstimate("health"), 5);
    }
  }

    @Test
  public void testSketchQueryWithFilter() throws Exception
  {
    SketchQuery query = new SketchQuery(
        new TableDataSource(dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        DimFilters.and(
            BoundDimFilter.gt("market", "spot"),
            BoundDimFilter.lte("quality", "premium")
        ),
        null,
        DefaultDimensionSpec.toSpec("market", "quality"),
        null, 16, SketchOp.FREQUENCY, null
    );

    List<Object[]> result = Sequences.toList(
        query.run(segmentWalker, Maps.<String, Object>newHashMap())
    );
    Assert.assertEquals(1, result.size());
    Object[] values = result.get(0);
    TypedSketch<ItemsSketch> sketch1 = (TypedSketch<ItemsSketch>) values[1];
    System.out.println(sketch1);
    sketch1 = TypedSketch.deserialize(
        query.getSketchOp(), sketch1.toByteArray(), GuavaUtils.nullFirstNatural()
    );

    Assert.assertEquals(186L, sketch1.value().getEstimate("upfront"), 5);
    Assert.assertEquals(186L, sketch1.value().getEstimate("total_market"), 5);

    TypedSketch<ItemsSketch> sketch2 = (TypedSketch<ItemsSketch>) values[2];
    System.out.println(sketch2);
    sketch2 = TypedSketch.deserialize(
        query.getSketchOp(), sketch2.toByteArray(), GuavaUtils.nullFirstNatural()
    );

    Assert.assertEquals(186L, sketch2.value().getEstimate("mezzanine"), 5);
    Assert.assertEquals(186L, sketch2.value().getEstimate("premium"), 5);
  }

  private void assertEqual(ItemsSketch expected, ItemsSketch result)
  {
    Assert.assertEquals(expected.toString(), result.toString());
  }
}
