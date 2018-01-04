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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class DimensionSelectorHavingSpecTest
{
  private Row getTestRow(Object dimensionValue)
  {
    return new MapBasedRow(0, ImmutableMap.of("dimension", dimensionValue));
  }

  @Test
  public void testDimSelectorHavingClauseSerde() throws Exception
  {
    HavingSpec dimHavingSpec = new DimensionSelectorHavingSpec("dim", "v", null);

    Map<String, Object> dimSelectMap = ImmutableMap.<String, Object>of(
        "type", "dimSelector",
        "dimension", "dim",
        "value", "v"
    );

    ObjectMapper mapper = new DefaultObjectMapper();
    assertEquals(dimHavingSpec, mapper.convertValue(dimSelectMap, DimensionSelectorHavingSpec.class));
  }

  @Test
  public void testEquals() throws Exception
  {
    ExtractionFn extractionFn1 = new RegexDimExtractionFn("^([^,]*),", false, "");
    ExtractionFn extractionFn2 = new RegexDimExtractionFn(",(.*)", false, "");
    ExtractionFn extractionFn3 = new RegexDimExtractionFn("^([^,]*),", false, "");

    HavingSpec dimHavingSpec1 = new DimensionSelectorHavingSpec("dim", "v", extractionFn1);
    HavingSpec dimHavingSpec2 = new DimensionSelectorHavingSpec("dim", "v", extractionFn3);
    HavingSpec dimHavingSpec3 = new DimensionSelectorHavingSpec("dim1", "v", null);
    HavingSpec dimHavingSpec4 = new DimensionSelectorHavingSpec("dim2", "v", null);
    HavingSpec dimHavingSpec5 = new DimensionSelectorHavingSpec("dim", "v1", null);
    HavingSpec dimHavingSpec6 = new DimensionSelectorHavingSpec("dim", "v2", null);
    HavingSpec dimHavingSpec7 = new DimensionSelectorHavingSpec("dim", null, null);
    HavingSpec dimHavingSpec8 = new DimensionSelectorHavingSpec("dim", null, null);
    HavingSpec dimHavingSpec9 = new DimensionSelectorHavingSpec("dim1", null, null);
    HavingSpec dimHavingSpec10 = new DimensionSelectorHavingSpec("dim2", null, null);
    HavingSpec dimHavingSpec11 = new DimensionSelectorHavingSpec("dim1", "v", null);
    HavingSpec dimHavingSpec12 = new DimensionSelectorHavingSpec("dim2", null, null);
    HavingSpec dimHavingSpec13 = new DimensionSelectorHavingSpec("dim", "value", extractionFn1);
    HavingSpec dimHavingSpec14 = new DimensionSelectorHavingSpec("dim", "value", extractionFn2);

    assertEquals(dimHavingSpec1, dimHavingSpec2);
    assertNotEquals(dimHavingSpec3, dimHavingSpec4);
    assertNotEquals(dimHavingSpec5, dimHavingSpec6);
    assertEquals(dimHavingSpec7, dimHavingSpec8);
    assertNotEquals(dimHavingSpec9, dimHavingSpec10);
    assertNotEquals(dimHavingSpec11, dimHavingSpec12);
    assertNotEquals(dimHavingSpec13, dimHavingSpec14);

  }

  @Test
  public void testToString()
  {
    ExtractionFn extractionFn = new RegexDimExtractionFn("^([^,]*),", false, "");
    String expected = "DimensionSelectorHavingSpec{" +
                                 "dimension='gender'," +
                                 " value='m'," +
                                 " extractionFunction='regex(/^([^,]*),/, 1)'}";
    Assert.assertEquals(new DimensionSelectorHavingSpec("gender", "m", extractionFn).toString(), expected);
    
    expected = "DimensionSelectorHavingSpec{" +
                                 "dimension='gender'," +
                                 " value='m'," +
                                 " extractionFunction='Identity'}";
    
    Assert.assertEquals(new DimensionSelectorHavingSpec("gender", "m", null).toString(), expected);
  }

  @Test(expected = NullPointerException.class)
  public void testNullDimension()
  {
    new DimensionSelectorHavingSpec(null, "value", null);
  }

  @Test
  public void testDimensionFilterSpec()
  {
    DimensionSelectorHavingSpec spec = new DimensionSelectorHavingSpec("dimension", "v", null);
    Predicate<Row> predicate = spec.toEvaluator(null, ImmutableList.<AggregatorFactory>of());
    assertTrue(predicate.apply(getTestRow("v")));
    assertTrue(predicate.apply(getTestRow(ImmutableList.of("v", "v1"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of())));
    assertFalse(predicate.apply(getTestRow("v1")));

    spec = new DimensionSelectorHavingSpec("dimension", null, null);
    predicate = spec.toEvaluator(null, ImmutableList.<AggregatorFactory>of());
    assertTrue(predicate.apply(getTestRow(ImmutableList.of())));
    assertTrue(predicate.apply(getTestRow(ImmutableList.of(""))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v", "v1"))));

    spec = new DimensionSelectorHavingSpec("dimension", "", null);
    predicate = spec.toEvaluator(null, ImmutableList.<AggregatorFactory>of());
    assertTrue(predicate.apply(getTestRow(ImmutableList.of())));
    assertTrue(predicate.apply(getTestRow(ImmutableList.of(""))));
    assertTrue(predicate.apply(getTestRow(ImmutableList.of("v", "v1", ""))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v", "v1"))));

    ExtractionFn extractionFn = new RegexDimExtractionFn("^([^,]*),", true, "default");
    spec = new DimensionSelectorHavingSpec("dimension", "v", extractionFn);
    predicate = spec.toEvaluator(null, ImmutableList.<AggregatorFactory>of());
    assertTrue(predicate.apply(getTestRow(ImmutableList.of("v,v1", "v2,v3"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v1,v4"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v"))));
    assertFalse(predicate.apply(getTestRow(ImmutableList.of("v1", "default"))));
    assertTrue(predicate.apply(getTestRow(ImmutableList.of("v,default", "none"))));
    
    spec = new DimensionSelectorHavingSpec("dimension", "default", extractionFn);
    predicate = spec.toEvaluator(null, ImmutableList.<AggregatorFactory>of());
    assertTrue(predicate.apply(getTestRow(ImmutableList.of("v1,v2", "none")))); 
  }
}
