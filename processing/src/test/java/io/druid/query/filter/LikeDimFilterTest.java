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

package io.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BytesRef;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.SubstringDimExtractionFn;
import io.druid.query.filter.LikeDimFilter.LikeMatcher;
import io.druid.segment.filter.Filters;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class LikeDimFilterTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = objectMapper.readValue(objectMapper.writeValueAsString(filter), DimFilter.class);
    Assert.assertEquals(filter, filter2);
  }

  @Test
  public void testGetCacheKey()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertArrayEquals(filter.getCacheKey(), filter2.getCacheKey());
    Assert.assertFalse(Arrays.equals(filter.getCacheKey(), filter3.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter, filter2);
    Assert.assertNotEquals(filter, filter3);
    Assert.assertEquals(filter.hashCode(), filter2.hashCode());
    Assert.assertNotEquals(filter.hashCode(), filter3.hashCode());
  }

  @Test
  public void testGetRequiredColumns()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(Filters.getDependents(filter), Sets.newHashSet("foo"));
  }

  @Test
  public void testParse()
  {
    LikeMatcher matcher = LikeMatcher.from("%", null);
    Assert.assertEquals(".*", matcher.regex());
    Assert.assertEquals("%", matcher.represent());
    Predicate<String> predicate1 = matcher.asPredicate();
    Predicate<BinaryRef> predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "");
    assertTrue(predicate1, predicate2, "D");
    assertTrue(predicate1, predicate2, "DU");

    matcher = LikeMatcher.from("_", null);
    Assert.assertEquals(".", matcher.regex());
    Assert.assertEquals("_", matcher.represent());
    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "");
    assertTrue(predicate1, predicate2, "D");
    assertFalse(predicate1, predicate2, "DU");

    matcher = LikeMatcher.from("__", null);
    Assert.assertEquals(".{2}", matcher.regex());
    Assert.assertEquals("_", matcher.represent());
    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "");
    assertFalse(predicate1, predicate2, "D");
    assertTrue(predicate1, predicate2, "DU");

    matcher = LikeMatcher.from("DUMMY", null);
    Assert.assertEquals("\\QDUMMY\\E", matcher.regex());
    Assert.assertEquals("L", matcher.represent());
    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "DUMMY");
    assertFalse(predicate1, predicate2, "DUMMYx");

    matcher = LikeMatcher.from("PROMO%", null);
    Assert.assertEquals("\\QPROMO\\E.*", matcher.regex());
    Assert.assertEquals("L%", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "PROMO");
    assertTrue(predicate1, predicate2, "PROMOT");
    assertFalse(predicate1, predicate2, "PROM");
    assertTrue(predicate1, predicate2, "PROMOxx");

    matcher = LikeMatcher.from("PROMO_", null);
    Assert.assertEquals("\\QPROMO\\E.", matcher.regex());
    Assert.assertEquals("L_", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "PROMO");
    assertTrue(predicate1, predicate2, "PROMOT");
    assertFalse(predicate1, predicate2, "PROM");
    assertFalse(predicate1, predicate2, "PROMOxx");

    matcher = LikeMatcher.from("PROMO__", null);
    Assert.assertEquals("\\QPROMO\\E.{2}", matcher.regex());
    Assert.assertEquals("L_", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "PROMO");
    assertFalse(predicate1, predicate2, "PROMOT");
    assertFalse(predicate1, predicate2, "PROM");
    assertTrue(predicate1, predicate2, "PROMOxx");

    matcher = LikeMatcher.from("%ECONOMY BRUSHED", null);
    Assert.assertEquals(".*\\QECONOMY BRUSHED\\E", matcher.regex());
    Assert.assertEquals("%L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "ECONOMY BRUSHED");
    assertTrue(predicate1, predicate2, "xECONOMY BRUSHED");
    assertTrue(predicate1, predicate2, "xxECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHEDx");

    matcher = LikeMatcher.from("_ECONOMY BRUSHED", null);
    Assert.assertEquals(".\\QECONOMY BRUSHED\\E", matcher.regex());
    Assert.assertEquals("_L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHED");
    assertTrue(predicate1, predicate2, "xECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "xxECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHEDx");

    matcher = LikeMatcher.from("ECONOMY%BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.*\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L%L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "ECONOMYBRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY BRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY  BRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHEDx");

    matcher = LikeMatcher.from("ECONOMY_BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMYBRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY  BRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHEDx");

    matcher = LikeMatcher.from("ECONOMY__BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.{2}\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMYBRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY  BRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHEDx");

    matcher = LikeMatcher.from("_ECONOMY%", null);
    Assert.assertEquals(".\\QECONOMY\\E.*", matcher.regex());
    Assert.assertEquals("_L%", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMYBRUSHED");
    assertTrue(predicate1, predicate2, "xECONOMY");
    assertTrue(predicate1, predicate2, "xECONOMY ");
    assertFalse(predicate1, predicate2, " xECONOMY");

    matcher = LikeMatcher.from("%ECONOMY_", null);
    Assert.assertEquals(".*\\QECONOMY\\E.", matcher.regex());
    Assert.assertEquals("%L_", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMYBRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY");
    assertTrue(predicate1, predicate2, "xECONOMY ");
    assertFalse(predicate1, predicate2, "xECONOMYx ");

    matcher = LikeMatcher.from("_ECONOMY_", null);
    Assert.assertEquals(".\\QECONOMY\\E.", matcher.regex());
    Assert.assertEquals("_L_", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMYBRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY");
    assertTrue(predicate1, predicate2, "xECONOMY ");
    assertFalse(predicate1, predicate2, "xECONOMYx ");

    matcher = LikeMatcher.from("%plum%", null);
    Assert.assertEquals(".*\\Qplum\\E.*", matcher.regex());
    Assert.assertEquals("%L%", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "turquoise plum");
    assertTrue(predicate1, predicate2, "antique plum smoke");
    assertFalse(predicate1, predicate2, "turquoise PLUM");
    assertTrue(predicate1, predicate2, "plu plum");

    matcher = LikeMatcher.from("ECONOMY%XX%BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.*\\QXX\\E.*\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L%L%L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMY BRUSH");
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMYXX BRUSHE");
    assertTrue(predicate1, predicate2, "ECONOMYXXBRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY XX BRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY  XXBRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMYXX  BRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY XX BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY XX BRUSHEDx");

    matcher = LikeMatcher.from("ECONOMY_XX_BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.\\QXX\\E.\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L_L", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertFalse(predicate1, predicate2, "ECONOMY BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMYXXBRUSHED");
    assertTrue(predicate1, predicate2, "ECONOMY XX BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY  XXBRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMYXX  BRUSHED");
    assertFalse(predicate1, predicate2, "xECONOMY XX BRUSHED");
    assertFalse(predicate1, predicate2, "ECONOMY XX BRUSHEDx");

    matcher = LikeMatcher.from("%나비스%한글%", null);
    Assert.assertEquals(".*\\Q나비스\\E.*\\Q한글\\E.*", matcher.regex());
    Assert.assertEquals("%L%L%", matcher.represent());

    predicate1 = matcher.asPredicate();
    predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "나비스한글");
    assertTrue(predicate1, predicate2, "나비스 한글");
    assertTrue(predicate1, predicate2, "뒵 나비스 한글 뵙");
    assertFalse(predicate1, predicate2, "나비스 한");
  }

  private void assertTrue(Predicate<String> predicate1, Predicate<BinaryRef> predicate2, String value)
  {
    Assert.assertTrue(predicate1.apply(value));
    if (predicate2 != null) {
      Assert.assertTrue(predicate2.apply(BytesRef.of(value)));
    }
  }

  private void assertFalse(Predicate<String> predicate1, Predicate<BinaryRef> predicate2, String value)
  {
    Assert.assertFalse(predicate1.apply(value));
    if (predicate2 != null) {
      Assert.assertFalse(predicate2.apply(BytesRef.of(value)));
    }
  }

  @Test
  public void testTPCH()
  {
    // TPCH-13
    LikeMatcher matcher = LikeMatcher.from("%unusual%accounts%", null);
    Assert.assertEquals(".*\\Qunusual\\E.*\\Qaccounts\\E.*", matcher.regex());
    Assert.assertEquals("%L%L%", matcher.represent());

    Predicate<String> predicate1 = matcher.asPredicate();
    Predicate<BinaryRef> predicate2 = matcher.asRawPredicate();
    assertTrue(predicate1, predicate2, "unusualaccounts");
    assertTrue(predicate1, predicate2, "unusual accounts");
    assertTrue(predicate1, predicate2, "un unusualaccounts");
    assertTrue(predicate1, predicate2, "un unusual ac accounts");
    assertTrue(predicate1, predicate2, "xx unusual accounts xx");
    assertFalse(predicate1, predicate2, "unusual account");
  }
}
