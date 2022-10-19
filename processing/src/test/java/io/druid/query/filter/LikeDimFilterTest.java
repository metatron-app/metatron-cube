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
    Predicate<String> predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply(""));
    Assert.assertTrue(predicate.apply("D"));
    Assert.assertTrue(predicate.apply("DU"));

    matcher = LikeMatcher.from("_", null);
    Assert.assertEquals(".", matcher.regex());
    Assert.assertEquals("_", matcher.represent());
    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply(""));
    Assert.assertTrue(predicate.apply("D"));
    Assert.assertFalse(predicate.apply("DU"));

    matcher = LikeMatcher.from("__", null);
    Assert.assertEquals(".{2}", matcher.regex());
    Assert.assertEquals("_", matcher.represent());
    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply(""));
    Assert.assertFalse(predicate.apply("D"));
    Assert.assertTrue(predicate.apply("DU"));

    matcher = LikeMatcher.from("DUMMY", null);
    Assert.assertEquals("\\QDUMMY\\E", matcher.regex());
    Assert.assertEquals("L", matcher.represent());
    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("DUMMY"));
    Assert.assertFalse(predicate.apply("DUMMYx"));

    matcher = LikeMatcher.from("PROMO%", null);
    Assert.assertEquals("\\QPROMO\\E.*", matcher.regex());
    Assert.assertEquals("L%", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("PROMO"));
    Assert.assertTrue(predicate.apply("PROMOT"));
    Assert.assertFalse(predicate.apply("PROM"));
    Assert.assertTrue(predicate.apply("PROMOxx"));

    matcher = LikeMatcher.from("PROMO_", null);
    Assert.assertEquals("\\QPROMO\\E.", matcher.regex());
    Assert.assertEquals("L_", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("PROMO"));
    Assert.assertTrue(predicate.apply("PROMOT"));
    Assert.assertFalse(predicate.apply("PROM"));
    Assert.assertFalse(predicate.apply("PROMOxx"));

    matcher = LikeMatcher.from("PROMO__", null);
    Assert.assertEquals("\\QPROMO\\E.{2}", matcher.regex());
    Assert.assertEquals("L_", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("PROMO"));
    Assert.assertFalse(predicate.apply("PROMOT"));
    Assert.assertFalse(predicate.apply("PROM"));
    Assert.assertTrue(predicate.apply("PROMOxx"));

    matcher = LikeMatcher.from("%ECONOMY BRUSHED", null);
    Assert.assertEquals(".*\\QECONOMY BRUSHED\\E", matcher.regex());
    Assert.assertEquals("%L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("xxECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("_ECONOMY BRUSHED", null);
    Assert.assertEquals(".\\QECONOMY BRUSHED\\E", matcher.regex());
    Assert.assertEquals("_L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("xxECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("ECONOMY%BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.*\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L%L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY  BRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("ECONOMY_BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY  BRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("ECONOMY__BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.{2}\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY  BRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("_ECONOMY%", null);
    Assert.assertEquals(".\\QECONOMY\\E.*", matcher.regex());
    Assert.assertEquals("_L%", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertTrue(predicate.apply("xECONOMY"));
    Assert.assertTrue(predicate.apply("xECONOMY "));
    Assert.assertFalse(predicate.apply(" xECONOMY"));

    matcher = LikeMatcher.from("%ECONOMY_", null);
    Assert.assertEquals(".*\\QECONOMY\\E.", matcher.regex());
    Assert.assertEquals("%L_", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY"));
    Assert.assertTrue(predicate.apply("xECONOMY "));
    Assert.assertFalse(predicate.apply("xECONOMYx "));

    matcher = LikeMatcher.from("_ECONOMY_", null);
    Assert.assertEquals(".\\QECONOMY\\E.", matcher.regex());
    Assert.assertEquals("_L_", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMYBRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY"));
    Assert.assertTrue(predicate.apply("xECONOMY "));
    Assert.assertFalse(predicate.apply("xECONOMYx "));

    matcher = LikeMatcher.from("%plum%", null);
    Assert.assertEquals(".*\\Qplum\\E.*", matcher.regex());
    Assert.assertEquals("%L%", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("turquoise plum"));
    Assert.assertTrue(predicate.apply("antique plum smoke"));
    Assert.assertFalse(predicate.apply("turquoise PLUM"));

    matcher = LikeMatcher.from("ECONOMY%XX%BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.*\\QXX\\E.*\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L%L%L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMYXXBRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY XX BRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY  XXBRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMYXX  BRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY XX BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY XX BRUSHEDx"));

    matcher = LikeMatcher.from("ECONOMY_XX_BRUSHED", null);
    Assert.assertEquals("\\QECONOMY\\E.\\QXX\\E.\\QBRUSHED\\E", matcher.regex());
    Assert.assertEquals("L_L_L", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMYXXBRUSHED"));
    Assert.assertTrue(predicate.apply("ECONOMY XX BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY  XXBRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMYXX  BRUSHED"));
    Assert.assertFalse(predicate.apply("xECONOMY XX BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY XX BRUSHEDx"));

    matcher = LikeMatcher.from("%나비스%한글%", null);
    Assert.assertEquals(".*\\Q나비스\\E.*\\Q한글\\E.*", matcher.regex());
    Assert.assertEquals("%L%L%", matcher.represent());

    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("나비스한글"));
    Assert.assertTrue(predicate.apply("나비스 한글"));
    Assert.assertTrue(predicate.apply("뒵 나비스 한글 뵙"));
    Assert.assertFalse(predicate.apply("나비스 한"));
  }

  @Test
  public void testTPCH()
  {
    // TPCH-13
    LikeMatcher matcher = LikeMatcher.from("%unusual%accounts%", null);
    Assert.assertEquals(".*\\Qunusual\\E.*\\Qaccounts\\E.*", matcher.regex());
    Assert.assertEquals("%L%L%", matcher.represent());

    Predicate<String> predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("unusualaccounts"));
    Assert.assertTrue(predicate.apply("unusual accounts"));
    Assert.assertTrue(predicate.apply("xx unusual accounts xx"));
    Assert.assertFalse(predicate.apply("unusual account"));
  }
}
