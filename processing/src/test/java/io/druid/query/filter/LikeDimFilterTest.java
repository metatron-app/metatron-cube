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
    LikeMatcher matcher = LikeMatcher.from("DUMMY", null);
    Assert.assertEquals(0, matcher.predicateType());
    Assert.assertEquals(
        new LikeMatcher("DUMMY", "DUMMY", null, Arrays.asList(), null), matcher
    );
    Predicate<String> predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("DUMMY"));
    Assert.assertFalse(predicate.apply("DUMMYx"));

    matcher = LikeMatcher.from("PROMO%", null);
    Assert.assertEquals(1, matcher.predicateType());
    Assert.assertEquals(
        new LikeMatcher("PROMO%", "PROMO", null, Arrays.asList(), "PROMO.*"), matcher
    );
    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("PROMO"));
    Assert.assertTrue(predicate.apply("PROMOT"));
    Assert.assertFalse(predicate.apply("PROM"));

    matcher = LikeMatcher.from("%ECONOMY BRUSHED", null);
    Assert.assertEquals(2, matcher.predicateType());
    Assert.assertEquals(
        new LikeMatcher("%ECONOMY BRUSHED", null, "ECONOMY BRUSHED", Arrays.asList(), ".*ECONOMY BRUSHED"), matcher
    );
    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("ECONOMY BRUSHED"));
    Assert.assertTrue(predicate.apply("xECONOMY BRUSHED"));
    Assert.assertFalse(predicate.apply("ECONOMY BRUSHEDx"));

    matcher = LikeMatcher.from("ECONOMY%BRUSHED", null);
    Assert.assertEquals(
        new LikeMatcher("ECONOMY%BRUSHED", "ECONOMY", "BRUSHED", Arrays.asList(), "ECONOMY.*BRUSHED"), matcher
    );
    Assert.assertEquals(3, matcher.predicateType());

    matcher = LikeMatcher.from("ECONOMY%XX%BRUSHED", null);
    Assert.assertEquals(
        new LikeMatcher("ECONOMY%XX%BRUSHED", "ECONOMY", "BRUSHED", Arrays.asList("XX"), "ECONOMY.*XX.*BRUSHED"), matcher
    );
    Assert.assertEquals(15, matcher.predicateType());

    matcher = LikeMatcher.from("%unusual%accounts%", null);
    Assert.assertEquals(12, matcher.predicateType());
    Assert.assertEquals(
        new LikeMatcher("%unusual%accounts%", null, null, Arrays.asList("unusual", "accounts"), ".*unusual.*accounts.*"), matcher
    );
    predicate = matcher.asPredicate();
    Assert.assertTrue(predicate.apply("unusualaccounts"));
    Assert.assertTrue(predicate.apply("unusual accounts"));
    Assert.assertTrue(predicate.apply("xx unusual accounts xx"));
    Assert.assertFalse(predicate.apply("unusual account"));
  }
}
