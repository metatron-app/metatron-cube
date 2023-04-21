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

package io.druid.query.filter;

import org.junit.Assert;
import org.junit.Test;

public class DimFiltersTest
{
  @Test
  public void testMerge()
  {
    DimFilter f1 = InDimFilter.of("a", "1", "3", "5");
    DimFilter f2 = InDimFilter.of("a", "2", "4", "6");

    Assert.assertEquals(f1, DimFilters.or(f1));
    Assert.assertEquals(f1, DimFilters.and(f1));
    Assert.assertEquals(f2, DimFilters.or(f2));
    Assert.assertEquals(f2, DimFilters.and(f2));

    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "4", "5", "6"), DimFilters.or(f1, f2));
    Assert.assertEquals(DimFilters.NONE, DimFilters.and(f1, f2));

    f2 = InDimFilter.of("a", "2", "4", "5");
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "4", "5"), DimFilters.or(f1, f2));
    Assert.assertEquals(SelectorDimFilter.of("a", "5"), DimFilters.and(f1, f2));

    f2 = InDimFilter.of("a", "2", "3", "5");
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "5"), DimFilters.or(f1, f2));
    Assert.assertEquals(InDimFilter.of("a", "3", "5"), DimFilters.and(f1, f2));

    DimFilter b1 = BoundDimFilter.range("a", "x", "y");   // not mergeable
    DimFilter b2 = BoundDimFilter.range("a", "p", "q");   // not mergeable

    Assert.assertEquals(OrDimFilter.of(b1, b2), DimFilters.or(b1, b2));
    Assert.assertEquals(OrDimFilter.of(f1, b1), DimFilters.or(f1, b1));
    Assert.assertEquals(AndDimFilter.of(f1, b1), DimFilters.and(f1, b1));

    Assert.assertEquals(OrDimFilter.of(InDimFilter.of("a", "1", "2", "3", "5"), b1), DimFilters.or(f1, f2, b1));
    Assert.assertEquals(OrDimFilter.of(InDimFilter.of("a", "1", "2", "3", "5"), b1), DimFilters.or(f1, b1, f2));
    Assert.assertEquals(OrDimFilter.of(b1, InDimFilter.of("a", "1", "2", "3", "5")), DimFilters.or(b1, f1, f2));

    Assert.assertEquals(AndDimFilter.of(InDimFilter.of("a", "3", "5"), b1), DimFilters.and(f1, f2, b1));
    Assert.assertEquals(AndDimFilter.of(InDimFilter.of("a", "3", "5"), b1), DimFilters.and(f1, b1, f2));
    Assert.assertEquals(AndDimFilter.of(b1, InDimFilter.of("a", "3", "5")), DimFilters.and(b1, f1, f2));

    DimFilter f3 = InDimFilter.of("a", "1", "5", "7");
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "5", "7"), DimFilters.or(f1, f2, f3));
    Assert.assertEquals(SelectorDimFilter.of("a", "5"), DimFilters.and(f1, f2, f3));

    f3 = SelectorDimFilter.of("a", "5");
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "5"), DimFilters.or(f1, f2, f3));
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "5"), DimFilters.or(f3, f1, f2));
    Assert.assertEquals(SelectorDimFilter.of("a", "5"), DimFilters.and(f1, f2, f3));
    Assert.assertEquals(SelectorDimFilter.of("a", "5"), DimFilters.and(f2, f3, f1));

    f3 = SelectorDimFilter.of("a", "4");
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "4", "5"), DimFilters.or(f1, f2, f3));
    Assert.assertEquals(InDimFilter.of("a", "1", "2", "3", "4", "5"), DimFilters.or(f3, f1, f2));
    Assert.assertEquals(DimFilters.NONE, DimFilters.and(f1, f2, f3));
    Assert.assertEquals(DimFilters.NONE, DimFilters.and(f3, f1, f2));
  }
}