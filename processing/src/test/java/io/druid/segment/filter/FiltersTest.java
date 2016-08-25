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

package io.druid.segment.filter;

import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import org.junit.Assert;
import org.junit.Test;

public class FiltersTest
{
  @Test
  public void testPartitionWithBitmapSupport() throws Exception
  {
    DimFilter dim1 = new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "mezzanine", null);

    DimFilter dim2 = new MathExprFilter(
        "return market === \"spot\" && quality === \"business\"; }"
    );

    DimFilter dim3 = AndDimFilter.of(dim1, dim2);

    DimFilter dim4 = OrDimFilter.of(dim1, dim2);

    // DIM1 AND !(DIM1 OR DIM2) -> DIM1 AND !DIM1 AND !DIM2
    DimFilter dim5 = AndDimFilter.of(dim1, NotDimFilter.of(dim4));

    DimFilter[] filters;

    filters = Filters.partitionWithBitmapSupport(null);
    Assert.assertNull(filters);

    filters = Filters.partitionWithBitmapSupport(dim1);
    assertEquals(dim1, filters[0]);
    Assert.assertNull(filters[1]);

    filters = Filters.partitionWithBitmapSupport(dim2);
    Assert.assertNull(filters[0]);
    assertEquals(dim2, filters[1]);

    filters = Filters.partitionWithBitmapSupport(dim3);
    assertEquals(dim1, filters[0]);
    assertEquals(dim2, filters[1]);

    filters = Filters.partitionWithBitmapSupport(dim4);
    Assert.assertNull(filters[0]);
    assertEquals(dim4, filters[1]);

    filters = Filters.partitionWithBitmapSupport(dim5);
    assertEquals(AndDimFilter.of(dim1, NotDimFilter.of(dim1)), filters[0]);
    assertEquals(NotDimFilter.of(dim2), filters[1]);
  }

  private void assertEquals(DimFilter expected, DimFilter result)
  {
    Assert.assertEquals(expected.toString(), result.toString());
  }
}
