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

package io.druid.segment.filter;

import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.LucenePointFilter;
import org.junit.Assert;
import org.junit.Test;

public class FiltersTest
{
  @Test
  public void testCNFComplex()
  {
    DimFilter dim1 = BoundDimFilter.gt("market", "a");
    DimFilter dim2 = BoundDimFilter.lt("market", "b");
    DimFilter dim3 = LucenePointFilter.bbox("l.c", new double[] {1, 2}, new double[] {3, 4});
    DimFilter dim4 = LucenePointFilter.bbox("l.c", new double[] {1.1, 1.2}, new double[] {3.1, 4.1});
    DimFilter complex = DimFilters.and(dim1, DimFilters.and(dim2, DimFilters.or(dim3, dim4)));

    DimFilter expected = DimFilters.and(dim1, dim2, DimFilters.or(dim3, dim4));
    Assert.assertEquals(expected, DimFilters.convertToCNF(complex));
  }
}
