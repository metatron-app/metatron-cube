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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.IntPredicate;
import io.druid.data.ValueDesc;
import io.druid.query.RowResolver;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.ordering.Direction;
import io.druid.segment.VirtualColumns;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class FiltersTest
{
  private final ValueDesc dimensionType = ValueDesc.DIM_STRING;
  private final RowResolver resolver = new RowResolver(
      ImmutableMap.<String, ValueDesc>of("market", dimensionType, "quality", dimensionType),
      VirtualColumns.EMPTY
  );

  @Test
  public void testCNF()
  {
    DimFilter dim1 = BoundDimFilter.gt("market", "a");
    DimFilter dim2 = BoundDimFilter.lt("market", "b");
    DimFilter dim3 = BoundDimFilter.gt("market", "s");

    DimFilter cnf = DimFilters.convertToCNF(DimFilters.or(DimFilters.and(dim1, dim2), dim3));
    assertEquals(DimFilters.and(DimFilters.or(dim3, dim1), DimFilters.or(dim3, dim2)), cnf);
  }

  @Test
  public void testAllFilter()
  {
    Assert.assertSame(DimFilters.ALL, DimFilters.convertToCNF(DimFilters.ALL));
  }

  private void assertEquals(DimFilter expected, DimFilter result)
  {
    Assert.assertEquals(expected.toString(), result.toString());
  }

  @Test
  public void test() throws IOException
  {
    Random r = new Random();
    RoaringBitmapFactory f = new RoaringBitmapFactory();
    MutableBitmap m = f.makeEmptyMutableBitmap();
    for (int i = 0; i < 1000000; i++) {
      m.add(r.nextInt(2000000));
    }

    File file = File.createTempFile("test", "bitmap");
    FileOutputStream o = new FileOutputStream(file);
    o.write(f.makeImmutableBitmap(m).toBytes());
    o.close();

    ImmutableBitmap l = f.mapImmutableBitmap(Files.map(file));

    int c1 = 0;
    long s = System.currentTimeMillis();
    for (int k = 0; k < 20; k++) {
      for (int i = 0; i < 2000000; i++) {
        if (l.get(i)) {
          c1++;
        }
      }
    }
    System.out.println("elapsed " + c1 + ":" + (System.currentTimeMillis() - s));

    int c2 = 0;
    s = System.currentTimeMillis();
    for (int k = 0; k < 20; k++) {
      IntPredicate predicate = Filters.toMatcher(l, Direction.ASCENDING);
      for (int i = 0; i < 2000000; i++) {
        if (predicate.apply(i)) {
          c2++;
        }
      }
    }
    System.out.println("elapsed " + c2 + ":" + (System.currentTimeMillis() - s));
    Assert.assertEquals("matcher mismatch", c1, c2);

    int c3 = 0;
    s = System.currentTimeMillis();
    for (int k = 0; k < 20; k++) {
      IntPredicate predicate = Filters.toMatcher(l, Direction.DESCENDING);
      for (int i = 2000000 - 1; i >= 0; i--) {
        if (predicate.apply(i)) {
          c3++;
        }
      }
    }
    System.out.println("elapsed " + c3 + ":" + (System.currentTimeMillis() - s));
    Assert.assertEquals("matcher mismatch", c1, c3);
  }
}
