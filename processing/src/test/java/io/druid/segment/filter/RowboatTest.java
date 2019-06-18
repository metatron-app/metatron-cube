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

import io.druid.collections.IntList;
import io.druid.segment.Rowboat;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RowboatTest
{
  @Test
  public void testRowboatCompare()
  {
    Rowboat rb1 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{7}, 1, 5);
    Rowboat rb2 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{7}, 1, 5);
    Assert.assertEquals(0, rb1.compareTo(rb2));

    Rowboat rb3 = new Rowboat(12345L, new int[][]{new int[]{3}, new int[]{2}}, new Object[]{7}, 1, 5);
    Assert.assertNotEquals(0, rb1.compareTo(rb3));
  }

  @Test
  public void testRowboatComprise()
  {
    Rowboat rb1 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{7}, 0, 10);
    rb1.comprised(new IntList(0, 11));
    Rowboat rb2 = new Rowboat(12345L, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{7}, 1, 5);

    IntList comprisedRows = rb1.getComprisedRows();
    comprisedRows.addAll(rb2.getComprisedRows());
    Assert.assertEquals(3 * 2, comprisedRows.size());
    Assert.assertArrayEquals(new int[] {0, 10, 0, 11, 1, 5}, comprisedRows.compact());
  }

  @Test
  public void testBiggerCompare()
  {
    Rowboat rb1 = new Rowboat(
        0,
        new int[][]{
            new int[]{0},
            new int[]{138},
            new int[]{44},
            new int[]{374},
            new int[]{0},
            new int[]{0},
            new int[]{552},
            new int[]{338},
            new int[]{910},
            new int[]{25570},
            new int[]{9},
            new int[]{0},
            new int[]{0},
            new int[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        1, 0
    );

    Rowboat rb2 = new Rowboat(
        0,
        new int[][]{
            new int[]{0},
            new int[]{138},
            new int[]{44},
            new int[]{374},
            new int[]{0},
            new int[]{0},
            new int[]{553},
            new int[]{338},
            new int[]{910},
            new int[]{25580},
            new int[]{9},
            new int[]{0},
            new int[]{0},
            new int[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        1, 0
    );

    Assert.assertNotEquals(0, rb1.compareTo(rb2));
  }

  @Test
  public void testToString()
  {
    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=[[1], [2]], metrics=[someMetric], comprisedRows=[1, 5]}",
        new Rowboat(0, new int[][]{new int[]{1}, new int[]{2}}, new Object[]{"someMetric"}, 1, 5).toString()
    );
  }

  @Test
  public void testLotsONullString()
  {
    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=null, metrics=null, comprisedRows=[1, 5]}",
        new Rowboat(0, null, null, 1, 5).toString()
    );
  }
}
