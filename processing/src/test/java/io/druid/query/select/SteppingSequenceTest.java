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

package io.druid.query.select;

import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.common.SteppingSequence;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class SteppingSequenceTest
{
  RawRows rows1 = new RawRows(null, null, Arrays.asList(new Object[] {"a"}, new Object[] {"b"}, new Object[] {"c"}));
  RawRows rows2 = new RawRows(null, null, Arrays.asList(new Object[]{"d"}, new Object[]{"e"}, new Object[]{"f"}));
  RawRows rows3 = new RawRows(null, null, Arrays.asList(new Object[]{"g"}, new Object[]{"h"}, new Object[]{"i"}));
  Sequence<RawRows> sequence = Sequences.<RawRows>concat(
      Arrays.asList(
          Sequences.<RawRows>simple(Arrays.<RawRows>asList(rows1)),
          Sequences.<RawRows>simple(Arrays.<RawRows>asList(rows2)),
          Sequences.<RawRows>simple(Arrays.<RawRows>asList(rows3))
      )
  );

  @Test
  public void test() {
    List<RawRows> list = applyLimit(3);
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(rows1.getRows(), list.get(0).getRows());

    list = applyLimit(5);
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(rows1.getRows(), list.get(0).getRows());
    Assert.assertEquals(rows2.getRows(), list.get(1).getRows());

    list = applyLimit(6);
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(rows1.getRows(), list.get(0).getRows());
    Assert.assertEquals(rows2.getRows(), list.get(1).getRows());
  }

  private List<RawRows> applyLimit(final int limit)
  {
    return Sequences.toList(
        new SteppingSequence<RawRows>(sequence)
        {
          private int count;

          @Override
          protected void accumulating(RawRows in)
          {
            count += in.getRows().size();
          }

          @Override
          protected boolean withinThreshold()
          {
            return count < limit;
          }
        }, Lists.<RawRows>newArrayList()
    );
  }
}
