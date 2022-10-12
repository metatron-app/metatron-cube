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

package io.druid.common.guava;

import io.druid.common.utils.Sequences;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CombiningSequenceSimpleTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void test() throws Exception
  {
    Sequence<Object[]> sequence = Sequences.of(
        new Object[]{"a", 1}, new Object[]{"a", 2},
        new Object[]{"b", 1},
        new Object[]{"c", 1}, new Object[]{"c", 2}, new Object[]{"c", 3},
        new Object[]{"d", 1}
    );
    List<Object[]> expected = Arrays.asList(
        new Object[]{"a", "3"}, new Object[]{"b", "1"}, new Object[]{"c", "6"}, new Object[]{"d", "1"}
    );
    Sequence<Object[]> combined = CombiningSequence.create(
        sequence,
        (o1, o2) -> ((Comparable) o1[0]).compareTo(o2[0]),
        new CombineFn.Identical<Object[]>()
        {
          @Override
          public Object[] apply(Object[] arg1, Object[] arg2)
          {
            if (arg1 == null) {
              return arg2;
            }
            arg1[1] = (Integer) arg1[1] + (Integer) arg2[1];
            return arg1;
          }

          @Override
          public Object[] done(Object[] ret)
          {
            ret[1] = String.valueOf(ret[1]);
            return ret;
          }
        }
    );
    List<Object[]> values = Sequences.toList(combined);
    for (int i = 0; i < Math.min(values.size(), expected.size()); i++) {
      Assert.assertArrayEquals(expected.get(i), values.get(i));
    }
    Assert.assertEquals(expected.size(), values.size());
  }
}
