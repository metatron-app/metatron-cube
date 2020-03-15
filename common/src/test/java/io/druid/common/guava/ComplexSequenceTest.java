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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.druid.common.Yielders;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.nary.BinaryFn;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ComplexSequenceTest
{
  @Test
  public void testConcatOnLimit() throws Exception
  {
    Assert.assertEquals(Arrays.asList(1, 9, 10, 0b11001), testConcatOnLimit(0));   // is this a bug?
    Assert.assertEquals(Arrays.asList(1, 9, 10, 0b11001), testConcatOnLimit(1));
    Assert.assertEquals(Arrays.asList(1, 2, 3, 9, 10, 0b11001), testConcatOnLimit(3));
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 9, 10, 0b11011), testConcatOnLimit(5));
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 0b11111), testConcatOnLimit(7));
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 0b10111), testConcatOnLimit(10));
  }

  private List<Integer> testConcatOnLimit(int limit) throws Exception
  {
    MutableInt counter = new MutableInt();
    Sequence<Integer> sequence = Sequences.limit(Sequences.withBaggage(Sequences.concat(
        Sequences.limit(Sequences.concat(
            Sequences.withEffect(Sequences.simple(Arrays.asList(1, 2, 3)), () -> counter.add(1)),
            Sequences.withEffect(Sequences.simple(Arrays.asList(4, 5, 6)), () -> counter.add(2)),
            Sequences.withEffect(Sequences.simple(Arrays.asList(7, 8)), () -> counter.add(4))
        ), limit),
        Sequences.withEffect(Sequences.lazy(() -> Sequences.simple(Arrays.asList(9, 10))), () -> counter.add(8))
    ), () -> counter.add(16)), 8);
    List<Integer> values = collect(sequence);
    values.add(counter.intValue());
    return values;
  }

  @Test
  public void testComplexSequence()
  {
    Sequence<Integer> complex;
    check("[3, 5]", complex = Sequences.concat(combine(simple(3)), combine(simple(5))));
    check("[8]", complex = combine(complex));
    check("[8, 6, 3, 5]", complex = Sequences.concat(complex, Sequences.concat(combine(simple(2, 4)), simple(3, 5))));
    check("[22]", complex = combine(complex));
    check("[22]", Sequences.concat(complex, simple()));
  }

  private void check(String expected, Sequence<Integer> complex)
  {
    List<Integer> combined = Sequences.toList(complex);
    Assert.assertEquals(expected, combined.toString());
    Assert.assertEquals(expected, collect(complex).toString());
  }

  private <T> List<T> collect(Sequence<T> sequence)
  {
    List<T> values = Lists.newArrayList();
    Yielder<T> yielder = Yielders.each(sequence);
    while (!yielder.isDone()) {
      values.add(yielder.get());
      yielder = yielder.next(null);
    }
    Yielders.close(yielder);
    return values;
  }

  private Sequence<Integer> simple(int... values)
  {
    return Sequences.simple(Ints.asList(values));
  }

  private Sequence<Integer> combine(Sequence<Integer> sequence)
  {
    return CombiningSequence.create(sequence, alwaysSame, plus);
  }

  private final Ordering<Integer> alwaysSame = new Ordering<Integer>()
  {
    @Override
    public int compare(Integer left, Integer right)
    {
      return 0;
    }
  };

  private final BinaryFn<Integer, Integer, Integer> plus = new BinaryFn.Identical<Integer>()
  {
    @Override
    public Integer apply(Integer arg1, Integer arg2)
    {
      if (arg1 == null) {
        return arg2;
      }

      if (arg2 == null) {
        return arg1;
      }

      return arg1 + arg2;
    }
  };
}
