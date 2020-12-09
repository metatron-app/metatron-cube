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

package io.druid.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

public class JoinPostProcessorTest
{
  final JoinPostProcessor inner = proc(JoinType.INNER);
  final JoinPostProcessor lo = proc(JoinType.LO);
  final JoinPostProcessor ro = proc(JoinType.RO);
  final JoinPostProcessor full = proc(JoinType.FULL);

  @Test
  public void testJoin() throws Exception
  {
    // no match
    List<Object[]> l = Arrays.<Object[]>asList(array("spot", "automotive", 100));
    List<Object[]> r = Arrays.<Object[]>asList(array("spot", "business", 200));

    test1(inner, new int[][]{}, l, r);
    test1(lo, new int[][]{{100, -1}}, l, r);
    test1(ro, new int[][]{{-1, 200}}, l, r);
    test1(full, new int[][]{{100, -1}, {-1, 200}}, l, r);

    // inner product
    l = Arrays.<Object[]>asList(array("spot", "automotive", 100), array("spot", "automotive", 200), array("upfront", "automotive", 201));
    r = Arrays.<Object[]>asList(array("spot", "automotive", 300), array("spot", "automotive", 400), array("upfront", "business", 401));

    test1(inner, new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}}, l, r);
    test1(lo, new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}, {201, -1}}, l, r);
    test1(ro, new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}, {-1, 401}}, l, r);
    test1(full, new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}, {201, -1}, {-1, 401}}, l, r);

    // more1
    l = Arrays.<Object[]>asList(array("spot", "automotive", 100), array("spot", "business", 200), array("total", "mezzanine", 300));
    r = Arrays.<Object[]>asList(array("spot", "automotive", 400), array("spot", "automotive", 500), array("total", "mezzanine", 600));

    test1(inner, new int[][]{{100, 400}, {100, 500}, {300, 600}}, l, r);
    test1(lo, new int[][]{{100, 400}, {100, 500}, {200, -1}, {300, 600}}, l, r);
    test1(ro, new int[][]{{100, 400}, {100, 500}, {300, 600}}, l, r);
    test1(full, new int[][]{{100, 400}, {100, 500}, {200, -1}, {300, 600}}, l, r);

    // more2
    l = Arrays.<Object[]>asList(array("spot", "business", 100), array("spot", "health", 200), array("total", "automotive", 300));
    r = Arrays.<Object[]>asList(array("spot", "automotive", 400), array("spot", "health", 500), array("total", "business", 600));

    test1(inner, new int[][]{{200, 500}}, l, r);
    test1(lo, new int[][]{{100, -1}, {200, 500}, {300, -1}}, l, r);
    test1(ro, new int[][]{{-1, 400}, {200, 500}, {-1, 600}}, l, r);
    test1(full, new int[][]{{-1, 400}, {100, -1}, {200, 500}, {300, -1}, {-1, 600}}, l, r);
  }

  private static Object[] array(Object... elements)
  {
    return elements;
  }

  private void test1(JoinPostProcessor processor, int[][] expected, List<Object[]> left, List<Object[]> right)
  {
    JoinPostProcessor.JoinAlias l = new JoinPostProcessor.JoinAlias(
        Arrays.asList("ds1"), Arrays.asList("a", "b", "x"), Arrays.asList("a", "b"), new int[] {0, 1}, left
    );
    JoinPostProcessor.JoinAlias r = new JoinPostProcessor.JoinAlias(
        Arrays.asList("ds2"), Arrays.asList("c", "d", "y"), Arrays.asList("c", "d"), new int[] {0, 1}, right
    );
    validate(expected, Lists.newArrayList(processor.join(l, r, 0).iterator));

    if (processor == inner || processor == lo) {
      JoinPostProcessor.JoinAlias lhs = new JoinPostProcessor.JoinAlias(
          Arrays.asList("ds1"), Arrays.asList("a", "b", "x"), Arrays.asList("a", "b"), new int[]{0, 1}, left
      );
      JoinPostProcessor.JoinAlias rhs = new JoinPostProcessor.JoinAlias(
          Arrays.asList("ds2"), Arrays.asList("c", "d", "y"), Arrays.asList("c", "d"), new int[]{0, 1}, right.iterator()
      );
      validate(expected, Lists.newArrayList(processor.join(lhs, rhs, 0).iterator));

      JoinPostProcessor.JoinAlias lh = new JoinPostProcessor.JoinAlias(
          Arrays.asList("ds1"), Arrays.asList("a", "b", "x"), Arrays.asList("a", "b"), new int[]{0, 1}, left
      );
      JoinPostProcessor.JoinAlias rh = new JoinPostProcessor.JoinAlias(
          Arrays.asList("ds2"), Arrays.asList("c", "d", "y"), Arrays.asList("c", "d"), new int[]{0, 1}, right.iterator()
      );
      validate(expected, Lists.newArrayList(processor.join(lh, rh, 0).iterator));
    }
  }

  private void validate(int[][] expected, List<Object[]> joined)
  {
    System.out.println("------------->");
    int[] index = new int[] {2, 5};
    for (Object[] x : joined) {
      System.out.println(Arrays.toString(x));
    }
    Assert.assertEquals(expected.length, joined.size());
    for (int i = 0; i < expected.length; i++) {
      int[] actual = new int[index.length];
      for (int x = 0; x < index.length; x++) {
        final Integer value = (Integer) joined.get(i)[index[x]];
        actual[x] = value == null ? -1 : value;
      }
      Assert.assertArrayEquals(expected[i], actual);
    }
  }

  @Test
  public void testMultiJoin() throws Exception
  {
    JoinPostProcessor inner = proc(JoinType.INNER);
    JoinPostProcessor lo = proc(JoinType.LO);
    JoinPostProcessor ro = proc(JoinType.RO);

    // no match
    List<Object[]> a1 = Arrays.<Object[]>asList(array("spot", "automotive", 100));
    List<Object[]> a2 = Arrays.<Object[]>asList(array("spot", "business", 200));
    List<Object[]> a3 = Arrays.<Object[]>asList(array("spot", "entertainment", 300));

    test2(inner, new int[][]{}, a1, a2, a3);
    test2(lo, new int[][]{{100, -1, -1}}, a1, a2, a3);
    test2(ro, new int[][]{{-1, -1, 300}}, a1, a2, a3);

    // inner product
    a1 = Arrays.<Object[]>asList(array("spot", "automotive", 100), array("spot", "automotive", 200));
    a2 = Arrays.<Object[]>asList(array("spot", "automotive", 300), array("spot", "automotive", 400));
    a3 = Arrays.<Object[]>asList(array("spot", "automotive", 500), array("spot", "automotive", 600));

    test2(
        inner,
        new int[][]{
            {100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
            {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}
        },
        a1, a2, a3
    );
    test2(
        lo,
        new int[][]{
            {100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
            {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}
        },
        a1, a2, a3
    );
    test2(
        ro,
        new int[][]{{100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
                    {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}
        },
        a1, a2, a3
    );

    // more1
    a1 = Arrays.<Object[]>asList(array("spot", "automotive", 100), array("spot", "business", 200), array("total", "mezzanine", 300));
    a2 = Arrays.<Object[]>asList(array("spot", "automotive", 400), array("spot", "automotive", 500), array("total", "mezzanine", 600));
    a3 = Arrays.<Object[]>asList(array("spot", "-", 700), array("spot", "business", 800), array("total", "mezzanine", 900));

    test2(inner, new int[][]{{300, 600, 900}}, a1, a2, a3);
    test2(lo, new int[][]{{100, 400, -1}, {100, 500, -1}, {200, -1, 800}, {300, 600, 900}}, a1, a2, a3);
    test2(ro, new int[][]{{-1, -1, 700}, {-1, -1, 800}, {300, 600, 900}}, a1, a2, a3);
  }

  private void test2(
      JoinPostProcessor processor,
      int[][] expected,
      List<Object[]> r1,
      List<Object[]> r2,
      List<Object[]> r3
  ) throws Exception
  {
    JoinPostProcessor.JoinAlias a1 = new JoinPostProcessor.JoinAlias(
        Arrays.asList("ds1"), Arrays.asList("a", "b", "x"), Arrays.asList("a", "b"), new int[] {0, 1}, r1
    );
    JoinPostProcessor.JoinAlias a2 = new JoinPostProcessor.JoinAlias(
        Arrays.asList("ds2"), Arrays.asList("c", "d", "y"), Arrays.asList("c", "d"), new int[] {0, 1}, r2
    );
    JoinPostProcessor.JoinAlias a3 = new JoinPostProcessor.JoinAlias(
        Arrays.asList("ds3"), Arrays.asList("e", "f", "z"), Arrays.asList("e", "f"), new int[] {0, 1}, r3
    );
    Future[] futures = new Future[] {
        Futures.immediateFuture(a1), Futures.immediateFuture(a2), Futures.immediateFuture(a3)
    };
    int[] index = new int[] {2, 5, 8};

    JoinPostProcessor.JoinResult result = processor.join(futures, -1);
    List<Object[]> joined = Lists.newArrayList(result.iterator);
    System.out.println("-------------");
    System.out.println("collation " + result.collation);
    for (Object[] x : joined) {
      System.out.println(Arrays.toString(x));
    }
    Assert.assertEquals(expected.length, joined.size());
    for (int i = 0; i < expected.length; i++) {
      int[] actual = new int[index.length];
      for (int x = 0; x < index.length; x++) {
        final Integer value = (Integer) joined.get(i)[index[x]];
        actual[x] = value == null ? -1 : value;
      }
      Assert.assertArrayEquals(expected[i], actual);
    }
  }

  private static Supplier<List<OrderByColumnSpec>> collation(String... columns)
  {
    return Suppliers.ofInstance(OrderByColumnSpec.ascending(columns));
  }

  private JoinPostProcessor proc(JoinType type)
  {
    JoinElement element1 = new JoinElement(type, "ds1", Arrays.asList("a", "b"), "ds2", Arrays.asList("c", "d"));
    JoinElement element2 = new JoinElement(type, "ds1", Arrays.asList("a", "b"), "ds3", Arrays.asList("e", "f"));
    return new JoinPostProcessor(
        new JoinQueryConfig(),
        Arrays.asList(element1, element2),
        false,
        false,
        null,
        null,
        0
    );
  }
}
