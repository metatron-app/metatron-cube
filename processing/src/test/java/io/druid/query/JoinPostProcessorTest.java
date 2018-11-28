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

package io.druid.query;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.collections.StupidPool;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.search.SearchQuery;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 */
public class JoinPostProcessorTest
{
  static final QueryToolChestWarehouse warehouse;

  static {
    final QueryConfig queryConfig = new QueryConfig();
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );
    Map<Class<? extends Query>, QueryToolChest> mappings =
        ImmutableMap.<Class<? extends Query>, QueryToolChest>of(
            SearchQuery.class, new SearchQueryQueryToolChest(queryConfig.getSearch(), null),
            GroupByQuery.class, new GroupByQueryQueryToolChest(
                queryConfig, new GroupByQueryEngine(pool), pool, null
            )
        );
    warehouse = new MapQueryToolChestWarehouse(queryConfig, mappings);
  }

  @Test
  public void testJoin() throws Exception
  {
    JoinPostProcessor inner = test1(JoinType.INNER);
    JoinPostProcessor lo = test1(JoinType.LO);
    JoinPostProcessor ro = test1(JoinType.RO);

    // no match
    List<Map<String, Object>> l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100)
    );
    List<Map<String, Object>> r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "business", "y", 200)
    );
    assertJoin(new int[][]{}, inner.join(l, r), inner.hashJoin(l, r));
    assertJoin(new int[][]{{100, -1}}, lo.join(l, r), lo.hashJoin(l, r));
    assertJoin(new int[][]{{-1, 200}}, ro.join(l, r));

    // inner product
    l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 200)
    );
    r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 300),
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400)
    );
    assertJoin(new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}}, inner.join(l, r), inner.hashJoin(l, r));
    assertJoin(new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}}, lo.join(l, r), lo.hashJoin(l, r));
    assertJoin(new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}}, ro.join(l, r));

    // more1
    l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "business", "x", 200),
        ImmutableMap.<String, Object>of("a", "total", "b", "mezzanine", "x", 300)
    );
    r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400),
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 500),
        ImmutableMap.<String, Object>of("c", "total", "d", "mezzanine", "y", 600)
    );
    assertJoin(new int[][]{{100, 400}, {100, 500}, {300, 600}}, inner.join(l, r), inner.hashJoin(l, r));
    assertJoin(new int[][]{{100, 400}, {100, 500}, {200, -1}, {300, 600}}, lo.join(l, r), lo.hashJoin(l, r));
    assertJoin(new int[][]{{100, 400}, {100, 500}, {300, 600}}, ro.join(l, r));

    // more2
    l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "business", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "health", "x", 200),
        ImmutableMap.<String, Object>of("a", "total", "b", "automotive", "x", 300)
    );
    r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400),
        ImmutableMap.<String, Object>of("c", "spot", "d", "health", "y", 500),
        ImmutableMap.<String, Object>of("c", "total", "d", "business", "y", 600)
    );
    assertJoin(new int[][]{{200, 500}}, inner.join(l, r), inner.hashJoin(l, r));
    assertJoin(new int[][]{{100, -1}, {200, 500}, {300, -1}}, lo.join(l, r), lo.hashJoin(l, r));
    assertJoin(new int[][]{{-1, 400}, {200, 500}, {-1, 600}}, ro.join(l, r));
  }

  private JoinPostProcessor test1(JoinType type)
  {
    Set<String> dataSources = ImmutableSet.of("ds1", "ds2");
    JoinElement element1 = new JoinElement(type, "ds1", Arrays.asList("a", "b"), "ds2", Arrays.asList("c", "d"));
    JoinElement element2 = new JoinElement(type, "ds1.a = ds2.c && ds1.b = ds2.d").rewrite(dataSources);
    Assert.assertEquals(element1, element2);
    return new JoinPostProcessor(
        new JoinQueryConfig(),
        Arrays.asList(element1),
        false,
        warehouse,
        Executors.newSingleThreadExecutor()
    );
  }

  @Test
  public void testMultiJoin() throws Exception
  {
    JoinPostProcessor inner = test2(JoinType.INNER);
    JoinPostProcessor lo = test2(JoinType.LO);
    JoinPostProcessor ro = test2(JoinType.RO);

    // no match
    List<Map<String, Object>> a1 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100)
    );
    List<Map<String, Object>> a2 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "business", "y", 200)
    );
    List<Map<String, Object>> a3 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("e", "spot", "f", "entertainment", "z", 300)
    );
    assertJoin(new int[][]{}, inner.join(a1, a2, a3));
    assertJoin(new int[][]{}, inner.hashJoin(a1, a2, a3));

    assertJoin(new int[][]{{100, -1, -1}}, lo.join(a1, a2, a3));
    assertJoin(new int[][]{{100, -1, -1}}, lo.hashJoin(a1, a2, a3));

    assertJoin(new int[][]{{-1, -1, 300}}, ro.join(a1, a2, a3));

    // inner product
    a1 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 200)
    );
    a2 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 300),
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400)
    );
    a3 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("e", "spot", "f", "automotive", "z", 500),
        ImmutableMap.<String, Object>of("e", "spot", "f", "automotive", "z", 600)
    );
    assertJoin(
        new int[][]{{100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
                    {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}},
        inner.join(a1, a2, a3), inner.hashJoin(a1, a2, a3)
    );
    assertJoin(
        new int[][]{{100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
                    {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}},
        lo.join(a1, a2, a3), lo.hashJoin(a1, a2, a3)
    );
    assertJoin(
        new int[][]{{100, 300, 500}, {100, 300, 600}, {100, 400, 500}, {100, 400, 600},
                    {200, 300, 500}, {200, 300, 600}, {200, 400, 500}, {200, 400, 600}},
        ro.join(a1, a2, a3)
    );

    // more1
    a1 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "business", "x", 200),
        ImmutableMap.<String, Object>of("a", "total", "b", "mezzanine", "x", 300)
    );
    a2 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400),
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 500),
        ImmutableMap.<String, Object>of("c", "total", "d", "mezzanine", "y", 600)
    );
    a3 = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("e", "spot", "f", "-", "z", 700),
        ImmutableMap.<String, Object>of("e", "spot", "f", "business", "z", 800),
        ImmutableMap.<String, Object>of("e", "total", "f", "mezzanine", "z", 900)
    );
    assertJoin(new int[][]{{300, 600, 900}}, inner.join(a1, a2, a3), inner.hashJoin(a1, a2, a3));
    assertJoin(
        new int[][]{{100, 400, -1}, {100, 500, -1}, {200, -1, 800}, {300, 600, 900}},
        lo.join(a1, a2, a3), lo.hashJoin(a1, a2, a3)
    );
    assertJoin(
        new int[][]{{-1, -1, 700}, {-1, -1, 800}, {300, 600, 900}}, ro.join(a1, a2, a3)
    );
  }

  private JoinPostProcessor test2(JoinType type)
  {
    return new JoinPostProcessor(
        new JoinQueryConfig(),
        Arrays.asList(
            new JoinElement(type, "ds1", Arrays.asList("a", "b"), "ds2", Arrays.asList("c", "d")),
            new JoinElement(type, "ds1", Arrays.asList("a", "b"), "ds3", Arrays.asList("e", "f"))
        ),
        false,
        warehouse,
        Executors.newSingleThreadExecutor()
    );
  }

  private final String[] k = new String[]{"x", "y", "z"};

  @SafeVarargs
  private final void assertJoin(int[][] expected, Iterable<Map<String, Object>>... joins)
  {
    for (Iterable<Map<String, Object>> join : joins) {
      System.out.println("-------------");
      for (Object x : join) {
        System.out.println(x);
      }
      int x = 0;
      for (Map<String, Object> joined : join) {
        for (int i = 0; i < expected[x].length; i++) {
          validate(joined, expected[x][i], joined.get(k[i]));
        }
        x++;
      }
      if (x != expected.length) {
        Assert.fail("needs more result");
      }
    }
  }

  private void validate(Map<String, Object> joined, int i, Object x)
  {
    if (i < 0) {
      Assert.assertNull(joined.toString(), x);
    } else {
      Assert.assertNotNull(joined.toString(), x);
      Assert.assertEquals(joined.toString(), i, ((Integer) x).intValue());
    }
  }
}
