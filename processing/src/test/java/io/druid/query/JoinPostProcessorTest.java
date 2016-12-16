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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.druid.collections.StupidPool;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 */
public class JoinPostProcessorTest
{
  static final QueryToolChestWarehouse warehouse;

  static {
    final Supplier<GroupByQueryConfig> supplier = Suppliers.ofInstance(new GroupByQueryConfig());
    final ObjectMapper mapper = new DefaultObjectMapper();
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
            SearchQuery.class, new SearchQueryQueryToolChest(new SearchQueryConfig(), null),
            GroupByQuery.class, new GroupByQueryQueryToolChest(
                supplier, mapper, new GroupByQueryEngine(supplier, pool), pool, null
            )
        );
    warehouse = new MapQueryToolChestWarehouse(mappings);
  }

  @Test
  public void testJoin()
  {
    JoinPostProcessor processor = new JoinPostProcessor(
        JoinType.INNER,
        Arrays.asList("ds1"), Arrays.asList("a", "b"),
        Arrays.asList("ds2"), Arrays.asList("c", "d"),
        warehouse,
        Executors.newSingleThreadExecutor()
    );

    // no match
    List<Map<String, Object>> l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100)
    );
    List<Map<String, Object>> r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "business", "y", 200)
    );
    assertJoin(processor.join(l, r, JoinType.INNER), new int[][]{});
    assertJoin(processor.join(l, r, JoinType.LO), new int[][]{{100, -1}});
    assertJoin(processor.join(l, r, JoinType.RO), new int[][]{{-1, 200}});

    // inner product
    l = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 100),
        ImmutableMap.<String, Object>of("a", "spot", "b", "automotive", "x", 200)
    );
    r = Arrays.<Map<String, Object>>asList(
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 300),
        ImmutableMap.<String, Object>of("c", "spot", "d", "automotive", "y", 400)
    );
    assertJoin(processor.join(l, r, JoinType.INNER), new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}});
    assertJoin(processor.join(l, r, JoinType.LO), new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}});
    assertJoin(processor.join(l, r, JoinType.RO), new int[][]{{100, 300}, {100, 400}, {200, 300}, {200, 400}});

    // more
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
    assertJoin(processor.join(l, r, JoinType.INNER), new int[][]{{100, 400}, {100, 500}, {300, 600}});
    assertJoin(processor.join(l, r, JoinType.LO), new int[][]{{100, 400}, {100, 500}, {200, -1}, {300, 600}});
    assertJoin(processor.join(l, r, JoinType.RO), new int[][]{{100, 400}, {100, 500}, {300, 600}});

    // more
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
    assertJoin(processor.join(l, r, JoinType.INNER), new int[][]{{200, 500}});
    assertJoin(processor.join(l, r, JoinType.LO), new int[][]{{100, -1}, {200, 500}, {300, -1}});
    assertJoin(processor.join(l, r, JoinType.RO), new int[][]{{-1, 400}, {200, 500}, {-1, 600}});
  }

  private void assertJoin(Iterable<Map<String, Object>> join, int[][] expected)
  {
    System.out.println("-------------");
    for (Object x : join) {
      System.out.println(x);
    }
    int i = 0;
    for (Map<String, Object> joined : join) {
      Object x = joined.get("x");
      Object y = joined.get("y");
      if (expected[i][0] < 0) {
        Assert.assertNull(joined.toString(), x);
      } else {
        Assert.assertNotNull(joined.toString(), x);
        Assert.assertEquals(joined.toString(), expected[i][0], ((Integer) x).intValue());
      }
      if (expected[i][1] < 0) {
        Assert.assertNull(joined.toString(), y);
      } else {
        Assert.assertNotNull(joined.toString(), y);
        Assert.assertEquals(joined.toString(), expected[i][1], ((Integer) y).intValue());
      }
      i++;
    }
    if (i != expected.length) {
      Assert.fail("needs more result");
    }
  }
}
