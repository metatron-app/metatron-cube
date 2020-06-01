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

package io.druid.query.sketch;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.data.ValueDesc;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.TestHelper;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 */
@SuppressWarnings("unchecked")
public class SketchHandlerTest
{
  @Test
  public void testX()
  {
    int[] values = new int[]{
        78869, 17849, 132555, 139420, 16217, 25090, 134177, 131703, 53982, 95489, 159307, 90105, 177803, 82039, 188156,
        56678, 154290, 127743, 180511, 5314, 100874, 25662, 124156, 196617, 85012, 65248, 101036, 158508, 44842, 31571,
        43711, 11215, 82015, 140972, 55217, 85062, 73756, 2598, 36190, 41006, 25721, 159499, 168375, 96018, 15053,
        116928, 181222, 174024, 56603, 179510, 44492, 113533, 137986, 128010, 163964, 107070, 115580, 125061, 187860,
        54738, 172461, 82239, 85546, 113631, 60587, 23784, 125430, 52641, 163961, 137911, 80269, 167177, 113808, 51317,
        194838, 6191, 180273, 159338, 170447, 178944, 198541, 189406, 167269, 79176, 29671, 121272, 120835, 180898,
        65636, 12481, 140942, 78318, 122904, 101433, 100221, 101279, 128109, 39570, 170976, 112369, 100
    };
    final Comparator natural = Ordering.natural();
    final Comparator reverse = natural.reversed();
    final Comparator numeric = StringComparators.LONG;
    List<Integer> x = Ints.asList(values);
    for (Comparator c : Arrays.asList(natural, reverse, numeric)) {
      SketchHandler.Quantile q = new SketchHandler.Quantile();
      TypedSketch<ItemsUnion> sketch = q.newUnion(16, ValueDesc.STRING, c);
      Collections.shuffle(x);
      for (int value : values) {
        q.updateWithValue(sketch, String.valueOf(value));
      }
      Object[] quantiles = q.toSketch(sketch).value().getQuantiles(21);
      Object[] sorted = Arrays.copyOf(quantiles, quantiles.length);
      Arrays.sort(sorted, c);
      Assert.assertArrayEquals(c.toString(), sorted, quantiles);
    }
  }

  @Test
  public void testCardinalityQuantile()
  {
    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(16, ValueDesc.FLOAT, null);
    for (int i = 0; i < 100; i++) {
      q.updateWithValue(sketch, 1);
    }
    for (int i = 2; i < 10; i++) {
      for (int j = 0; j < 5; j++) {
        q.updateWithValue(sketch, i);
      }
    }
    ItemsSketch v = q.toSketch(sketch).rhs;
    Assert.assertEquals(1, v.getQuantile(0.5f));
  }

  @Test
  public void testQuantile()
  {
    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(16, ValueDesc.FLOAT, null);

    q.updateWithValue(sketch, 1.5f);
    q.updateWithValue(sketch, 2.5f);
    q.updateWithValue(sketch, 3.5f);
    q.updateWithValue(sketch, 4.5f);
    q.updateWithValue(sketch, 2.5f);
    q.updateWithValue(sketch, 1.5f);
    q.updateWithValue(sketch, -1.5f);
    q.updateWithValue(sketch, -3.5f);
    q.updateWithValue(sketch, 4.5f);
    q.updateWithValue(sketch, 7.5f);
    q.updateWithValue(sketch, 11.5f);

    q.updateWithValue(sketch, 1.2f);
    q.updateWithValue(sketch, 2.2f);
    q.updateWithValue(sketch, 3.2f);
    q.updateWithValue(sketch, 4.2f);
    q.updateWithValue(sketch, 2.2f);
    q.updateWithValue(sketch, 1.2f);
    q.updateWithValue(sketch, -1.2f);
    q.updateWithValue(sketch, -3.2f);
    q.updateWithValue(sketch, 4.2f);
    q.updateWithValue(sketch, 7.2f);
    q.updateWithValue(sketch, 11.2f);

    ItemsSketch<Float> r = q.toSketch(sketch).value();
    Assert.assertArrayEquals(new Float[]{-3.5f, -1.2f, 1.5f, 2.5f, 4.2f, 7.2f, 11.5f}, r.getQuantiles(7));
  }

  @Test
  public void testQuantileMerge()
  {
    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch1 = q.newUnion(16, ValueDesc.FLOAT, null);

    q.updateWithValue(sketch1, 1.5f);
    q.updateWithValue(sketch1, 2.5f);
    q.updateWithValue(sketch1, 3.5f);
    q.updateWithValue(sketch1, 4.5f);
    q.updateWithValue(sketch1, 2.5f);
    q.updateWithValue(sketch1, 1.5f);
    q.updateWithValue(sketch1, -1.5f);
    q.updateWithValue(sketch1, -3.5f);
    q.updateWithValue(sketch1, 4.5f);
    q.updateWithValue(sketch1, 7.5f);
    q.updateWithValue(sketch1, 11.5f);

    TypedSketch<ItemsUnion> sketch2 = q.newUnion(16, ValueDesc.FLOAT, null);
    q.updateWithValue(sketch2, 1.2f);
    q.updateWithValue(sketch2, 2.2f);
    q.updateWithValue(sketch2, 3.2f);
    q.updateWithValue(sketch2, 4.2f);
    q.updateWithValue(sketch2, 2.2f);
    q.updateWithValue(sketch2, 1.2f);
    q.updateWithValue(sketch2, -1.2f);
    q.updateWithValue(sketch2, -3.2f);
    q.updateWithValue(sketch2, 4.2f);
    q.updateWithValue(sketch2, 7.2f);
    q.updateWithValue(sketch2, 11.2f);

    SketchBinaryFn binary = new SketchBinaryFn(16, q);
    TypedSketch<ItemsSketch> sketch = binary.merge(q.toSketch(sketch1), q.toSketch(sketch2));
    ItemsSketch<Float> r = sketch.value();
    Assert.assertArrayEquals(new Float[]{-3.5f, -1.2f, 1.5f, 2.5f, 4.2f, 7.2f, 11.5f}, r.getQuantiles(7));
  }

  @Test
  public void testQuantileMergePerf() throws Exception
  {
    final Random r = new Random();
    final int nomEntries = 16384;
    final ValueDesc type = ValueDesc.DOUBLE;

    SketchHandler.Quantile handler = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch1 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch1, r.nextLong() + r.nextDouble());
    }
    TypedSketch<ItemsUnion> sketch2 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch2, r.nextLong() + r.nextDouble());
    }
    TypedSketch<ItemsUnion> sketch3 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch3, r.nextLong() + r.nextDouble());
    }
    long p = System.currentTimeMillis();
    TypedSketch<ItemsUnion> union = handler.newUnion(nomEntries, type, null);
    handler.updateWithSketch(union, handler.toSketch(sketch1).value());
    handler.updateWithSketch(union, handler.toSketch(sketch2).value());
    handler.updateWithSketch(union, handler.toSketch(sketch3).value());

    ItemsSketch sketch = handler.toSketch(union).value();
    System.out.println("Quantiles.. " + Arrays.toString(sketch.getQuantiles(10)));
    System.out.println("Elapsed.. " + (System.currentTimeMillis() - p));
    Assert.assertTrue((System.currentTimeMillis() - p) < 150);

    sketch1 = writeAndRead(handler, sketch1);
    sketch2 = writeAndRead(handler, sketch2);
    sketch3 = writeAndRead(handler, sketch3);

    p = System.currentTimeMillis();
    union = handler.newUnion(nomEntries, type, null);
    handler.updateWithSketch(union, sketch1.value());
    handler.updateWithSketch(union, sketch2.value());
    handler.updateWithSketch(union, sketch3.value());
    sketch = handler.toSketch(union).value();
    System.out.println("Quantiles.. " + Arrays.toString(sketch.getQuantiles(10)));
    System.out.println("Elapsed.. " + (System.currentTimeMillis() - p));
    Assert.assertTrue((System.currentTimeMillis() - p) < 150);
  }

  @Test
  public void testThetaMergePerf() throws Exception
  {
    final Random r = new Random();
    final int nomEntries = 16384;
    final ValueDesc type = ValueDesc.DOUBLE;

    SketchHandler.Theta handler = new SketchHandler.Theta();
    TypedSketch<Union> sketch1 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch1, r.nextLong() + r.nextDouble());
    }
    TypedSketch<Union> sketch2 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch2, r.nextLong() + r.nextDouble());
    }
    TypedSketch<Union> sketch3 = handler.newUnion(nomEntries, type, null);
    for (int i = 0; i < 200_0000; i++) {
      handler.updateWithValue(sketch3, r.nextLong() + r.nextDouble());
    }
    long p = System.currentTimeMillis();
    TypedSketch<Union> union = handler.newUnion(nomEntries, type, null);
    handler.updateWithSketch(union, handler.toSketch(sketch1).value());
    handler.updateWithSketch(union, handler.toSketch(sketch2).value());
    handler.updateWithSketch(union, handler.toSketch(sketch3).value());

    Sketch sketch = handler.toSketch(union).value();
    System.out.println("Cardinality.. " + sketch.getEstimate());
    System.out.println("Elapsed.. " + (System.currentTimeMillis() - p));
    Assert.assertTrue((System.currentTimeMillis() - p) < 50);

    sketch1 = writeAndRead(handler, sketch1);
    sketch2 = writeAndRead(handler, sketch2);
    sketch3 = writeAndRead(handler, sketch3);

    p = System.currentTimeMillis();
    union = handler.newUnion(nomEntries, type, null);
    handler.updateWithSketch(union, sketch1.value());
    handler.updateWithSketch(union, sketch2.value());
    handler.updateWithSketch(union, sketch3.value());
    sketch = handler.toSketch(union).value();
    System.out.println("Cardinality.. " + sketch.getEstimate());
    System.out.println("Elapsed.. " + (System.currentTimeMillis() - p));
    Assert.assertTrue((System.currentTimeMillis() - p) < 50);
  }

  private TypedSketch writeAndRead(SketchHandler handler, TypedSketch sketch) throws Exception
  {
    final String object = TestHelper.JSON_MAPPER.writeValueAsString(new Object[] {handler.toSketch(sketch)});
    return TypedSketch.deserialize(handler.op(), TestHelper.JSON_MAPPER.readValue(object, Object[].class)[0], null);
  }

  @Test
  public void testStringQuantile()
  {
    List<String> values = Arrays.asList(
        "automotive",
        "business",
        "entertainment",
        "health",
        "mezzanine",
        "news",
        "premium",
        "technology",
        "travel"
    );

    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(16, ValueDesc.STRING, Ordering.natural());
    for (String value : values) {
      q.updateWithValue(sketch, value);
    }

    ItemsSketch<String> r = q.toSketch(sketch).value();
    Assert.assertArrayEquals(new String[]{"automotive", "mezzanine", "travel"}, r.getQuantiles(3));

    sketch = q.newUnion(16, ValueDesc.STRING, Ordering.natural().reverse());
    for (String value : values) {
      q.updateWithValue(sketch, value);
    }

    r = q.toSketch(sketch).value();
    Assert.assertArrayEquals(new String[]{"travel", "mezzanine", "automotive"}, r.getQuantiles(3));
  }

  @Test
  public void testStringArrayQuantile()
  {
    final char separator = '\u0001';
    Set<String> x1 = Sets.newHashSet(
        "automotive",
        "business",
        "entertainment",
        "health",
        "mezzanine",
        "news",
        "premium",
        "technology",
        "travel"
    );
    Set<String> x2 = Sets.newHashSet(
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    );
    List<String> values = Lists.newArrayList(
        Iterables.transform(
            Sets.cartesianProduct(x1, x2), new Function<List<String>, String>()
            {
              @Override
              public String apply(List<String> input)
              {
                return StringUtils.join(input, separator);
              }
            }
        )
    );
    Collections.shuffle(values);

    Comparator comparator = StringComparators.makeComparator(
        "stringArray(" + separator + ", lexicographic:desc, dayOfWeek.en)"
    );

    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(32, ValueDesc.STRING, comparator);
    for (String value : values) {
      q.updateWithValue(sketch, value);
    }

    ItemsSketch<String> r = q.toSketch(sketch).value();
    String[] expected = {
        "travel\u0001Monday", "travel\u0001Friday", "technology\u0001Wednesday", "premium\u0001Monday",
        "premium\u0001Saturday", "news\u0001Thursday", "mezzanine\u0001Tuesday", "mezzanine\u0001Saturday",
        "health\u0001Thursday", "entertainment\u0001Tuesday", "entertainment\u0001Sunday", "business\u0001Friday",
        "automotive\u0001Wednesday", "automotive\u0001Sunday"
    };
    Assert.assertArrayEquals(expected, r.getQuantiles(14));

    sketch = q.newUnion(32, ValueDesc.STRING, Ordering.from(comparator).reverse());
    for (String value : values) {
      q.updateWithValue(sketch, value);
    }

    r = q.toSketch(sketch).value();
    expected = new String[]{
        "automotive\u0001Sunday", "automotive\u0001Wednesday", "business\u0001Friday", "entertainment\u0001Sunday",
        "entertainment\u0001Tuesday", "health\u0001Thursday", "mezzanine\u0001Saturday", "mezzanine\u0001Tuesday",
        "news\u0001Thursday", "premium\u0001Saturday", "premium\u0001Monday", "technology\u0001Wednesday",
        "travel\u0001Friday", "travel\u0001Monday"
    };
    Assert.assertArrayEquals(expected, r.getQuantiles(14));
  }

  @Test
  public void testStructQuantile()
  {
    Set<Object> x1 = Sets.<Object>newHashSet(
        "automotive",
        "business",
        "entertainment",
        "health",
        "mezzanine",
        "news",
        "premium",
        "technology",
        "travel"
    );
    Set<Object> x2 = Sets.<Object>newHashSet(
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    );
    Set<Object> x3 = Sets.<Object>newHashSet(
        6, 5, 4, 3, 2, 1
    );
    List<Object[]> values = Lists.newArrayList(
        Iterables.transform(
            Sets.cartesianProduct(x1, x2, x3), new Function<List<Object>, Object[]>()
            {
              @Override
              public Object[] apply(List<Object> input)
              {
                return input.toArray(new Object[3]);
              }
            }
        )
    );
    Collections.shuffle(values);

    final Comparator c1 = Ordering.natural().reverse();
    final Comparator c2 = StringComparators.DAY_OF_WEEK;
    final Comparator c3 = Ordering.natural();
    final Comparator[] cx = new Comparator[] {c1, c2, c3};
    final Comparator<Object[]> c = new Comparator<Object[]>()
    {
      @Override
      public int compare(Object[] o1, Object[] o2)
      {
        int compare = 0;
        for (int i = 0; compare == 0 && i < cx.length; i++) {
          compare = cx[i].compare(o1[i], o2[i]);
        }
        return compare;
      }
    };

    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(256, ValueDesc.STRING, c);
    for (Object[] value : values) {
      q.updateWithValue(sketch, value);
    }

    ItemsSketch<Object[]> r = q.toSketch(sketch).value();
    Object[][] expected = {
        a("travel", "Monday", 1), a("travel", "Saturday", 5), a("technology", "Friday", 3), a("premium", "Thursday", 2),
        a("news", "Tuesday", 6), a("mezzanine", "Monday", 4), a("mezzanine", "Sunday", 3), a("health", "Saturday", 1),
        a("entertainment", "Thursday", 5), a("business", "Wednesday", 4), a("automotive", "Tuesday", 2),
        a("automotive", "Sunday", 6)
    };
    Assert.assertArrayEquals(expected, r.getQuantiles(12));
  }

  private static Object[] a(Object... x)
  {
    return x;
  }
}
