/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.granularity.GranularityType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CuboidsTest
{
  @Test
  public void test()
  {
    List<String> baseDimensions = Arrays.asList("L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_RETURNFLAG");

    List<String> dimensions8 = Arrays.asList("L_LINENUMBER");
    BigInteger cubeId8 = Cuboids.toCubeId(GuavaUtils.indexOf(baseDimensions, dimensions8), GranularityType.ALL);
    Iterable<String> names8 = buildNames(
        cubeId8,
        dimensions8, Arrays.asList("L_DISCOUNT", "L_EXTENDEDPRICE", "L_QUANTITY")
    );
    List<String> dimensions10 = Arrays.asList("L_LINENUMBER", "L_RETURNFLAG");
    BigInteger cubeId10 = Cuboids.toCubeId(GuavaUtils.indexOf(baseDimensions, dimensions10), GranularityType.ALL);
    Iterable<String> names10 = buildNames(
        cubeId10,
        dimensions10, Arrays.asList("L_EXTENDEDPRICE", "L_QUANTITY")
    );
    Map<BigInteger, CuboidSpec> cuboids = Cuboids.extractCuboids(Iterables.concat(names8, names10));
    Assert.assertEquals(2, cuboids.size());
    CuboidSpec cube8 = cuboids.get(cubeId8);
    Assert.assertNotNull(cube8);
    Assert.assertEquals(dimensions8, cube8.getDimensions());

    Map<String, Set<String>> metrics8 = ImmutableMap.of(
        "L_DISCOUNT", Sets.newHashSet(Arrays.asList("count", "min", "max", "sum")),
        "L_EXTENDEDPRICE", Sets.newHashSet(Arrays.asList("count", "min", "max", "sum")),
        "L_QUANTITY", Sets.newHashSet(Arrays.asList("count", "min", "max", "sum"))
    );
    Assert.assertEquals(metrics8, cube8.getMetrics());

    CuboidSpec cube10 = cuboids.get(cubeId10);
    Assert.assertNotNull(cube10);
    Assert.assertEquals(dimensions10, cube10.getDimensions());
    Map<String, Set<String>> metrics10 = ImmutableMap.of(
        "L_EXTENDEDPRICE", Sets.newHashSet(Arrays.asList("count", "min", "max", "sum")),
        "L_QUANTITY", Sets.newHashSet(Arrays.asList("count", "min", "max", "sum"))
    );
    Assert.assertEquals(metrics10, cube10.getMetrics());
  }

  private static Iterable<String> buildNames(BigInteger cubeId, List<String> dimensions, List<String> metrics)
  {
    return Iterables.concat(
        Iterables.transform(dimensions, new Function<String, String>()
        {
          @Override
          public String apply(String input)
          {
            return Cuboids.dimension(cubeId, input);
          }
        }),
        Iterables.concat(Iterables.transform(metrics, new Function<String, Iterable<String>>()
        {
          final List<String> aggregators = Arrays.asList("count", "min", "max", "sum");

          @Override
          public Iterable<String> apply(final String metric)
          {
            return Iterables.transform(aggregators, new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return Cuboids.metric(cubeId, metric, input);
              }
            });
          }
        }))
    );
  }
}