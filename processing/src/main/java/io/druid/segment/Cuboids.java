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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.GranularityType;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactory.CubeSupport;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cuboids
{
  private static final Logger LOG = new Logger(Cuboids.class);

  public static final String COUNT_ALL_METRIC = "$$";
  public static final Set<String> BASIC_AGGREGATORS = ImmutableSet.of("count", "min", "max", "sum");

  public static final int PAGES = 16;
  public static final int BUFFER_SIZE = 256 << 10;
  public static final int METRIC_COMPRESSION_DISABLE = 32;

  private static final Pattern CUBOID = Pattern.compile("^___(\\d+)_(.+)___(.*)?$");

  private static final int GRANULARITY_SHIFT = 5;   // reserve 1 more
  private static final BigInteger GRANULARITIES_MASK = BigInteger.valueOf(0b11111);

  public static String dimension(BigInteger cubeId, String columnName)
  {
    return String.format("___%d_%s___", cubeId, columnName);
  }

  public static String metric(BigInteger cubeId, String columnName, String aggregation)
  {
    return String.format("___%d_%s___%s", cubeId, columnName, aggregation);
  }

  public static String metricColumn(String metricName, String aggregation)
  {
    return String.format("%s___%s", metricName, aggregation);
  }

  public static BigInteger toCubeId(int[] cubeDimIndices, GranularityType granularity)
  {
    BigInteger cubeId = BigInteger.ZERO;
    for (int cubeDimIndex : cubeDimIndices) {
      cubeId = cubeId.add(BigInteger.ONE.shiftLeft(cubeDimIndex));
    }
    return cubeId.shiftLeft(GRANULARITY_SHIFT).add(BigInteger.valueOf(granularity.ordinal()));
  }

  public static GranularityType getGranularity(BigInteger cubeId)
  {
    return GranularityType.values()[cubeId.and(GRANULARITIES_MASK).intValue()];
  }

  static Map<BigInteger, CuboidSpec> extractCuboids(Iterable<String> values)
  {
    final Map<BigInteger, CuboidSpec> cuboids = Maps.newHashMap();
    final Matcher matcher = CUBOID.matcher("");
    for (String value : values) {
      matcher.reset(value);
      if (matcher.matches()) {
        BigInteger cubeId = new BigInteger(matcher.group(1));
        String columnName = matcher.group(2);
        String aggregator = matcher.group(3);
        CuboidSpec cuboid = cuboids.computeIfAbsent(cubeId, new Function<BigInteger, CuboidSpec>()
        {
          @Override
          public CuboidSpec apply(BigInteger cubeId)
          {
            return new CuboidSpec(getGranularity(cubeId), Lists.newArrayList(), Maps.newHashMap());
          }
        });
        if (columnName.equals(Row.TIME_COLUMN_NAME)) {
          continue;
        }
        if (StringUtils.isNullOrEmpty(aggregator)) {
          cuboid.getDimensions().add(columnName);
        } else {
          cuboid.getMetrics().computeIfAbsent(
              columnName, new Function<String, Set<String>>()
              {
                @Override
                public Set<String> apply(String s) { return Sets.newHashSet();}
              }
          ).add(aggregator);
        }
      }
    }
    return cuboids;
  }

  // hate this
  static AggregatorFactory convert(String aggregator, String name, String fieldName, ValueDesc inputType)
  {
    switch (aggregator) {
      case "count":
        return CountAggregatorFactory.of(name, fieldName);
      case "min":
        return new GenericMinAggregatorFactory(name, fieldName, inputType);
      case "max":
        return new GenericMaxAggregatorFactory(name, fieldName, inputType);
      case "sum":
        return new GenericSumAggregatorFactory(name, fieldName, inputType);
    }
    LOG.warn("Not supported cube aggregator [%s]", aggregator);
    return null;
  }

  static String name(AggregatorFactory aggregator)
  {
    if (!(aggregator instanceof CubeSupport)) {
      return null;
    }
    final CubeSupport cubeSupport = (CubeSupport) aggregator;
    if (cubeSupport.getPredicate() != null) {
      return null;   // cannot
    }
    return aggregator instanceof CountAggregatorFactory ? "count" :
           aggregator instanceof GenericMinAggregatorFactory ? "min" :
           aggregator instanceof GenericMaxAggregatorFactory ? "max" :
           aggregator instanceof GenericSumAggregatorFactory ? "sum" : null;
  }

  public static boolean supports(Map<String, Set<String>> metrics, List<AggregatorFactory> aggregators)
  {
    for (AggregatorFactory aggregator : aggregators) {
      if (!supports(metrics, aggregator)) {
        return false;
      }
    }
    return true;
  }

  private static boolean supports(Map<String, Set<String>> metrics, AggregatorFactory aggregator)
  {
    if (!(aggregator instanceof CubeSupport)) {
      return false;
    }
    final CubeSupport cubeSupport = (CubeSupport) aggregator;
    if (cubeSupport.getPredicate() != null) {
      return false;   // cannot
    }
    if (AggregatorFactory.isCountAll(aggregator)) {
      Set<String> supports = metrics.get(COUNT_ALL_METRIC);
      return supports != null && supports.contains(cubeSupport.getCubeName());
    }
    final Set<String> supports = metrics.get(cubeSupport.getFieldName());
    return !GuavaUtils.isNullOrEmpty(supports) && supports.contains(cubeSupport.getCubeName());
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseAggregationQuery> T rewrite(T query)
  {
    final List<AggregatorFactory> rewritten = Lists.newArrayList();
    for (AggregatorFactory aggregator : query.getAggregatorSpecs()) {
      rewritten.add(toCubeCombiner(aggregator));
    }
    return (T) query.withAggregatorSpecs(rewritten);
  }

  private static AggregatorFactory toCubeCombiner(AggregatorFactory aggregator)
  {
    if (AggregatorFactory.isCountAll(aggregator)) {
      return new LongSumAggregatorFactory(aggregator.getName(), COUNT_ALL_METRIC);
    }
    final CubeSupport cubeSupport = (CubeSupport) aggregator;
    return cubeSupport.getCombiningFactory(
        Cuboids.metricColumn(cubeSupport.getFieldName(), cubeSupport.getCubeName())
    );
  }
}
