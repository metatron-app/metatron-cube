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

package io.druid.query.sketch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SummaryQueryTest extends SketchQueryRunnerTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testSummaryQuery() throws Exception
  {
    SummaryQuery query = new SummaryQuery(
        TableDataSource.of(TestIndex.MMAPPED_SPLIT),
        new MultipleIntervalSegmentSpec(Arrays.asList(TestIndex.INTERVAL)),
        null,
        DefaultDimensionSpec.toSpec("market", "quality", "partial_null_column"),
        Arrays.asList("index", "indexMin"),
        null,
        0,
        true,
        true,
        null
    );

    List result = Sequences.toList(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(1, result.size());

    Map<String, Map<String, Object>> summary = (Map<String, Map<String, Object>>) result.get(0);
    Assert.assertEquals(6, summary.size());

    // time
    Map<String, Object> timeStats = summary.get("__time");
    List perSegments = (List) timeStats.get("segments");
    Assert.assertEquals(2, perSegments.size());
    Map<String, Object> segment1 = (Map<String, Object>) perSegments.get(0);
    Assert.assertEquals(TestIndex.INTERVAL_TOP, segment1.get("interval"));
    Assert.assertEquals(611l, segment1.get("rows"));
    Assert.assertEquals(611l, segment1.get("ingestedRows"));
    Assert.assertEquals(26751l, segment1.get("serializedSize"));
    Assert.assertNotNull(segment1.get("lastAccessTime"));

    Map<String, Object> segment2 = (Map<String, Object>) perSegments.get(1);
    Assert.assertEquals(TestIndex.INTERVAL_BOTTOM, segment2.get("interval"));
    Assert.assertEquals(598l, segment2.get("rows"));
    Assert.assertEquals(598l, segment2.get("ingestedRows"));
    Assert.assertEquals(26386l, segment2.get("serializedSize"));
    Assert.assertNotNull(segment2.get("lastAccessTime"));

    // dimension
    Map<String, Object> marketStats = summary.get("market");
    Assert.assertEquals(ValueDesc.STRING, marketStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("string", 2), marketStats.get("typeDetail"));
    Assert.assertEquals("spot", marketStats.get("min"));
    Assert.assertEquals("upfront", marketStats.get("max"));
    Assert.assertEquals("spot", marketStats.get("median"));
    Assert.assertArrayEquals(
        new String[]{"spot", "total_market", "upfront"}, (String[]) marketStats.get("quantiles")
    );
    Assert.assertArrayEquals(
        new double[]{0.6923076923076923, 0.3076923076923077}, (double[]) marketStats.get("pmf"), 0.0001
    );
    Assert.assertArrayEquals(
        new double[]{0.6923076923076923, 1.0}, (double[]) marketStats.get("cdf"), 0.0001
    );
    Assert.assertArrayEquals(
        new Object[]{"spot", "total_market"}, (Object[]) marketStats.get("iqr")
    );
    Assert.assertEquals(1209l, marketStats.get("count"));
    List frequentItems = ((List) marketStats.get("frequentItems"));
    Assert.assertEquals(3, frequentItems.size());
    Assert.assertEquals(ImmutableMap.of("value", "spot", "count", 837), frequentItems.get(0));
    Assert.assertEquals(ImmutableMap.of("value", "total_market", "count", 186), frequentItems.get(1));
    Assert.assertEquals(ImmutableMap.of("value", "upfront", "count", 186), frequentItems.get(2));
    Assert.assertEquals(0l, marketStats.get("missing"));
    Assert.assertEquals(3.0d, marketStats.get("cardinality"));

    // dimension
    Map<String, Object> qualityStats = summary.get("quality");
    Assert.assertEquals(ValueDesc.STRING, qualityStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("string", 2), qualityStats.get("typeDetail"));
    Assert.assertEquals("automotive", qualityStats.get("min"));
    Assert.assertEquals("travel", qualityStats.get("max"));
    Assert.assertEquals("mezzanine", qualityStats.get("median"));
    Assert.assertArrayEquals(
        new String[]{
            "automotive", "business", "entertainment", "health",
            "mezzanine", "news", "premium", "technology", "travel"
        },
        (String[]) qualityStats.get("quantiles")
    );
    final double x = 0.07692307692307693;
    Assert.assertArrayEquals(
        new double[]{x, x, x, x, x * 3, x, x * 3, x * 2},
        (double[]) qualityStats.get("pmf"), 0.0001
    );
    Assert.assertArrayEquals(
        new double[]{x, x * 2, x * 3, x * 4, x * 7, x * 8, x * 11, x * 13},
        (double[]) qualityStats.get("cdf"), 0.0001
    );
    Assert.assertArrayEquals(new Object[]{"health", "premium"}, (Object[]) qualityStats.get("iqr"));
    Assert.assertEquals(1209l, qualityStats.get("count"));
    frequentItems = ((List) qualityStats.get("frequentItems"));
    Assert.assertEquals(9, frequentItems.size());
    Assert.assertEquals(ImmutableMap.of("value", "mezzanine", "count", 279), frequentItems.get(0));
    Assert.assertEquals(ImmutableMap.of("value", "premium", "count", 279), frequentItems.get(1));
    Assert.assertEquals(ImmutableMap.of("value", "automotive", "count", 93), frequentItems.get(2));
    Assert.assertEquals(0l, qualityStats.get("missing"));
    Assert.assertEquals(9.0d, qualityStats.get("cardinality"));

    // dimension
    Map<String, Object> nullStats = summary.get("partial_null_column");
    Assert.assertEquals(ValueDesc.STRING, nullStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("string", 2), nullStats.get("typeDetail"));
    Assert.assertEquals(186l, nullStats.get("count"));
    frequentItems = ((List) nullStats.get("frequentItems"));
    Assert.assertEquals(2, frequentItems.size());
    Assert.assertEquals(ImmutableMap.of("value", "", "count", 1023), frequentItems.get(0));
    Assert.assertEquals(ImmutableMap.of("value", "value", "count", 186), frequentItems.get(1));
    Assert.assertEquals(1023l, nullStats.get("missing"));
    Assert.assertEquals(1.0d, nullStats.get("cardinality"));

    // metric
    Map<String, Object> indexStats = summary.get("index");
    Assert.assertEquals(ValueDesc.DOUBLE, indexStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("double", 2), indexStats.get("typeDetail"));
    Assert.assertEquals(59.02102279663086d, indexStats.get("min"));
    Assert.assertEquals(1870.06103515625d, indexStats.get("max"));
    Assert.assertEquals(122.128173828125d, indexStats.get("median"));
    Assert.assertArrayEquals(
        new Double[]{
            59.02102279663086, 96.59458923339844, 103.10391998291016, 108.39460754394531,
            115.01331329345703, 122.128173828125, 134.415283203125, 691.9589233398438, 1006.402099609375,
            1179.6959228515625, 1870.06103515625
        },
        (Double[]) indexStats.get("quantiles")
    );
    Assert.assertArrayEquals(
        new Object[]{105.61347198486328, 873.3065185546875},
        (Object[]) indexStats.get("iqr")
    );
    Assert.assertEquals(1209l, indexStats.get("count"));
    Assert.assertArrayEquals(
        new double[]{-1045.926097869873, 2024.8460884094238},
        (double[]) indexStats.get("outlierThreshold"), 0.0001
    );
    Assert.assertEquals(0l, indexStats.get("zeros"));
    Assert.assertEquals(416.321345853845, indexStats.get("mean"));
    Assert.assertEquals(223501.33231703882, indexStats.get("variance"));
    Assert.assertEquals(472.7592752311041, indexStats.get("stddev"));
    Assert.assertEquals(-0.397621490102432, indexStats.get("skewness"));
    Assert.assertEquals(0l, indexStats.get("outliers"));
    Assert.assertEquals(0.9999999999999998, indexStats.get("covariance.indexMin"));
    Assert.assertEquals(1199.0d, indexStats.get("cardinality"));

    Map<String, Object> indexMinStats = summary.get("indexMin");
    Assert.assertEquals(ValueDesc.FLOAT, indexMinStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("float", 2), indexMinStats.get("typeDetail"));
    Assert.assertEquals(59.021023f, indexMinStats.get("min"));
    Assert.assertEquals(1870.061f, indexMinStats.get("max"));
    Assert.assertEquals(122.128174f, indexMinStats.get("median"));
    Assert.assertArrayEquals(
        new Float[]{
            59.021023f, 96.59459f, 103.10392f, 108.39461f,
            115.01331f, 122.128174f, 134.41528f, 691.9589f, 1006.4021f,
            1179.6959f, 1870.061f
        },
        (Float[]) indexMinStats.get("quantiles")
    );
    Assert.assertArrayEquals(
        new Object[]{105.61347f, 873.3065f},
        (Object[]) indexMinStats.get("iqr")
    );
    Assert.assertEquals(1209l, indexMinStats.get("count"));
    Assert.assertArrayEquals(
        new double[]{-1045.926097869873, 2024.8460884094238},
        (double[]) indexMinStats.get("outlierThreshold"), 0.0001
    );
    Assert.assertEquals(0l, indexMinStats.get("zeros"));
    Assert.assertEquals(416.321345853845, indexMinStats.get("mean"));
    Assert.assertEquals(223501.33231703882, indexMinStats.get("variance"));
    Assert.assertEquals(472.7592752311041, indexMinStats.get("stddev"));
    Assert.assertEquals(-0.397621490102432, indexMinStats.get("skewness"));
    Assert.assertEquals(0l, indexMinStats.get("outliers"));
    Assert.assertEquals(1199.0d, indexMinStats.get("cardinality"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSummaryQueryWithRound() throws Exception
  {
    SummaryQuery query = new SummaryQuery(
        TableDataSource.of(TestIndex.MMAPPED_SPLIT),
        new MultipleIntervalSegmentSpec(Arrays.asList(TestIndex.INTERVAL)),
        null,
        null,
        Arrays.asList("index"),
        null,
        2,
        false,
        false,
        null
    );

    List result = Sequences.toList(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(1, result.size());

    Map<String, Map<String, Object>> summary = (Map<String, Map<String, Object>>) result.get(0);
    Assert.assertEquals(1, summary.size());

    Map<String, Object> indexStats = summary.get("index");
    Assert.assertEquals(ValueDesc.DOUBLE, indexStats.get("type"));
    Assert.assertEquals(ImmutableMap.of("double", 2), indexStats.get("typeDetail"));
    Assert.assertEquals(59.02102279663086d, indexStats.get("min"));
    Assert.assertEquals(1870.06103515625d, indexStats.get("max"));
    Assert.assertEquals(122.128173828125d, indexStats.get("median"));
    Assert.assertArrayEquals(
        new Double[]{
            59.02102279663086, 96.59458923339844, 103.10391998291016, 108.39460754394531,
            115.01331329345703, 122.128173828125, 134.415283203125, 691.9589233398438, 1006.402099609375,
            1179.6959228515625, 1870.06103515625
        },
        (Double[]) indexStats.get("quantiles")
    );
    Assert.assertArrayEquals(
        new Object[]{105.61347198486328, 873.3065185546875},
        (Object[]) indexStats.get("iqr")
    );
    Assert.assertEquals(1209l, indexStats.get("count"));
    Assert.assertArrayEquals(
        new double[]{-1045.926097869873, 2024.8460884094238},
        (double[]) indexStats.get("outlierThreshold"), 0.0001
    );
    Assert.assertEquals(0l, indexStats.get("zeros"));
    Assert.assertEquals(416.32, indexStats.get("mean"));
    Assert.assertEquals(223501.33, indexStats.get("variance"));
    Assert.assertEquals(472.76, indexStats.get("stddev"));
    Assert.assertEquals(-0.398, indexStats.get("skewness"));
    Assert.assertEquals(0l, indexStats.get("outliers"));
    Assert.assertEquals(1199.0d, indexStats.get("cardinality"));
  }
}
