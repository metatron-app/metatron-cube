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

package io.druid.query.aggregation.post;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.TypeResolver;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ArithmeticPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    PostAggregator.Processor arithmeticPostAggregator;
    PostAggregator.Processor mathPostAggregator;
    CountAggregator agg = new CountAggregator();
    MutableLong aggregate = null;
    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put("rows", agg.get(aggregate));

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    DateTime timestamp = DateTime.now();
    for (PostAggregator.Processor postAggregator : PostAggregators.toProcessors(postAggregatorList)) {
      metricValues.put(postAggregator.getName(), postAggregator.compute(timestamp, metricValues));
    }

    arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggregatorList).processor(TypeResolver.UNKNOWN);
    mathPostAggregator = new MathPostAggregator("add", "roku + rows").processor(TypeResolver.UNKNOWN);
    Assert.assertEquals(9.0, arithmeticPostAggregator.compute(timestamp, metricValues));
    Assert.assertEquals(9L, mathPostAggregator.compute(timestamp, metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("subtract", "-", postAggregatorList).processor(TypeResolver.UNKNOWN);
    mathPostAggregator = new MathPostAggregator("add", "roku - rows").processor(TypeResolver.UNKNOWN);
    Assert.assertEquals(3.0, arithmeticPostAggregator.compute(timestamp, metricValues));
    Assert.assertEquals(3L, mathPostAggregator.compute(timestamp, metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("multiply", "*", postAggregatorList).processor(TypeResolver.UNKNOWN);
    mathPostAggregator = new MathPostAggregator("add", "roku * rows").processor(TypeResolver.UNKNOWN);
    Assert.assertEquals(18.0, arithmeticPostAggregator.compute(timestamp, metricValues));
    Assert.assertEquals(18L, mathPostAggregator.compute(timestamp, metricValues));

    arithmeticPostAggregator = new ArithmeticPostAggregator("divide", "/", postAggregatorList).processor(TypeResolver.UNKNOWN);
    mathPostAggregator = new MathPostAggregator("add", "roku / rows").processor(TypeResolver.UNKNOWN);
    Assert.assertEquals(2.0, arithmeticPostAggregator.compute(timestamp, metricValues));
    Assert.assertEquals(2L, mathPostAggregator.compute(timestamp, metricValues));
  }

  @Test
  public void testComparator()
  {
    ArithmeticPostAggregator arithmeticPostAggregator;
    MutableLong aggregate = null;
    CountAggregator agg = new CountAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put("rows", agg.get(aggregate));

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    arithmeticPostAggregator = new ArithmeticPostAggregator("add", "+", postAggregatorList);
    Comparator comp = arithmeticPostAggregator.getComparator();
    PostAggregator.Processor processor = arithmeticPostAggregator.processor(TypeResolver.UNKNOWN);

    DateTime timestamp = DateTime.now();
    Object before = processor.compute(timestamp, metricValues);
    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    aggregate = agg.aggregate(aggregate);
    metricValues.put("rows", agg.get(aggregate));
    Object after = processor.compute(timestamp, metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }

  @Test
  public void testQuotient() throws Exception
  {
    PostAggregator.Processor agg = new ArithmeticPostAggregator(
        null,
        "quotient",
        ImmutableList.<PostAggregator>of(
            new FieldAccessPostAggregator("numerator", "value"),
            new ConstantPostAggregator("zero", 0)
        ),
        "numericFirst"
    ).processor(TypeResolver.UNKNOWN);

    DateTime timestamp = DateTime.now();
    Assert.assertEquals(Double.NaN, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", 0)));
    Assert.assertEquals(Double.NaN, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", Double.NaN)));
    Assert.assertEquals(Double.POSITIVE_INFINITY, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", 1)));
    Assert.assertEquals(Double.NEGATIVE_INFINITY, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", -1)));
  }

  @Test
  public void testDiv() throws Exception
  {
    PostAggregator.Processor agg = new ArithmeticPostAggregator(
        null,
        "/",
        ImmutableList.of(
            new FieldAccessPostAggregator("numerator", "value"),
            new ConstantPostAggregator("denomiator", 0)
        )
    ).processor(TypeResolver.UNKNOWN);
    DateTime timestamp = DateTime.now();
    Assert.assertEquals(0.0, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", 0)));
    Assert.assertEquals(0.0, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", Double.NaN)));
    Assert.assertEquals(0.0, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", 1)));
    Assert.assertEquals(0.0, agg.compute(timestamp, ImmutableMap.<String, Object>of("value", -1)));
  }

  @Test
  public void testNumericFirstOrdering() throws Exception
  {
    ArithmeticPostAggregator agg = new ArithmeticPostAggregator(
        null,
        "quotient",
        ImmutableList.<PostAggregator>of(
            new ConstantPostAggregator("zero", 0),
            new ConstantPostAggregator("zero", 0)
        ),
        "numericFirst"
    );
    final Comparator numericFirst = agg.getComparator();
    Assert.assertTrue(numericFirst.compare(Double.NaN, 0.0) < 0);
    Assert.assertTrue(numericFirst.compare(Double.POSITIVE_INFINITY, 0.0) < 0);
    Assert.assertTrue(numericFirst.compare(Double.NEGATIVE_INFINITY, 0.0) < 0);
    Assert.assertTrue(numericFirst.compare(0.0, Double.NaN) > 0);
    Assert.assertTrue(numericFirst.compare(0.0, Double.POSITIVE_INFINITY) > 0);
    Assert.assertTrue(numericFirst.compare(0.0, Double.NEGATIVE_INFINITY) > 0);

    Assert.assertTrue(numericFirst.compare(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY) < 0);
    Assert.assertTrue(numericFirst.compare(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY) > 0);
    Assert.assertTrue(numericFirst.compare(Double.NaN, Double.POSITIVE_INFINITY) > 0);
    Assert.assertTrue(numericFirst.compare(Double.NaN, Double.NEGATIVE_INFINITY) > 0);
    Assert.assertTrue(numericFirst.compare(Double.POSITIVE_INFINITY, Double.NaN) < 0);
    Assert.assertTrue(numericFirst.compare(Double.NEGATIVE_INFINITY, Double.NaN) < 0);
  }
}
