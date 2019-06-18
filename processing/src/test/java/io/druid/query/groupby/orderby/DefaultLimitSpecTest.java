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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.Intervals;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.ordering.Direction;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class DefaultLimitSpecTest
{
  private final GroupByQuery query = new GroupByQuery(
      TableDataSource.of("test"),
      MultipleIntervalSegmentSpec.of(Intervals.ETERNITY), null, Granularities.ALL,
      null, null, null, null, null, null, null, null, null, null
  );

  private final List<Row> testRowsList;
  private final Sequence<Row> testRowsSequence;

  public DefaultLimitSpecTest()
  {
    testRowsList = ImmutableList.of(
        createRow("2011-04-01", "k1", 9.0d, "k2", 2L, "k3", 3L),
        createRow("2011-04-01", "k1", 10.0d, "k2", 1L, "k3", 2L),
        createRow("2011-04-01", "k1", 20.0d, "k2", 3L, "k3", 1L)
    );

    testRowsSequence = Sequences.simple(testRowsList);
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.getObjectMapper();

    //defaults
    String json = "{\"type\": \"default\"}";

    LimitSpec spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );

    Assert.assertEquals(
        new LimitSpec(null, null),
        spec
    );

    //non-defaults
    json = "{\n"
           + "  \"type\":\"default\",\n"
           + "  \"columns\":[{\"dimension\":\"d\",\"direction\":\"ASCENDING\"}],\n"
           + "  \"limit\":10\n"
           + "}";

    spec = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json, LimitSpec.class)),
        LimitSpec.class
    );

    Assert.assertEquals(
        new LimitSpec(ImmutableList.of(new OrderByColumnSpec("d", Direction.ASCENDING)), 10),
        spec
    );
  }

  @Test
  public void testBuildSimple()
  {
    LimitSpec limitSpec = LimitSpecs.of(2);

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        query,
        true
    );

    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );
  }

  @Test
  public void testBuildWithExplicitOrder()
  {
    LimitSpec limitSpec = LimitSpecs.of(2, new OrderByColumnSpec("k1", Direction.ASCENDING));

    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        query.withDimensionSpecs(
            DefaultDimensionSpec.toSpec("k1"))
             .withAggregatorSpecs(
                 ImmutableList.<AggregatorFactory>of(new LongSumAggregatorFactory("k2", "k2")))
             .withPostAggregatorSpecs(
                 ImmutableList.<PostAggregator>of(new ConstantPostAggregator("k3", 1L))),
        true
    );
    // changed to use natural-ordering instead of toString & compare
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );
  }

  @Test
  @Ignore("not allowed in aggregation query.. todo?")
  public void testBuildWithExplicitOrderInvalid()
  {
    LimitSpec limitSpec = LimitSpecs.of(2, new OrderByColumnSpec("k1", Direction.ASCENDING));

    // if there is a post-aggregator with same name then that is used to build ordering
    Function<Sequence<Row>, Sequence<Row>> limitFn = limitSpec.build(
        query.withDimensionSpecs(
            DefaultDimensionSpec.toSpec("k1"))
             .withAggregatorSpecs(
                 ImmutableList.<AggregatorFactory>of(new LongSumAggregatorFactory("k1", "k1")))
             .withPostAggregatorSpecs(
                 ImmutableList.<PostAggregator>of(new ConstantPostAggregator("k3", 1L))),
        true
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );

    limitFn = limitSpec.build(
        query.withDimensionSpecs(
                 DefaultDimensionSpec.toSpec("k1"))
             .withAggregatorSpecs(
                 ImmutableList.<AggregatorFactory>of(new LongSumAggregatorFactory("k2", "k2")))
             .withPostAggregatorSpecs(
                 ImmutableList.<PostAggregator>of(
                     new ArithmeticPostAggregator(
                         "k1", "+",
                         ImmutableList.<PostAggregator>of(
                             new ConstantPostAggregator("x", 1),
                             new ConstantPostAggregator("y", 1))
                     ))),
        true
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );

    limitFn = limitSpec.build(
        query.withDimensionSpecs(
                 DefaultDimensionSpec.toSpec("k1"))
             .withAggregatorSpecs(
                 ImmutableList.<AggregatorFactory>of(new LongSumAggregatorFactory("k2", "k2")))
             .withPostAggregatorSpecs(
                 ImmutableList.<PostAggregator>of(new MathPostAggregator("k1", "1 + 1"))),
        true
    );
    Assert.assertEquals(
        ImmutableList.of(testRowsList.get(0), testRowsList.get(1)),
        Sequences.toList(limitFn.apply(testRowsSequence), new ArrayList<Row>())
    );
  }

  private Row createRow(String timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = Maps.newHashMap();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = new DateTime(timestamp);
    return new MapBasedRow(ts, theVals);
  }
}
