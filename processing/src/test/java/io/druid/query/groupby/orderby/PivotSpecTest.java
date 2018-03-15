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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.ordering.Direction;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PivotSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.getObjectMapper();

    String json0 = "{\"pivotColumns\": [\"col0\"], \"valueColumns\": [\"col1\"] }";
    PivotSpec spec0 = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json0, PivotSpec.class)),
        PivotSpec.class
    );
    PivotSpec expected0 = PivotSpec.of(PivotColumnSpec.toSpecs("col0"), "col1");
    Assert.assertEquals(expected0.hashCode(), expected0.hashCode());
    Assert.assertEquals(expected0, spec0);
    Assert.assertTrue(Arrays.equals(expected0.getCacheKey(), expected0.getCacheKey()));

    String json1 = "{" +
                   "\"pivotColumns\": ["
                   + "{\"dimension\": \"col0\", \"direction\": \"DESCENDING\", \"values\": [\"val1\", \"val2\"] } " +
                   "]," +
                   "\"valueColumns\": [\"col1\"] }";
    PivotSpec spec1 = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json1, PivotSpec.class)),
        PivotSpec.class
    );
    PivotSpec expected1 = PivotSpec.of(
        Arrays.asList(
            new PivotColumnSpec(
                "col0", null,
                Direction.DESCENDING, null, Arrays.asList("val1", "val2")
            )
        ),
        "col1"
    );
    Assert.assertEquals(expected1.hashCode(), spec1.hashCode());
    Assert.assertEquals(expected1, spec1);
    Assert.assertTrue(Arrays.equals(expected1.getCacheKey(), expected1.getCacheKey()));

    String json2 =
        "{" +
        "\"pivotColumns\": ["
        + "{\"dimension\": \"col0\", \"direction\": \"DESCENDING\", \"values\": [\"val1\", \"val2\"] } " +
        "]," +
        "\"valueColumns\": [\"col1\"], " +
        "\"separator\": \"|\", " +
        "\"partitionExpressions\": [{\"condition\": \"^value1|.*\", \"expression\": \"_=$sum(_)\"}], " +
        "\"tabularFormat\": true" +
        "}";
    PivotSpec spec2 = mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(json2, PivotSpec.class)),
        PivotSpec.class
    );
    PivotSpec expected2 = PivotSpec.tabular(
        Arrays.asList(
            new PivotColumnSpec(
                "col0", null,
                Direction.DESCENDING, null, Arrays.asList("val1", "val2")
            )
        ),
        "col1"
    ).withSeparator("|").withPartitionExpressions(PartitionExpression.of(new String[] {"^value1|.*", "_=$sum(_)"}));
    Assert.assertEquals(expected2.hashCode(), spec2.hashCode());
    Assert.assertEquals(expected2, spec2);
    Assert.assertTrue(Arrays.equals(expected2.getCacheKey(), expected2.getCacheKey()));
  }

  @Test
  public void test()
  {
    final String[] columns1 = new String[]{"col1", "col2", "col3"};
    List<Row> rows1 = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns1,
        new Object[]{"a", "x", 1},
        new Object[]{"b", "y", 2},
        new Object[]{"c", "y", 3}
    );

    final String[] columns2 = new String[]{"col1", "x", "y"};
    List<Row> expected = GroupByQueryRunnerTestHelper.createExpectedRows(
        columns2,
        new Object[]{"a", 1, null},
        new Object[]{"b", null, 2},
        new Object[]{"c", null, 3}
    );

    WindowContext context = WindowContext.newInstance(Arrays.asList("col1"), Maps.<String, ValueDesc>newHashMap()).on(
        Arrays.<String>asList("col1"),
        Arrays.<OrderByColumnSpec>asList(OrderByColumnSpec.asc("col1"))
    );
    PivotSpec pivot = PivotSpec.of(PivotColumnSpec.toSpecs("col2"), "col3");
    WindowingSpec.PartitionEvaluator evaluator = pivot.create(context);
    Iterable<Row> results = Iterables.concat(
        evaluator.evaluate(new Object[]{"a"}, Arrays.asList(rows1.get(0))),
        evaluator.evaluate(new Object[]{"b"}, Arrays.asList(rows1.get(1))),
        evaluator.evaluate(new Object[]{"c"}, Arrays.asList(rows1.get(2)))
    );
    GroupByQueryRunnerTestHelper.validate(columns2, expected, results);
  }
}
