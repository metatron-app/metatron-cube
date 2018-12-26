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

package io.druid.data.input;

import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EvaluationTest
{
  @Test
  public void testOverride()
  {
    final DelimitedParseSpec parseSpec = new DelimitedParseSpec(
        new ExpressionTimestampSpec("time"),
        DimensionsSpec.ofStringDimensions(Arrays.<String>asList("dim0", "dim1")),
        ",",
        null,
        Arrays.asList("dim0", "dim1", "met1")
    );
    final List<String> dimensionNames = parseSpec.getDimensionsSpec().getDimensionNames();
    final AggregatorFactory[] aggregators = new AggregatorFactory[] {
        new CountAggregatorFactory("count"),
        new GenericSumAggregatorFactory("sum", "met1", ValueDesc.DOUBLE),
        new GenericMinAggregatorFactory("min", "met1", ValueDesc.DOUBLE),
        new GenericMaxAggregatorFactory("max", "met1", ValueDesc.DOUBLE)
    };
    InputRowParser<Object[]> parser = new InputRowParser<Object[]>()
    {
      @Override
      public InputRow parse(Object[] input)
      {
        int i = 1;
        Map<String, Object> event = Maps.newHashMap();
        for (String dimension : dimensionNames) {
          event.put(dimension, input[i++]);
        }
        event.put("met1", input[i]);
        return new MapBasedInputRow((DateTime) input[0], dimensionNames, event);
      }

      @Override
      public TimestampSpec getTimestampSpec()
      {
        return parseSpec.getTimestampSpec();
      }

      @Override
      public DimensionsSpec getDimensionsSpec()
      {
        return parseSpec.getDimensionsSpec();
      }

      @Override
      public InputRowParser withDimensionExclusions(Set<String> exclusions)
      {
        throw new UnsupportedOperationException();
      }
    };
    List<Evaluation> evaluations = Arrays.asList(
        new Evaluation("met1", "if (met1=='NULL', -100, met1)"),
        new Evaluation("met1", "if (met1=='NULL', -100, met1)")
    );
    InputRowParser<Object[]> wrapped = InputRowParsers.wrap(
        parser,
        aggregators,
        evaluations,
        Arrays.<Validation>asList(),
        false
    );
    InputRow row = wrapped.parse(new Object[]{new DateTime("2018-08-07"), "x", "y", "NULL"});
    Assert.assertEquals(-100L, row.getRaw("met1"));
  }
}
