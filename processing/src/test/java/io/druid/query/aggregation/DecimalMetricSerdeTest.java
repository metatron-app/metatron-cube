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

package io.druid.query.aggregation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.data.Rows;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 */
public class DecimalMetricSerdeTest
{
  static {
    if (ComplexMetrics.getSerdeFactory("decimal") == null) {
      ComplexMetrics.registerSerdeFactory("decimal", new DecimalMetricSerde.Factory());
    }
  }

  @Test
  public void test()
  {
    test("decimal(0, 3)", "1923.234", null, "2391.900", "-37248.000", "-3.141", "843923123123.999");
    test("decimal(0, 2, UP)", "1923.24", null, "2391.90", "-37248.00", "-3.15", "843923123124.00");
    test("decimal(6, 4, CEILING)", "1923.2400", null, "2391.9000", "-37248.0000", "-3.1415", "843924000000.0000");
    test("decimal(5, 2, CEILING)", "1923.30", null, "2391.90", "-37248.00", "-3.14", "843930000000.00");
    test("decimal(4, 2, CEILING)", "1924.00", null, "2392.00", "-37240.00", "-3.14", "843900000000.00");
  }

  @SuppressWarnings("unchecked")
  private void test(String type, String... expected)
  {
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type);
    GenericIndexed<BigDecimal> indexed = GenericIndexed.<BigDecimal>fromIterable(
        Iterables.transform(
            Arrays.<String>asList("1923.2341", null, "2391.9", "-37248", "-3.141592", "843923123123.99992"),
            new Function<String, BigDecimal>()
            {
              @Override
              public BigDecimal apply(String input)
              {
                return Rows.parseDecimal(input);
              }
            }
        ), serde.getObjectStrategy()
    );
    Assert.assertEquals(expected[0], indexed.get(0).toPlainString());
    Assert.assertNull(indexed.get(1));
    Assert.assertEquals(expected[2], indexed.get(2).toPlainString());
    Assert.assertEquals(expected[3], indexed.get(3).toPlainString());
    Assert.assertEquals(expected[4], indexed.get(4).toPlainString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegal1()
  {
    ComplexMetrics.getSerdeForType("decimal(0, -1)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegal2()
  {
    ComplexMetrics.getSerdeForType("decimal(0, 31)");
  }
}
