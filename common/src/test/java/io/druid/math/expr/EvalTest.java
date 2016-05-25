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

package io.druid.math.expr;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class EvalTest
{
  private long evalLong(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = Parser.parse(x).eval(bindings);
    Assert.assertEquals(ExprType.LONG, ret.type());
    return ret.longValue();
  }

  private double evalDouble(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = Parser.parse(x).eval(bindings);
    Assert.assertEquals(ExprType.DOUBLE, ret.type());
    return ret.doubleValue();
  }

  @Test
  public void testDoubleEval()
  {
    Map<String, Number> mapping = new HashMap<>();
    mapping.put("x", 2.0d);

    Expr.NumericBinding bindings = Parser.withMap(mapping);

    Assert.assertEquals(2.0, evalDouble("x", bindings), 0.0001);
    Assert.assertEquals(2.0, evalDouble("\"x\"", bindings), 0.0001);
    Assert.assertEquals(304.0, evalDouble("300 + \"x\" * 2", bindings), 0.0001);

    Assert.assertFalse(evalDouble("1.0 && 0.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("1.0 && 2.0", bindings) > 0.0);

    Assert.assertTrue(evalDouble("1.0 || 0.0", bindings) > 0.0);
    Assert.assertFalse(evalDouble("0.0 || 0.0", bindings) > 0.0);

    Assert.assertTrue(evalDouble("2.0 > 1.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 >= 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("1.0 < 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 <= 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 == 2.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("2.0 != 1.0", bindings) > 0.0);

    Assert.assertEquals(3.5, evalDouble("2.0 + 1.5", bindings), 0.0001);
    Assert.assertEquals(0.5, evalDouble("2.0 - 1.5", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("2.0 * 1.5", bindings), 0.0001);
    Assert.assertEquals(4.0, evalDouble("2.0 / 0.5", bindings), 0.0001);
    Assert.assertEquals(0.2, evalDouble("2.0 % 0.3", bindings), 0.0001);
    Assert.assertEquals(8.0, evalDouble("2.0 ^ 3.0", bindings), 0.0001);
    Assert.assertEquals(-1.5, evalDouble("-1.5", bindings), 0.0001);

    Assert.assertTrue(evalDouble("!-1.0", bindings) > 0.0);
    Assert.assertTrue(evalDouble("!0.0", bindings) > 0.0);
    Assert.assertFalse(evalDouble("!2.0", bindings) > 0.0);

    Assert.assertEquals(2.0, evalDouble("sqrt(4.0)", bindings), 0.0001);
    Assert.assertEquals(2.0, evalDouble("if(1.0, 2.0, 3.0)", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("if(0.0, 2.0, 3.0)", bindings), 0.0001);


    // exists
    Assert.assertEquals(3.0, evalDouble("case (x - 1, 0.0, 2.0, 1.0, 3.0)", bindings), 0.0001);

    // not-exists (implicit 0)
    Assert.assertEquals(0.0, evalDouble("case (x + 10, 0.0, 2.0, 2.0, 3.0)", bindings), 0.0001);
    // not-exists (explicit)
    Assert.assertEquals(100.0, evalDouble("case (x + 10, 0.0, 2.0, 2.0, 3.0, 100.0)", bindings), 0.0001);
  }

  @Test
  public void testEvalIf()
  {
    Map<String, Number> mapping = new HashMap<>();
    for (int i = 0; i < 30; i++) {
      mapping.put("x", i);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      String eval = Parser.parse("if(x < 10, 'X', x < 20, 'Y', 'Z')").eval(bindings).stringValue();
      if (i < 10) {
        Assert.assertEquals("X", eval);
      } else if (i < 20) {
        Assert.assertEquals("Y", eval);
      } else {
        Assert.assertEquals("Z", eval);
      }
    }
  }

  @Test
  public void testLongEval()
  {
    Map<String, Number> mapping = new HashMap<>();
    mapping.put("x", 9223372036854775807L);

    Expr.NumericBinding bindings = Parser.withMap(mapping);

    Assert.assertEquals(9223372036854775807L, evalLong("x", bindings));
    Assert.assertEquals(9223372036854775807L, evalLong("\"x\"", bindings));
    Assert.assertEquals(92233720368547759L, evalLong("\"x\" / 100 + 1", bindings));

    Assert.assertFalse(evalLong("9223372036854775807 && 0", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 && 9223372036854775806", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 || 0", bindings) > 0);
    Assert.assertFalse(evalLong("-9223372036854775807 || -9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("-9223372036854775807 || 9223372036854775807", bindings) > 0);
    Assert.assertFalse(evalLong("0 || 0", bindings) > 0);

    Assert.assertTrue(evalLong("9223372036854775807 > 9223372036854775806", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 >= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775806 < 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 <= 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 == 9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("9223372036854775807 != 9223372036854775806", bindings) > 0);

    Assert.assertEquals(9223372036854775807L, evalLong("9223372036854775806 + 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("9223372036854775807 - 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("4611686018427387903 * 2", bindings));
    Assert.assertEquals(4611686018427387903L, evalLong("9223372036854775806 / 2", bindings));
    Assert.assertEquals(7L, evalLong("9223372036854775807 % 9223372036854775800", bindings));
    Assert.assertEquals(9223372030926249001L, evalLong("3037000499 ^ 2", bindings));
    Assert.assertEquals(-9223372036854775807L, evalLong("-9223372036854775807", bindings));

    Assert.assertTrue(evalLong("!-9223372036854775807", bindings) > 0);
    Assert.assertTrue(evalLong("!0", bindings) > 0);
    Assert.assertFalse(evalLong("!9223372036854775807", bindings) > 0);

    Assert.assertEquals(3037000499L, evalLong("cast(sqrt(9223372036854775807), 'long')", bindings));
    Assert.assertEquals(
        9223372036854775807L, evalLong("if(9223372036854775807, 9223372036854775807, 9223372036854775806)", bindings)
    );
    Assert.assertEquals(
        9223372036854775806L, evalLong("if(0, 9223372036854775807, 9223372036854775806)", bindings)
    );

    Assert.assertEquals(1271023381000L, evalLong("timestamp('2010-04-12T07:03:01+09:00')", bindings));


    // exists
    Assert.assertEquals(3L, evalLong("case (x - 1, 9223372036854775807, 2, 9223372036854775806, 3)", bindings));

    // not-exists (implicit 0)
    Assert.assertEquals(0L, evalLong("case (x + 10, 0, 2, 1, 3)", bindings));
    // not-exists (explicit)
    Assert.assertEquals(100L, evalLong("case (x + 10, 0, 2, 1, 3, 100)", bindings));
  }
}
