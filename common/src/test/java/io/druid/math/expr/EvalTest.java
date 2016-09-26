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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

  @Test
  public void testFormat()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", 1, "y", "ss"));
    Assert.assertEquals("001, ss", Parser.parse("format('%03d, %s', x, y)").eval(bindings).stringValue());
  }

  @Test
  public void testLPad()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", 7, "y", "2010"));
    Assert.assertEquals("007", Parser.parse("lpad(x, 3, '0')").eval(bindings).stringValue());
    Assert.assertEquals("2010", Parser.parse("lpad(y, 3, '0')").eval(bindings).stringValue());
  }

  @Test
  public void testRPad()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", 7, "y", "2010"));
    Assert.assertEquals("700", Parser.parse("rpad(x, 3, '0')").eval(bindings).stringValue());
    Assert.assertEquals("2010", Parser.parse("rpad(y, 3, '0')").eval(bindings).stringValue());
  }

  @Test
  public void testSplit()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "a:b:c"));
    Assert.assertEquals("c", Parser.parse("split(x, ':', 2)").eval(bindings).stringValue());
    Assert.assertEquals(null, Parser.parse("split(x, ':', 4)").eval(bindings).stringValue());
  }

  @Test
  public void testRight()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "abcde", "y", "abc"));
    Assert.assertEquals("bcde", Parser.parse("right(x, 4)").eval(bindings).stringValue());
    Assert.assertEquals("abc", Parser.parse("right(y, 4)").eval(bindings).stringValue());
  }

  @Test
  public void testMid()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "abcde", "y", "abc"));
    Assert.assertEquals("cd", Parser.parse("mid(x, 2, 4)").eval(bindings).stringValue());
    Assert.assertEquals("d", Parser.parse("mid(x, 3, 4)").eval(bindings).stringValue());
  }

  @Test
  public void testReplace()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "abcxbcdexbc", "y", "abc"));
    Assert.assertEquals("a!x!dex!", Parser.parse("replace(x, 'bc', '!')").eval(bindings).stringValue());
  }

  @Test
  public void testIn()
  {
    Set<String> strings = Sets.newHashSet("a", "c", "f");
    Map<String, Object> mapping = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String value = String.valueOf((char) ('a' + i));
      mapping.put("x", value);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      boolean eval = Parser.parse("in(x, 'a', 'c', 'f')").eval(bindings).asBoolean();
      Assert.assertEquals(strings.contains(value), eval);
    }
    Set<Long> longs = Sets.newHashSet(1L, 3L, 5L);
    for (int i = 0; i < 5; i++) {
      long value = i;
      mapping.put("x", value);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      boolean eval = Parser.parse("in(x, 1, 3, 5)").eval(bindings).asBoolean();
      Assert.assertEquals(longs.contains(value), eval);
    }
    Set<Double> doubles = Sets.newHashSet(1D, 3D, 5D);
    for (int i = 0; i < 5; i++) {
      double value = i;
      mapping.put("x", value);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      boolean eval = Parser.parse("in(x, 1.0, 3.0, 5.0)").eval(bindings).asBoolean();
      Assert.assertEquals(doubles.contains(value), eval);
    }
  }

  @Test
  public void testJavaScript()
  {
    Map<String, Object> mapping = new HashMap<>();
    for (int i = 0; i < 30; i++) {
      mapping.put("x", i);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      String eval = Parser.parse(
          "javascript('x', 'if (x < 10) return \"X\"; else if (x < 20) return \"Y\"; else return \"Z\";')").eval(bindings).stringValue();
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
  public void testLike()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of());
    Assert.assertTrue(Parser.parse("like ('navis', '%s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', 'n%v_%')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', '%v__')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', '%vi%')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', '__vi_')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', 'n%s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("like ('navis', '_a%i_')").eval(bindings).asBoolean());

    Assert.assertFalse(Parser.parse("like ('nabis', 'n%v_')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("like ('nabis', '%v__%')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("like ('nabis', '%vi%')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("like ('nabis', '__vi_')").eval(bindings).asBoolean());
  }

  @Test
  public void testRegex()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of());
    Assert.assertTrue(Parser.parse("regex ('navis', '.*s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', 'n.*v..*')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', '.*v..')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', '.*vi.*')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', '..vi.')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', 'n.*s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex ('navis', '.a.*i.')").eval(bindings).asBoolean());

    Assert.assertFalse(Parser.parse("regex ('nabis', 'n.*v.*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex ('nabis', '.*v...*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex ('nabis', '.*vi.*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex ('nabis', '..vi.')").eval(bindings).asBoolean());

    Assert.assertEquals("navi", Parser.parse("regex ('navis', '(.*)s', 1)").eval(bindings).asString());
    Assert.assertEquals("is", Parser.parse("regex ('navis', '(.*)v(..)', 2)").eval(bindings).asString());
    Assert.assertEquals("navis", Parser.parse("regex ('navis', '.*vi.*', 0)").eval(bindings).asString());
  }
}
