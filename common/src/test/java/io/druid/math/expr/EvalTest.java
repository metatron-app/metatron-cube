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

package io.druid.math.expr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularity;
import io.druid.granularity.GranularityType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class EvalTest
{
  private ExprEval _eval(String x, Expr.NumericBinding bindings)
  {
    return Parser.parse(x).eval(bindings);
  }

  private long evalLong(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.LONG, ret.type());
    return ret.longValue();
  }

  private Boolean evalBoolean(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.BOOLEAN, ret.type());
    return ret.isNull() ? null : ret.asBoolean();
  }

  private double evalDouble(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.DOUBLE, ret.type());
    return ret.doubleValue();
  }

  private String evalString(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.STRING, ret.type());
    return ret.stringValue();
  }

  private DateTime evalDateTime(String x, Expr.NumericBinding bindings)
  {
    ExprEval ret = _eval(x, bindings);
    Assert.assertEquals(ValueDesc.DATETIME, ret.type());
    return ret.asDateTime();
  }

  private Object eval(String x, Expr.NumericBinding bindings)
  {
    return _eval(x, bindings).value();
  }

  @Test
  public void testBoolean()
  {
    ExprEval eval = Parser.parse("true").eval(null);
    Assert.assertEquals(ValueDesc.BOOLEAN, eval.type());
    Assert.assertTrue((Boolean) eval.value());

    eval = Parser.parse("TRUE").eval(null);
    Assert.assertEquals(ValueDesc.BOOLEAN, eval.type());
    Assert.assertTrue((Boolean) eval.value());

    eval = Parser.parse("false").eval(null);
    Assert.assertEquals(ValueDesc.BOOLEAN, eval.type());
    Assert.assertFalse((Boolean) eval.value());

    eval = Parser.parse("FALSE").eval(null);
    Assert.assertEquals(ValueDesc.BOOLEAN, eval.type());
    Assert.assertFalse((Boolean) eval.value());
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

    Assert.assertFalse(evalBoolean("1.0 && 0.0", bindings));
    Assert.assertTrue(evalBoolean("1.0 && 2.0", bindings));

    Assert.assertTrue(evalBoolean("1.0 || 0.0", bindings));
    Assert.assertFalse(evalBoolean("0.0 || 0.0", bindings));

    Assert.assertTrue(evalBoolean("2.0 > 1.0", bindings));
    Assert.assertTrue(evalBoolean("2.0 >= 2.0", bindings));
    Assert.assertTrue(evalBoolean("1.0 < 2.0", bindings));
    Assert.assertTrue(evalBoolean("2.0 <= 2.0", bindings));
    Assert.assertTrue(evalBoolean("2.0 == 2.0", bindings));
    Assert.assertTrue(evalBoolean("2.0 != 1.0", bindings));

    Assert.assertEquals(3.5, evalDouble("2.0 + 1.5", bindings), 0.0001);
    Assert.assertEquals(0.5, evalDouble("2.0 - 1.5", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("2.0 * 1.5", bindings), 0.0001);
    Assert.assertEquals(4.0, evalDouble("2.0 / 0.5", bindings), 0.0001);
    Assert.assertEquals(0.2, evalDouble("2.0 % 0.3", bindings), 0.0001);
    Assert.assertEquals(8.0, evalDouble("2.0 ^ 3.0", bindings), 0.0001);
    Assert.assertEquals(-1.5, evalDouble("-1.5", bindings), 0.0001);

    Assert.assertTrue(evalBoolean("!-1.0", bindings));
    Assert.assertTrue(evalBoolean("!0.0", bindings));
    Assert.assertFalse(evalBoolean("!2.0", bindings));

    Assert.assertEquals(2.0, evalDouble("sqrt(4.0)", bindings), 0.0001);
    Assert.assertEquals(2.0, evalDouble("if(1.0, 2.0, 3.0)", bindings), 0.0001);
    Assert.assertEquals(3.0, evalDouble("if(0.0, 2.0, 3.0)", bindings), 0.0001);


    // exists
    Assert.assertEquals(3.0, evalDouble("switch (x - 1, 0.0, 2.0, 1.0, 3.0)", bindings), 0.0001);
    // not-exists (implicit 0)
    Assert.assertEquals(0.0, evalDouble("switch (x + 10, 0.0, 2.0, 2.0, 3.0)", bindings), 0.0001);
    // not-exists (explicit)
    Assert.assertEquals(100.0, evalDouble("switch (x + 10, 0.0, 2.0, 2.0, 3.0, 100.0)", bindings), 0.0001);

    // exists
    Assert.assertEquals(3.0, evalDouble("case (x - 1 == 0.0, 2.0, x - 1 == 1.0, 3.0)", bindings), 0.0001);
    // not-exists (implicit 0)
    Assert.assertEquals(0.0, evalDouble("case (x + 10 == 0.0, 2.0, x + 10 == 2.0, 3.0)", bindings), 0.0001);
    // not-exists (explicit)
    Assert.assertEquals(100.0, evalDouble("case (x + 10 == 0.0, 2.0, x + 10 == 2.0, 3.0, 100.0)", bindings), 0.0001);
  }

  @Test
  public void testNullAsBoolean()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of());
    Assert.assertFalse(Parser.parse("X").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("!X").eval(bindings).asBoolean());
  }

  @Test
  public void testIf()
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
  public void testDummyEq()
  {
    Assert.assertTrue(_eval("x == x", null).asBoolean());
    Assert.assertTrue(_eval("x >= x", null).asBoolean());
    Assert.assertTrue(_eval("x <= x", null).asBoolean());

    Assert.assertFalse(_eval("x != x", null).asBoolean());
    Assert.assertFalse(_eval("x > x", null).asBoolean());
    Assert.assertFalse(_eval("x < x", null).asBoolean());
  }

  @Test
  public void testCase()
  {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("setl_rem_amt", "1646446000-");
    Expr.NumericBinding bindings = Parser.withMap(mapping);

    String ex1 = "case(endsWith(setl_rem_amt,'-'), concat('-',left(setl_rem_amt, length(setl_rem_amt) - 1)))";
    String ex2 = "case(endsWith(setl_rem_amt,'-'), concat('-',left(setl_rem_amt, length(setl_rem_amt) - 1)), setl_rem_amt)";
    Assert.assertEquals("-1646446000", evalString(ex1, bindings));
    Assert.assertEquals("-1646446000", evalString(ex2, bindings));

    mapping.put("setl_rem_amt", "1646446000");
    Assert.assertNull(evalString(ex1, bindings));
    Assert.assertEquals("1646446000", evalString(ex2, bindings));
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

    Assert.assertFalse(evalBoolean("9223372036854775807 && 0", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775807 && 9223372036854775806", bindings));

    Assert.assertTrue(evalBoolean("9223372036854775807 || 0", bindings));
    Assert.assertFalse(evalBoolean("-9223372036854775807 || -9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("-9223372036854775807 || 9223372036854775807", bindings));
    Assert.assertFalse(evalBoolean("0 || 0", bindings));

    Assert.assertTrue(evalBoolean("9223372036854775807 > 9223372036854775806", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775807 >= 9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775806 < 9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775807 <= 9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775807 == 9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("9223372036854775807 != 9223372036854775806", bindings));

    Assert.assertEquals(9223372036854775807L, evalLong("9223372036854775806 + 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("9223372036854775807 - 1", bindings));
    Assert.assertEquals(9223372036854775806L, evalLong("4611686018427387903 * 2", bindings));
    Assert.assertEquals(4611686018427387903L, evalLong("9223372036854775806 / 2", bindings));
    Assert.assertEquals(7L, evalLong("9223372036854775807 % 9223372036854775800", bindings));
    Assert.assertEquals(9223372030926249001L, evalLong("3037000499 ^ 2", bindings));
    Assert.assertEquals(-9223372036854775807L, evalLong("-9223372036854775807", bindings));

    Assert.assertTrue(evalBoolean("!-9223372036854775807", bindings));
    Assert.assertTrue(evalBoolean("!0", bindings));
    Assert.assertFalse(evalBoolean("!9223372036854775807", bindings));

    Assert.assertEquals(3037000499L, evalLong("cast(sqrt(9223372036854775807), 'long')", bindings));
    Assert.assertEquals(
        9223372036854775807L, evalLong("if(9223372036854775807, 9223372036854775807, 9223372036854775806)", bindings)
    );
    Assert.assertEquals(
        9223372036854775806L, evalLong("if(0, 9223372036854775807, 9223372036854775806)", bindings)
    );

    Assert.assertEquals(1294704000000L, evalLong("timestamp('2011-01-11T00:00:00.000Z')", bindings));
    Assert.assertEquals(1271023381000L, evalLong("timestamp('2010-04-12T07:03:01+09:00')", bindings));
    Assert.assertEquals(
        new DateTime("2010-04-12T07:03:01+09:00"),
        evalDateTime("dateTime('2010-04-12T07:03:01+09:00')", bindings)
    );

    // exists
    Assert.assertEquals(3L, evalLong("switch (x - 1, 9223372036854775807, 2, 9223372036854775806, 3)", bindings));
    // not-exists (implicit 0)
    Assert.assertEquals(0L, evalLong("switch (x + 10, 0, 2, 1, 3)", bindings));
    // not-exists (explicit)
    Assert.assertEquals(100L, evalLong("switch (x + 10, 0, 2, 1, 3, 100)", bindings));

    // exists
    Assert.assertEquals(
        3L,
        evalLong("case (x - 1 == 9223372036854775807, 2, x - 1 == 9223372036854775806, 3)", bindings)
    );
    // not-exists (implicit 0)
    Assert.assertEquals(0L, evalLong("case (x + 10 == 0, 2, x + 10 == 1, 3)", bindings));
    // not-exists (explicit)
    Assert.assertEquals(100L, evalLong("case (x + 10 == 0, 2, x + 10 == 1, 3, 100)", bindings));

    Interval eval = (Interval) eval("recent('1D 10s')", bindings);
    long now = System.currentTimeMillis();
    Assert.assertEquals(now - 86410000L, eval.getStartMillis(), 1000);
    Assert.assertEquals(now, eval.getEndMillis(), 1000);

    eval = (Interval) eval("recent('7D 10s', '5D 1s')", bindings);
    now = System.currentTimeMillis();
    Assert.assertEquals(now - (86400000L * 7) - 10000, eval.getStartMillis(), 1000);
    Assert.assertEquals(now - (86400000L * 5) - 1000, eval.getEndMillis(), 1000);

    // format (string to string)
    Assert.assertEquals(
        "11-16-2016 PM 05:11:39.662-0800", evalString(
            "time_format("
            + "'2016-11-17 오전 10:11:39.662+09:00', "
            + "format='yyyy-MM-dd a hh:mm:ss.SSSZ', "
            + "locale='ko', "
            + "out.format='MM-dd-yyyy a hh:mm:ss.SSSZ', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );
    Assert.assertEquals(
        "11-16-2016 PM 05:11:39.662-0800", evalString(
            "time_format("
            + "'2016-11-17 오전 10:11:39.662+0900', "
            + "format='yyyy-MM-dd a hh:mm:ss.SSSZZ', "
            + "locale='ko', "
            + "out.format='MM-dd-yyyy a hh:mm:ss.SSSZ', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );
    // invalid
    Assert.assertNull(
        evalString(
            "time_format("
            + "'2016-11-17 AM 10:11:39.662+0900', "
            + "format='yyyy-MM-dd a hh:mm:ss.SSSZZ', "
            + "locale='ko', "
            + "out.format='MM-dd-yyyy a hh:mm:ss.SSSZ', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );
    Assert.assertNull(
        evalString(
            "time_format("
            + "'2016-11-31 오전 10:11:39.662+0900', "
            + "format='yyyy-MM-dd a hh:mm:ss.SSSZZ', "
            + "locale='ko', "
            + "out.format='MM-dd-yyyy a hh:mm:ss.SSSZ', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );

    long time = new DateTime("2016-11-17T10:11:39.662+0900").getMillis();
    // format (long to string)
    Assert.assertEquals(
        "11-16-2016 PM 05:11:39.662-0800", evalString(
            "time_format("
            + time + ", "
            + "out.format='MM-dd-yyyy a hh:mm:ss.SSSZ', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );
    // with escape
    Assert.assertEquals(
        "2016-11-16T17:11:39.662Z", evalString(
            "time_format("
            + time + ", "
            + "out.format='yyyy-MM-dd\\'T\\'HH:mm:ss.SSS\\'Z\\'', "
            + "out.locale='us', "
            + "out.timezone='PST'"
            + ")", bindings
        )
    );
    // quarter
    Assert.assertEquals(
        "Q4 2016", evalString(
            "time_format("
            + time + ", "
            + "out.format='qqq yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
    // escape
    Assert.assertEquals(
        "X Q4 2016", evalString(
            "time_format("
            + time + ", "
            + "out.format='\\'X\\' qqq yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
    // week in month
    Assert.assertEquals(
        "3, 11 2016", evalString(
            "time_format("
            + time + ", "
            + "out.format='W, MM yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
  }

  @Test
  public void testBasicDecimal()
  {
    final BigDecimal x = BigDecimal.valueOf(100);
    final BigDecimal y = BigDecimal.valueOf(200.1234);
    final double p = 1234.5678;
    final long q = 12345678;
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", x, "y", y, "p", p, "q", q));
    Assert.assertEquals(x.add(y), eval("x + y", bindings));
    Assert.assertEquals(x.subtract(y), eval("x - y", bindings));
    Assert.assertEquals(x.multiply(y), eval("x * y", bindings));
    Assert.assertEquals(x.divide(y, MathContext.DECIMAL64), eval("x / y", bindings));
    Assert.assertEquals(x.add(BigDecimal.valueOf(p)), eval("x + p", bindings));
    Assert.assertEquals(BigDecimal.valueOf(q).add(y), eval("q + y", bindings));

    // currently, round is the sole function supports decimal type
    Assert.assertEquals(BigDecimal.valueOf(200), eval("round(y)", bindings));
    Assert.assertEquals(BigDecimal.valueOf(200.12), eval("round(y, 2)", bindings));
  }

  @Test
  public void testWeekInMonth()
  {
    HashMap<String, Object> mapping = Maps.<String, Object>newHashMap();
    Expr.NumericBinding bindings = Parser.withMap(mapping);

    // null
    mapping.put("time", null);
    Assert.assertNull(
        evalString("time_format(time, out.format='W\\'th\\' MMM yyyy', out.locale='en', out.timezone='UTC')", bindings)
    );

    // thursday
    mapping.put("time", new DateTime("2016-07-01T10:11:39.662+0900").getMillis());
    Assert.assertEquals(
        "0th Jul 2016", evalString(
            "time_format("
            + "time, "
            + "out.format='W\\'th\\' MMM yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
    // wednesday
    mapping.put("time", new DateTime("2016-09-01T10:11:39.662+0900").getMillis());
    Assert.assertEquals(
        "1th Sep 2016", evalString(
            "time_format("
            + "time, "
            + "out.format='W\\'th\\' MMM yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
    mapping.put("time", new DateTime("2016-01-31T10:11:39.662+0900").getMillis());
    Assert.assertEquals(
        "4th Jan 2016", evalString(
            "time_format("
            + "time, "
            + "out.format='W\\'th\\' MMM yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
    mapping.put("time", new DateTime("2016-05-31T10:11:39.662+0900").getMillis());
    Assert.assertEquals(
        "5th May 2016", evalString(
            "time_format("
            + "time, "
            + "out.format='W\\'th\\' MMM yyyy', "
            + "out.locale='en', "
            + "out.timezone='UTC'"
            + ")", bindings
        )
    );
  }

  @Test
  public void testStandard() throws ParseException
  {
    testStandard("2018-03-05T08:09:24.432+0100", "2018-03-05T07:09:24.432+0000");
    testStandard("2018-03-05 08:09:24.432+0100", "2018-03-05T07:09:24.432+0000");

    testStandard("2018-03-05T08:09:24.432", "2018-03-05T08:09:24.432+0000");
    testStandard("2018-03-05 08:09:24.432", "2018-03-05T08:09:24.432+0000");

    testStandard("2018-03-05T08:09:24", "2018-03-05T08:09:24.000+0000");
    testStandard("2018-03-05 08:09:24", "2018-03-05T08:09:24.000+0000");

    testStandard("2018-03-05", "2018-03-05T00:00:00.000+0000");
  }

  private void testStandard(String time, String expected)
  {
    DateTime dateTime = JodaUtils.STANDARD_PARSER.parseDateTime(time);
    Assert.assertEquals(expected, JodaUtils.STANDARD_PRINTER.print(dateTime.getMillis()));
  }

  @Test
  public void testQuarter() throws ParseException
  {
    testQuarter("2018-03-05T08:09:24.432+0000", "yyyy-qq'('MMM')T'HH:mm:ss", "ko", "2018-01(3월)T08:09:24");
    testQuarter("2018-03-05T08:09:24.432+0000", "yyyy-qqq'('MMMM')T'HH:mm:ss", "ja", "2018-Q1(3月)T08:09:24");
    testQuarter("2018-03-05T08:09:24.432+0000", "yyyy-qqqq'('MMMM')T'HH:mm:ss", "de", "2018-Q01(März)T08:09:24");
  }

  private void testQuarter(String time, String format, String locale, String expected)
  {
    DateTime dateTime = JodaUtils.STANDARD_PARSER.parseDateTime(time);
    Assert.assertEquals(expected, JodaUtils.toTimeFormatter(format, null, locale).print(dateTime.getMillis()));
  }

  @Test
  public void testOptional() throws ParseException
  {
    testOptional("2018-03-05T[]08:09:24.432+0000", "yyyy-MM-dd['T[]'HH:mm:ss.SSSZZ]", "2018-03-05T08:09:24.432+0000");
    testOptional("2018-03-05", "yyyy-MM-dd['T[]'HH:mm:ss.SSSZZ]", "2018-03-05T00:00:00.000+0000");
    testOptional("2018-08-06 01:00:34.23015", "yyyy-MM-dd HH:mm:ss[.SSSSS]", "2018-08-06T01:00:34.230+0000");
  }

  private void testOptional(String time, String format, String expected)
  {
    DateTime dateTime = JodaUtils.toTimeFormatter(format).parseDateTime(time);
    Assert.assertEquals(expected, JodaUtils.STANDARD_PRINTER.print(dateTime));
  }

  @Test
  public void testDatetimeFunctions()
  {
    DateTimeZone home = DateTimeZone.forID("Asia/Seoul");
    DateTime time = new DateTime("2016-03-04T22:25:00", home);

    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", time));
    Assert.assertEquals(4, evalLong("dayofmonth(x)", bindings));
    Assert.assertEquals(31, evalLong("lastdayofmonth(x)", bindings));
    Assert.assertEquals(64, evalLong("dayofyear(x)", bindings));
    Assert.assertEquals(22, evalLong("hour(x)", bindings));
    Assert.assertEquals(3, evalLong("month(x)", bindings));
    Assert.assertEquals(2016, evalLong("year(x)", bindings));
    Assert.assertEquals("March", evalString("monthname(x)", bindings));
    Assert.assertEquals("Friday", evalString("dayname(x)", bindings));
    Assert.assertEquals("3월", evalString("monthname(x, ,'ko')", bindings));
    Assert.assertEquals("금요일", evalString("dayname(x, ,'ko')", bindings));
    Assert.assertEquals(31, evalLong("last_day(x)", bindings));
    Assert.assertEquals(new DateTime("2016-03-08T01:25:00", home), evalDateTime("add_time(x, '3D 3H')", bindings));
    Assert.assertEquals(new DateTime("2016-03-03T19:22:00", home), evalDateTime("sub_time(x, '1D 3H 3m')", bindings));
    Assert.assertEquals(new DateTime("2016-03-04T22:24:59", home), evalDateTime("x - 1000", bindings));
    Assert.assertEquals(new DateTime("2016-03-04T22:25:01", home), evalDateTime("x + 1000", bindings));

    // utc
    Assert.assertEquals(4, evalLong("dayofmonth(x, 'UTC')", bindings));
    Assert.assertEquals(31, evalLong("lastdayofmonth(x, 'UTC')", bindings));
    Assert.assertEquals(64, evalLong("dayofyear(x, 'UTC')", bindings));
    Assert.assertEquals(13, evalLong("hour(x, 'UTC')", bindings));
    Assert.assertEquals(3, evalLong("month(x, 'UTC')", bindings));
    Assert.assertEquals(2016, evalLong("year(x, 'UTC')", bindings));
    Assert.assertEquals("March", evalString("monthname(x, 'UTC')", bindings));
    Assert.assertEquals("Friday", evalString("dayname(x, 'UTC', )", bindings));
    Assert.assertEquals("3月", evalString("monthname(x, 'UTC','ja')", bindings));
    Assert.assertEquals("金曜日", evalString("dayname(x, 'UTC','ja')", bindings));
    Assert.assertEquals(31, evalLong("last_day(x, 'UTC')", bindings));
    Assert.assertEquals(new DateTime("2016-03-07T16:25:00Z"), evalDateTime("add_time(x, '3D 3H', 'UTC')", bindings));
    Assert.assertEquals(new DateTime("2016-03-03T10:22:00Z"), evalDateTime("sub_time(x, '1D 3H 3m', 'UTC')", bindings));

    // invalids
    bindings = Parser.withMap(ImmutableMap.of("x", "xxxx"));
    Assert.assertEquals(-1, evalLong("dayofmonth(x)", bindings));
    Assert.assertEquals(-1, evalLong("lastdayofmonth(x)", bindings));
    Assert.assertEquals(-1, evalLong("dayofyear(x)", bindings));
    Assert.assertEquals(-1, evalLong("hour(x)", bindings));
    Assert.assertEquals(-1, evalLong("month(x)", bindings));
    Assert.assertEquals(-1, evalLong("year(x)", bindings));
    Assert.assertNull(evalString("monthname(x)", bindings));
    Assert.assertNull(evalString("dayname(x)", bindings));
    Assert.assertNull(evalString("monthname(x, ,'ko')", bindings));
    Assert.assertNull(evalString("dayname(x, ,'ko')", bindings));
    Assert.assertEquals(-1, evalLong("last_day(x)", bindings));
    Assert.assertNull(evalDateTime("add_time(x, '3D 3H')", bindings));
    Assert.assertNull(evalDateTime("sub_time(x, '1D 3H 3m')", bindings));
  }

  @Test
  public void testTimeDiff()
  {
    DateTime time = new DateTime("2016-03-04T22:25:00");
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", time));

    Assert.assertEquals(0, evalLong("datediff(x, timestamp('2016-03-05T22:20', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings));
    Assert.assertEquals(1, evalLong("datediff(x, timestamp('2016-03-05T22:30', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings));

    Assert.assertEquals(0, evalLong("years_between(x, timestamp('2017-03-04T22:20', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings));
    Assert.assertEquals(1, evalLong("years_between(x, timestamp('2017-03-04T22:30', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings));

    // invalid
    Assert.assertNull( _eval("years_between(x, timestamp('2017-03-04 22:20', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings).value());
    Assert.assertNull( _eval("years_between(y, timestamp('2017-03-04T22:20', 'yyyy-MM-dd\\'T\\'HH:mm'))", bindings).value());
  }

  @Test
  public void testComplexTimeFunctions()
  {
    DateTimeZone home = DateTimeZone.forID("Asia/Seoul");
    DateTime time = new DateTime("2016-03-04T22:25:00", home);
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", time));

    Assert.assertEquals(
        1479377499662L,
        evalLong("timestamp('2016-11-17 10:11:39.662', 'yyyy-MM-dd HH:mm:ss.SSS')", bindings)
    );
    Assert.assertEquals(
        1479377499662L,
        evalLong("timestamp('2016-11-17 10:11:39.662', format='yyyy-MM-dd HH:mm:ss.SSS')", bindings)
    );

    Assert.assertEquals(
        1479406299662L,
        evalLong("timestamp('2016-11-17 10:11:39.662', 'yyyy-MM-dd HH:mm:ss.SSS', 'America/Los_Angeles')", bindings)
    );
    Assert.assertEquals(
        1479406299662L,
        evalLong(
            "timestamp('2016-11-17 10:11:39.662', format='yyyy-MM-dd HH:mm:ss.SSS', timezone='America/Los_Angeles')",
            bindings
        )
    );

    Assert.assertEquals("257", evalString(
        "nvl(cast(datediff(x, timestamp('2016-11-17 10:11:39.662', 'yyyy-MM-dd HH:mm:ss.SSS')), 'STRING'), 'N/A')", bindings
    ));
    Assert.assertEquals("N/A", evalString(
        "nvl(cast(datediff(x, timestamp('2016-11-17T10:11:39', 'yyyy-MM-dd HH:mm:ss.SSS')), 'STRING'), 'N/A')", bindings
    ));

    Assert.assertEquals(4, evalLong("2020 - cast(left('20161117',4), 'LONG')", bindings));

    Assert.assertEquals(new DateTime(0L), evalDateTime("datetime(0)", bindings));
    Assert.assertEquals(new DateTime(-1L), evalDateTime("datetime(-1)", bindings));

    DateTimeZone LA = DateTimeZone.forID("America/Los_Angeles");
    Assert.assertEquals(
        new DateTime("2016-11-17T10:11:39.662", LA),
        evalDateTime(
            "datetime('2016-11-17 10:11:39.662', 'yyyy-MM-dd HH:mm:ss.SSS', 'America/Los_Angeles')",
            bindings
        )
    );
    Assert.assertEquals(
        new DateTime("2016-11-17T10:11:39.662", LA),
        evalDateTime(
            "datetime('2016-11-17 10:11:39.662', 'yyyy-MM-dd HH:mm:ss.SSS', timezone='America/Los_Angeles')",
            bindings
        )
    );
    Assert.assertEquals(
        new DateTime("2016-11-17T10:11:39.662", LA),
        evalDateTime(
            "datetime('2016-11-17 10:11:39.662', format='yyyy-MM-dd HH:mm:ss.SSS', timezone='America/Los_Angeles')",
            bindings
        )
    );
    DateTimeZone shanghai = DateTimeZone.forID("Asia/Shanghai");
    Assert.assertEquals(
        new DateTime("2016-11-18T02:11:39.662", shanghai),
        evalDateTime(
            "datetime('2016-11-17 10:11:39.662', format='yyyy-MM-dd HH:mm:ss.SSS', " +
            "timezone='America/Los_Angeles', out.timezone='Asia/Shanghai')",
            bindings
        )
    );
  }

  @Test
  public void testExtractTimestamp()
  {
    DateTime current = DateTimes.nowUtc();
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", current.getMillis()));
    Assert.assertEquals(current, evalDateTime("datetime(x, format='SSSSSS')", bindings));
  }

  @Test
  public void testDiffTimes()
  {
    DateTimeZone home = DateTimeZone.forID("Asia/Seoul");
    DateTime time1 = new DateTime("2016-03-04T22:25:00", home);
    DateTime time2 = new DateTime("2016-03-04T23:35:00", home);
    DateTime time3 = new DateTime("2016-03-05T01:15:00", home);
    DateTime time4 = new DateTime("2016-03-12T18:47:00", home);
    DateTime time5 = new DateTime("2016-04-01T00:12:00", home);
    DateTime time6 = new DateTime("2016-07-14T03:15:00", home);
    DateTime time7 = new DateTime("2017-02-09T02:53:00", home);
    DateTime time8 = new DateTime("2032-09-09T22:11:00", home);

    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.<String, Object>builder()
            .put("t1", time1.getMillis())
            .put("t2", time2.getMillis())
            .put("t3", time3.getMillis())
            .put("t4", time4.getMillis())
            .put("t5", time5.getMillis())
            .put("t6", time6.getMillis())
            .put("t7", time7.getMillis())
            .put("t8", time8.getMillis()).build()
    );

    Assert.assertEquals(70, evalLong("difftime('MINUTE', t1, t2)", bindings));
    Assert.assertEquals(170, evalLong("difftime('MINUTE', t1, t3)", bindings));

    Assert.assertEquals(1, evalLong("difftime('HOUR', t1, t2)", bindings));
    Assert.assertEquals(2, evalLong("difftime('HOUR', t1, t3)", bindings));

    Assert.assertEquals(0, evalLong("difftime('DAY', t1, t3)", bindings));
    Assert.assertEquals(7, evalLong("difftime('DAY', t1, t4)", bindings));
    Assert.assertEquals(27, evalLong("difftime('DAY', t1, t5)", bindings));

    Assert.assertEquals(0, evalLong("difftime('WEEK', t1, t3)", bindings));
    Assert.assertEquals(1, evalLong("difftime('WEEK', t1, t4)", bindings));
    Assert.assertEquals(3, evalLong("difftime('WEEK', t1, t5)", bindings));

    Assert.assertEquals(0, evalLong("difftime('MONTH', t1, t5)", bindings));
    Assert.assertEquals(4, evalLong("difftime('MONTH', t1, t6)", bindings));
    Assert.assertEquals(11, evalLong("difftime('MONTH', t1, t7)", bindings));

    Assert.assertEquals(0, evalLong("difftime('YEAR', t1, t7)", bindings));
    Assert.assertEquals(16, evalLong("difftime('YEAR', t1, t8)", bindings));
  }

  @Test
  public void testDatetimeExtract()
  {
    DateTime time1 = new DateTime("2016-03-04T16:25:00", DateTimeZone.forID("Asia/Seoul"));
    DateTime time2 = new DateTime("2032-09-09T22:11:00Z");

    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.of(
            "t1", time1.getMillis(),
            "t2", time2.getMillis()
        )
    );

    Assert.assertEquals(25, evalLong("datetime_extract('MINUTE', t1)", bindings));
    Assert.assertEquals(11, evalLong("datetime_extract('MINUTE', t2)", bindings));

    Assert.assertEquals(16, evalLong("datetime_extract('HOUR', t1, 'Asia/Seoul')", bindings));
    Assert.assertEquals(7, evalLong("datetime_extract('HOUR', t1, 'UTC')", bindings));
    Assert.assertEquals(23, evalLong("datetime_extract('HOUR', t1, 'PST')", bindings));

    Assert.assertEquals(7, evalLong("datetime_extract('HOUR', t2, 'Asia/Seoul')", bindings));
    Assert.assertEquals(22, evalLong("datetime_extract('HOUR', t2, 'UTC')", bindings));
    Assert.assertEquals(15, evalLong("datetime_extract('HOUR', t2, 'PST')", bindings));

    Assert.assertEquals(4, evalLong("datetime_extract('DAY', t1, 'Asia/Seoul')", bindings));
    Assert.assertEquals(4, evalLong("datetime_extract('DAY', t1, 'UTC')", bindings));
    Assert.assertEquals(3, evalLong("datetime_extract('DAY', t1, 'PST')", bindings));

    Assert.assertEquals(10, evalLong("datetime_extract('DAY', t2, 'Asia/Seoul')", bindings));
    Assert.assertEquals(9, evalLong("datetime_extract('DAY', t2, 'UTC')", bindings));
    Assert.assertEquals(9, evalLong("datetime_extract('DAY', t2, 'PST')", bindings));

    Assert.assertEquals(5, evalLong("datetime_extract('DOW', t1, 'UTC')", bindings));
    Assert.assertEquals(4, evalLong("datetime_extract('DOW', t2, 'UTC')", bindings));

    Assert.assertEquals(64, evalLong("datetime_extract('DOY', t1, 'UTC')", bindings));
    Assert.assertEquals(253, evalLong("datetime_extract('DOY', t2, 'UTC')", bindings));

    Assert.assertEquals(10, evalLong("datetime_extract('WEEK', t1, 'UTC')", bindings));
    Assert.assertEquals(37, evalLong("datetime_extract('WEEK', t2, 'UTC')", bindings));

    Assert.assertEquals(9, evalLong("datetime_extract('WEEKOFWEEKYEAR', t1, 'UTC')", bindings));
    Assert.assertEquals(37, evalLong("datetime_extract('WEEKOFWEEKYEAR', t2, 'UTC')", bindings));

    Assert.assertEquals(3, evalLong("datetime_extract('MONTH', t1, 'UTC')", bindings));
    Assert.assertEquals(9, evalLong("datetime_extract('MONTH', t2, 'UTC')", bindings));

    Assert.assertEquals(2016, evalLong("datetime_extract('YEAR', t1, 'UTC')", bindings));
    Assert.assertEquals(2032, evalLong("datetime_extract('YEAR', t2, 'UTC')", bindings));

    Assert.assertEquals(2016, evalLong("datetime_extract('WEEKYEAR', t1, 'UTC')", bindings));
    Assert.assertEquals(2032, evalLong("datetime_extract('WEEKYEAR', t2, 'UTC')", bindings));

    Assert.assertEquals(1, evalLong("datetime_extract('QUARTER', t1, 'UTC')", bindings));
    Assert.assertEquals(3, evalLong("datetime_extract('QUARTER', t2, 'UTC')", bindings));
  }

  @Test
  public void testDatetimeExtractFromString()
  {
    Map<String, Object> mapping = Maps.newHashMap();
    mapping.put("x", "2016-03-04 16:25:00.123");

    Expr.NumericBinding bindings = Parser.withMap(mapping);
    Assert.assertEquals(2016, evalLong("datetime_extract('YEAR', x)", bindings));
    Assert.assertEquals(3, evalLong("datetime_extract('MONTH', x)", bindings));
    Assert.assertEquals(4, evalLong("datetime_extract('DAY', x)", bindings));

    mapping.put("x", "2016-03-04");
    Assert.assertEquals(2016, evalLong("datetime_extract('YEAR', x)", bindings));
    Assert.assertEquals(3, evalLong("datetime_extract('MONTH', x)", bindings));
    Assert.assertEquals(4, evalLong("datetime_extract('DAY', x)", bindings));
  }

  @Test
  public void testDatetimeExtractWeekYear()
  {
    DateTime time1 = DateTimes.utc("2010-01-01");
    DateTime time2 = DateTimes.utc("2010-01-02");
    DateTime time3 = DateTimes.utc("2010-01-03");
    DateTime time4 = DateTimes.utc("2010-01-04");
    DateTime time5 = DateTimes.utc("2009-01-01");
    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.of(
            "t1", time1.getMillis(),
            "t2", time2.getMillis(),
            "t3", time3.getMillis(),
            "t4", time4.getMillis(),
            "t5", time5.getMillis()
        )
    );

    Assert.assertEquals(2009, evalLong("datetime_extract('WEEKYEAR', t1, 'UTC')", bindings));
    Assert.assertEquals(53, evalLong("datetime_extract('WEEKOFWEEKYEAR', t1, 'UTC')", bindings));

    Assert.assertEquals(2009, evalLong("datetime_extract('WEEKYEAR', t2, 'UTC')", bindings));
    Assert.assertEquals(53, evalLong("datetime_extract('WEEKOFWEEKYEAR', t2, 'UTC')", bindings));

    Assert.assertEquals(2009, evalLong("datetime_extract('WEEKYEAR', t3, 'UTC')", bindings));
    Assert.assertEquals(53, evalLong("datetime_extract('WEEKOFWEEKYEAR', t3, 'UTC')", bindings));

    Assert.assertEquals(2010, evalLong("datetime_extract('WEEKYEAR', t4, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEKOFWEEKYEAR', t4, 'UTC')", bindings));

    Assert.assertEquals(2010, evalLong("datetime_extract('YEAR', t1, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEK', t1, 'UTC')", bindings));

    Assert.assertEquals(2010, evalLong("datetime_extract('YEAR', t2, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEK', t2, 'UTC')", bindings));

    Assert.assertEquals(2010, evalLong("datetime_extract('YEAR', t3, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEK', t3, 'UTC')", bindings));

    Assert.assertEquals(2010, evalLong("datetime_extract('YEAR', t4, 'UTC')", bindings));
    Assert.assertEquals(2, evalLong("datetime_extract('WEEK', t4, 'UTC')", bindings));


    Assert.assertEquals(2009, evalLong("datetime_extract('WEEKYEAR', t5, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEKOFWEEKYEAR', t5, 'UTC')", bindings));

    Assert.assertEquals(2009, evalLong("datetime_extract('YEAR', t5, 'UTC')", bindings));
    Assert.assertEquals(1, evalLong("datetime_extract('WEEK', t5, 'UTC')", bindings));
  }

  @Test
  public void testFormat()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", 1, "y", "ss"));
    Assert.assertEquals("001, ss", Parser.parse("format('%03d, %s', x, y)").eval(bindings).stringValue());
  }

  @Test
  public void testFormatWithDate()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("Y", "2008", "M", "1", "D", "3", "HM", "754"));
    Assert.assertEquals(
        DateTimes.utc("2008-01-03T07:54:00.000Z"),
        Parser.parse("datetime(format('%s/%s/%s %s',Y,M,D,lpad(HM,4,'0')), format='yyyy/MM/dd HHmm')")
              .eval(bindings)
              .value()
    );
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
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "a|b|c"));
    Assert.assertEquals("c", Parser.parse("split(x, '|', 2)").eval(bindings).stringValue());
    Assert.assertEquals(null, Parser.parse("split(x, '|', 4)").eval(bindings).stringValue());
    Assert.assertEquals("c", Parser.parse("splitRegex(x, '\\\\|', 2)").eval(bindings).stringValue());
    Assert.assertEquals(null, Parser.parse("splitRegex(x, '\\\\|', 4)").eval(bindings).stringValue());
  }

  @Test
  public void testLeft()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "abcde", "y", "abc"));
    Assert.assertNull(Parser.parse("left(z, 0)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("left(x, 0)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("left(y, 0)").eval(bindings).stringValue());
    Assert.assertEquals("a", Parser.parse("left(x, 1)").eval(bindings).stringValue());
    Assert.assertEquals("a", Parser.parse("left(y, 1)").eval(bindings).stringValue());
    Assert.assertEquals("abcd", Parser.parse("left(x, 4)").eval(bindings).stringValue());
    Assert.assertEquals("abc", Parser.parse("left(y, 4)").eval(bindings).stringValue());

    Assert.assertEquals("abcd", Parser.parse("left(x, -1)").eval(bindings).stringValue());
    Assert.assertEquals("ab", Parser.parse("left(y, -1)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("left(x, -5)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("left(y, -5)").eval(bindings).stringValue());
  }

  @Test
  public void testRight()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "abcde", "y", "abc"));
    Assert.assertEquals("", Parser.parse("right(x, 0)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("right(y, 0)").eval(bindings).stringValue());
    Assert.assertEquals("e", Parser.parse("right(x, 1)").eval(bindings).stringValue());
    Assert.assertEquals("c", Parser.parse("right(y, 1)").eval(bindings).stringValue());
    Assert.assertEquals("bcde", Parser.parse("right(x, 4)").eval(bindings).stringValue());
    Assert.assertEquals("abc", Parser.parse("right(y, 4)").eval(bindings).stringValue());

    Assert.assertEquals("bcde", Parser.parse("right(x, -1)").eval(bindings).stringValue());
    Assert.assertEquals("bc", Parser.parse("right(y, -1)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("right(x, -5)").eval(bindings).stringValue());
    Assert.assertEquals("", Parser.parse("right(y, -5)").eval(bindings).stringValue());
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
  public void testConcat() throws IOException
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.of("x", "navis", "y", "manse"));
    ObjectMapper mapper = new ObjectMapper();
    String value = mapper.readValue("\"concat(x, '\\u0001', y)\"", String.class);
    Assert.assertEquals("navis\u0001manse", evalString(value, bindings));
  }

  @Test
  public void testIn()
  {
    Set<String> strings = Sets.newHashSet("a", "c", "f");
    Map<String, Object> mapping = new HashMap<>();
    Expr.NumericBinding bindings = Parser.withMap(mapping);
    for (String x : new String[]{"a", "b", "c", ""}) {
      boolean eval = Parser.parse("in('" + x + "', 'a', 'c', 'f')").eval(null).asBoolean();
      Assert.assertEquals(strings.contains(x), eval);
    }
    for (int i = 0; i < 5; i++) {
      String value = String.valueOf((char) ('a' + i));
      mapping.put("x", value);
      boolean eval = Parser.parse("in(x, 'a', 'c', 'f')").eval(bindings).asBoolean();
      Assert.assertEquals(strings.contains(value), eval);
    }
    Set<Long> longs = Sets.newHashSet(1L, 3L, 5L);
    for (int i = 0; i < 5; i++) {
      mapping.put("x", (long) i);
      boolean eval = Parser.parse("in(x, 1, 3, 5)").eval(bindings).asBoolean();
      Assert.assertEquals(longs.contains((long) i), eval);
    }
    Set<Double> doubles = Sets.newHashSet(1D, 3D, 5D);
    for (int i = 0; i < 5; i++) {
      mapping.put("x", (double) i);
      boolean eval = Parser.parse("in(x, 1.0, 3.0, 5.0)").eval(bindings).asBoolean();
      Assert.assertEquals(doubles.contains((double) i), eval);
    }
    mapping.put("x", Doubles.asList(1, 2, 3));
    TypeResolver resolver = Parser.withTypeMap(ImmutableMap.of("x", ValueDesc.ofArray(ValueType.DOUBLE)));
    Assert.assertFalse(Evals.evalBoolean(Parser.parse("anyInColumn(0, x)", resolver), bindings));
    Assert.assertTrue(Evals.evalBoolean(Parser.parse("anyInColumn(1, x)", resolver), bindings));
    Assert.assertTrue(Evals.evalBoolean(Parser.parse("anyInColumn(1, 5, x)", resolver), bindings));
    Assert.assertFalse(Evals.evalBoolean(Parser.parse("anyInColumn(5, x)", resolver), bindings));
    Assert.assertFalse(Evals.evalBoolean(Parser.parse("anyInColumn(5, 6, x)", resolver), bindings));

    Assert.assertFalse(Evals.evalBoolean(Parser.parse("allInColumn(0, x)", resolver), bindings));
    Assert.assertTrue(Evals.evalBoolean(Parser.parse("allInColumn(1, 2, x)", resolver), bindings));
    Assert.assertTrue(Evals.evalBoolean(Parser.parse("allInColumn(1, 2, 3, x)", resolver), bindings));
    Assert.assertFalse(Evals.evalBoolean(Parser.parse("allInColumn(1, 2, 3, 4, x)", resolver), bindings));
  }

  @Test
  public void testJavaScript()
  {
    Map<String, Object> mapping = new HashMap<>();
    for (int i = 0; i < 30; i++) {
      mapping.put("x", i);
      Expr.NumericBinding bindings = Parser.withMap(mapping);
      String eval = Parser.parse(
          "javascript('x', 'if (x < 10) return \"X\"; else if (x < 20) return \"Y\"; else return \"Z\";')"
      ).eval(bindings).stringValue();
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
    Assert.assertTrue(Parser.parse("regex.match('navis', '.*s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', 'n.*v..*')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', '.*v..')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', '.*vi.*')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', '..vi.')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', 'n.*s')").eval(bindings).asBoolean());
    Assert.assertTrue(Parser.parse("regex.match('navis', '.a.*i.')").eval(bindings).asBoolean());

    Assert.assertFalse(Parser.parse("regex.match('nabis', 'n.*v.*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex.match('nabis', '.*v...*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex.match('nabis', '.*vi.*')").eval(bindings).asBoolean());
    Assert.assertFalse(Parser.parse("regex.match('nabis', '..vi.')").eval(bindings).asBoolean());

    Assert.assertEquals("navi", Parser.parse("regex('navis', '(.*)s', 1)").eval(bindings).asString());
    Assert.assertEquals("is", Parser.parse("regex('navis', '(.*)v(..)', 2)").eval(bindings).asString());
    Assert.assertEquals("navis", Parser.parse("regex('navis', '.*vi.*', 0)").eval(bindings).asString());

    Assert.assertEquals(
        "198.126.63",
        Parser.parse("regex('198.126.63.1', '(\\\\d{1,4}(\\\\.\\\\d{1,4}){2})\\\\.(\\\\d{1,4})', 1)")
              .eval(bindings).asString()
    );
    Assert.assertEquals(
        "$1 = 198.126.63, $3 = 1",
        Parser.parse("regex('198.126.63.1', '(\\\\d{1,4}(\\\\.\\\\d{1,4}){2})\\\\.(\\\\d{1,4})', '\\'$1 = \\' + $1 + \\', $3 = \\' + $3')")
              .eval(bindings).asString()
    );
  }

  @Test
  @Ignore("needs native library and R_HOME env")
  public void testRFunc()
  {
    // basic
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", 30, "b", 3));
    Assert.assertEquals(33, evalLong("r('func <- function(a, b) { a + b }', 'func', a, b)", bindings));

    // R to Java
    Map eval = (Map) eval(
        "r('map <- function(a, b) {"
        + "  data.frame("
        + "    gender = c(\"Male\", \"Male\",\"Female\"), \n"
        + "    height = c(152, 171.5, 165), \n"
        + "    weight = c(81, 93, 78),\n"
        + "    Age = c(42, 38, 26)\n"
        + "  )"
        + "}', 'map', a, b)", bindings
    );
    Assert.assertEquals(4, eval.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) eval.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165}, (double[]) eval.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78}, (double[]) eval.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) eval.get("Age"), 0.001);

    // java to R
    bindings = Parser.withMap(
        ImmutableMap.<String, Object>of(
            "x", ImmutableMap.<String, Object>of(
                "gender", new String[]{"Male", "Male", "Female"},
                "height", new double[]{152, 171.5, 165},
                "weight", new double[]{81, 93, 78},
                "Age", new double[]{42, 38, 26}
            )
        )
    );
    Map map = (Map) eval("r('identity <- function(x) { x }', 'identity', x)", bindings);
    Assert.assertEquals(4, map.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165}, (double[]) map.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78}, (double[]) map.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) map.get("Age"), 0.001);

    bindings = Parser.withMap(
        ImmutableMap.<String, Object>of(
            "x", ImmutableList.<Object>of(
                new String[]{"Male", "Male", "Female"},
                new double[]{152, 171.5, 165},
                new double[]{81, 93, 78},
                new double[]{42, 38, 26}
            )
        )
    );
    List list = (List) eval("r('identity <- function(x) { x }', 'identity', x)", bindings);
    Assert.assertEquals(4, list.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) list.get(0));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165}, (double[]) list.get(1), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78}, (double[]) list.get(2), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) list.get(3), 0.001);
  }

  @Test
  @Ignore("needs jython-2.7.0 to be installed and system property 'python.home' is set")
  public void testPyFunc()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", 30, "b", 3));
    Assert.assertEquals(90, evalLong("py('def multi(a,b): return a * b', 'multi', a, b)", bindings));

    // python to Java
    Map eval = (Map) eval(
        "py('def map(a, b): \n"
        + "  dict = { } \n"
        + "  dict[\"gender\"] = [\"Male\", \"Male\",\"Female\"] \n"
        + "  dict[\"height\"] = [152, 171.5, 165, a] \n"
        + "  dict[\"weight\"] = [81, 93, 78, b] \n"
        + "  dict[\"Age\"] = [42, 38, 26, a * b] \n"
        + "  return dict\n'"
        + ", 'map', a, b)", bindings
    );
    Assert.assertEquals(4, eval.size());
    Assert.assertEquals(Lists.newArrayList("Male", "Male", "Female"), eval.get("gender"));
    Assert.assertEquals(Lists.newArrayList(152L, 171.5, 165L, 30L), eval.get("height"));
    Assert.assertEquals(Lists.newArrayList(81L, 93L, 78L, 3L), eval.get("weight"));
    Assert.assertEquals(Lists.newArrayList(42L, 38L, 26L, 90L), eval.get("Age"));

    // java to python
    Map<String, Object> param = ImmutableMap.<String, Object>of(
        "gender", new String[]{"Male", "Male", "Female"},
        "height", new double[]{152, 171.5, 165},
        "weight", new double[]{81, 93, 78},
        "Age", new double[]{42, 38, 26}
    );
    bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", param, "a", 30, "b", 3));
    Map map = (Map) eval("py('def identity(x): return x', 'identity', x)", bindings);
    Assert.assertEquals(4, map.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165}, (double[]) map.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78}, (double[]) map.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) map.get("Age"), 0.001);

    Map map2 = (Map) eval(
        "py('def modify(x, a, b):\n"
        + "  x[\"height\"].append(a)\n"
        + "  x[\"weight\"].append(b)\n"
        + "  x[\"Age\"].append(a * b)\n"
        + "  return x', 'modify', x, a, b)", bindings);
    Assert.assertEquals(4, map2.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map2.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165, 30}, (double[]) map2.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78, 3}, (double[]) map2.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26, 90}, (double[]) map2.get("Age"), 0.001);

    // dynamic method
    String pyCode = "py('def modify1(x, a, b):\n"
                    + "  x[\"height\"].append(a)\n"
                    + "  return x\n"
                    + "def modify2(x, a, b): \n"
                    + "  x[\"weight\"].append(b)\n"
                    + "  return x'";

    bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", param, "a", 30, "b", 3, "m", "modify1"));
    Map map3 = (Map) eval(pyCode + ", m, x, a, b)", bindings);

    Assert.assertEquals(4, map3.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map3.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165, 30}, (double[]) map3.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78}, (double[]) map3.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) map3.get("Age"), 0.001);

    bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", param, "a", 30, "b", 3, "m", "modify2"));
    Map map4 = (Map) eval(pyCode + ", m, x, a, b)", bindings);

    Assert.assertEquals(4, map4.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map4.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165}, (double[]) map4.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78, 3}, (double[]) map4.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26}, (double[]) map4.get("Age"), 0.001);
  }

  @Test
  @Ignore("needs jython-2.7.0 to be installed and system property 'python.home' is set")
  public void testPyEvalFunc()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", 30, "b", 3));
    Assert.assertEquals(90, evalLong("pyEval('a * b')", bindings));

    bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", new double[]{10, 20}, "b", new double[]{30, 40}));
    Assert.assertArrayEquals(new double[]{10, 20, 30, 40}, (double[]) eval("pyEval('a + b')", bindings), 0.001);

    Map<String, Object> param = ImmutableMap.<String, Object>of(
        "gender", new String[]{"Male", "Male", "Female"},
        "height", new double[]{152, 171.5, 165},
        "weight", new double[]{81, 93, 78},
        "Age", new double[]{42, 38, 26}
    );
    bindings = Parser.withMap(
        ImmutableMap.<String, Object>of("x", param, "a", 30, "b", 3)
    );

    Map map2 = (Map) eval(
        "pyEval('x[\"height\"].append(a), x[\"weight\"].append(b), x[\"Age\"].append(a * b), x', x, a, b)", bindings);
    Assert.assertEquals(4, map2.size());
    Assert.assertArrayEquals(new String[]{"Male", "Male", "Female"}, (String[]) map2.get("gender"));
    Assert.assertArrayEquals(new double[]{152, 171.5, 165, 30}, (double[]) map2.get("height"), 0.001);
    Assert.assertArrayEquals(new double[]{81, 93, 78, 3}, (double[]) map2.get("weight"), 0.001);
    Assert.assertArrayEquals(new double[]{42, 38, 26, 90}, (double[]) map2.get("Age"), 0.001);
  }

  @Test
  public void testExcel()
  {
    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.<String, Object>of("r", 0.5d, "n", 0.1d, "y", 3.5d, "p", -2.5d)
    );
    // don't know what the hell is this
    Assert.assertEquals(2.168962048d, Parser.parse("fv (r, n, y, p, 'true')").eval(bindings).asDouble(), 0.00001);
    Assert.assertEquals(1.983438510d, Parser.parse("pv (r, n, y, p, 'true')").eval(bindings).asDouble(), 0.00001);
    Assert.assertEquals(-9.22213780d, Parser.parse("pmt(r, n, y, p, 'true')").eval(bindings).asDouble(), 0.00001);
  }

  @Test
  public void testIPv4Address()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of());
    Assert.assertTrue(Parser.parse("ipv4_in('192.168.3.4', '192.168.0.0')").eval(bindings).asBoolean());
    Assert.assertTrue(
        Parser.parse("ipv4_in('192.168.3.4', '192.168.0.0', '192.168.128.128')")
              .eval(bindings)
              .asBoolean()
    );
    Assert.assertFalse(
        Parser.parse("ipv4_in('192.168.3.4', '192.168.32.0', '192.168.128.128')")
              .eval(bindings)
              .asBoolean()
    );
  }

  @Test
  public void testTypes()
  {
    TypeResolver bindings = Parser.withTypeMap(
        ImmutableMap.<String, ValueDesc>of(
            "a", ValueDesc.LONG, "b", ValueDesc.STRING, "c", ValueDesc.DOUBLE, "d", ValueDesc.DIM_STRING
        )
    );
    Assert.assertEquals(ValueDesc.LONG, Parser.parse("a * cast(b, 'long')", bindings).returns());
    Assert.assertEquals(ValueDesc.DOUBLE, Parser.parse("a * cast(b, 'double')", bindings).returns());
    Assert.assertEquals(ValueDesc.STRING, Parser.parse("concat(a, cast(b, 'double'))", bindings).returns());
    Assert.assertEquals(ValueDesc.LONG, Parser.parse("a * cast(b, 'long')", bindings).returns());

    Assert.assertEquals(ValueDesc.LONG, Parser.parse("if(C == '', 0, CAST(C, 'INT') / 10 * 10)", bindings).returns());
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("if(C == '', 0, CAST(C, 'INT') / 10 * 10.0)", bindings).returns()
    );
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("if(C == '', 0.0, CAST(C, 'INT') / 10 * 10.0)", bindings).returns()
    );

    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("if(C == '', 0, CAST(C, 'INT') / 10 * 10d)", bindings).returns()
    );
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("if(C == '', 0d, CAST(C, 'INT') / 10 * 10d)", bindings).returns()
    );

    Assert.assertEquals(ValueDesc.STRING, Parser.parse("if(C == '', b, 'x')", bindings).returns());
    Assert.assertEquals(ValueDesc.STRING, Parser.parse("if(C == '', d, 'x')", bindings).returns());

    Assert.assertEquals(
        ValueDesc.LONG,
        Parser.parse("switch(C, '', 0, '', CAST(C, 'INT') / 10 * 10)", bindings).returns()
    );
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("switch(C, '', 0, '', CAST(C, 'INT') / 10 * 10.0)", bindings).returns()
    );
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("switch(C, '', 0.0, '', CAST(C, 'INT') / 10 * 10.0)", bindings).returns()
    );

    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("switch(C, '', 0, '', CAST(C, 'INT') / 10 * 10d)", bindings).returns()
    );
    Assert.assertEquals(
        ValueDesc.DOUBLE,
        Parser.parse("switch(C, '', 0d, '', CAST(C, 'INT') / 10 * 10d)", bindings).returns()
    );

    Assert.assertEquals(ValueDesc.STRING, Parser.parse("switch(C, '', b, '', 'x')", bindings).returns());
    Assert.assertEquals(ValueDesc.STRING, Parser.parse("switch(C, '', d, '', 'x')", bindings).returns());
    Assert.assertEquals(ValueDesc.UNKNOWN, Parser.parse("switch(C, '', d, '', 'x', 3)", bindings).returns());
  }

  @Test
  public void testNullCasting()
  {
    // changed semantic.. <null op numeric> or <numeric op null> is null
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", 10, "b", 20.D, "c", ""));
    Assert.assertTrue(_eval("a + c", bindings).isNull());
    Assert.assertTrue(_eval("a - c", bindings).isNull());
    Assert.assertTrue(_eval("a * c", bindings).isNull());
    Assert.assertTrue(_eval("a / c", bindings).isNull());

    Assert.assertTrue(_eval("c + a", bindings).isNull());
    Assert.assertTrue(_eval("c - a", bindings).isNull());
    Assert.assertTrue(_eval("c * a", bindings).isNull());
    Assert.assertTrue(_eval("c / a", bindings).isNull());

    Assert.assertTrue(_eval("b + c", bindings).isNull());
    Assert.assertTrue(_eval("b - c", bindings).isNull());
    Assert.assertTrue(_eval("b * c", bindings).isNull());
    Assert.assertTrue(_eval("b / c", bindings).isNull());

    Assert.assertTrue(_eval("c + b", bindings).isNull());
    Assert.assertTrue(_eval("c - b", bindings).isNull());
    Assert.assertTrue(_eval("c * b", bindings).isNull());
    Assert.assertTrue(_eval("c / b", bindings).isNull());
  }

  @Test
  public void testBetween()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", 10, "b", 20.D, "c", ""));
    Assert.assertTrue(_eval("between(a, 10, 15)", bindings).asBoolean());
    Assert.assertTrue(_eval("between(b, 15, 20)", bindings).asBoolean());
    Assert.assertFalse(_eval("between(b, 25, 30)", bindings).asBoolean());
  }

  @Test
  public void testStartsWith()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", "navis", "b", "NavIs", "c", ""));
    Assert.assertTrue(_eval("startsWith(a, 'na')", bindings).asBoolean());
    Assert.assertFalse(_eval("startsWith(b, 'na')", bindings).asBoolean());
    Assert.assertTrue(_eval("startsWithIgnoreCase(b, 'na')", bindings).asBoolean());
    Assert.assertFalse(_eval("startsWith(c, 'na')", bindings).asBoolean());

    Assert.assertTrue(_eval("endsWith(a, 'is')", bindings).asBoolean());
    Assert.assertFalse(_eval("endsWith(b, 'is')", bindings).asBoolean());
    Assert.assertTrue(_eval("endsWithIgnoreCase(b, 'is')", bindings).asBoolean());
  }

  @Test
  public void testMatchFind()
  {
    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("a", "navis", "b", "NavIs", "c", ""));
    Assert.assertTrue(_eval("regex.match(a, 'navis')", bindings).asBoolean());
    Assert.assertFalse(_eval("regex.match(a, 'avis')", bindings).asBoolean());
    Assert.assertTrue(_eval("regex.find(a, 'avis')", bindings).asBoolean());
    Assert.assertFalse(_eval("regex.find(a, 'avis', 2)", bindings).asBoolean());
  }

  @Test
  public void testXTrim()
  {
    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.<String, Object>of(
            "a", "  navis ",
            "b", "??navis?",
            "c", "*?NavIs ?",
            "d", ""
        )
    );
    Assert.assertEquals("navis", evalString("btrim(a)", bindings));
    Assert.assertEquals("  navis", evalString("rtrim(a)", bindings));
    Assert.assertEquals("navis ", evalString("ltrim(a)", bindings));

    Assert.assertEquals("navis", evalString("btrim(b, '?')", bindings));
    Assert.assertEquals("??navis", evalString("rtrim(b, '?')", bindings));
    Assert.assertEquals("navis?", evalString("ltrim(b, '?')", bindings));

    Assert.assertEquals("NavIs ", evalString("btrim(c, '*?')", bindings));
    Assert.assertEquals("*?NavIs ", evalString("rtrim(c, '*?')", bindings));
    Assert.assertEquals("NavIs ?", evalString("ltrim(c, '*?')", bindings));
  }

  @Test
  public void testGranularFunctions()
  {
    DateTime time1 = new DateTime("2016-03-04T22:25:00");
    DateTime time2 = new DateTime("2018-10-27T22:25:00");
    Expr.NumericBinding bindings = Parser.withMap(
        ImmutableMap.<String, Object>of(
            "__time1", time1,
            "__time2", time2.getMillis()
        )
    );
    for (GranularityType type : GranularityType.values()) {
      Granularity granularity = type.getDefaultGranularity();
      Assert.assertEquals(
          granularity.bucketStart(time1).getMillis(),
          evalLong("bucketStart(__time1, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketStart(time1),
          evalDateTime("bucketStartDateTime(__time1, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketStart(time2).getMillis(),
          evalLong("bucketStart(__time2, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketStart(time2),
          evalDateTime("bucketStartDateTime(__time2, '" + type.name() + "')", bindings)
      );

      Assert.assertEquals(
          granularity.bucketEnd(time1).getMillis(),
          evalLong("bucketEnd(__time1, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketEnd(time1),
          evalDateTime("bucketEndDateTime(__time1, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketEnd(time2).getMillis(),
          evalLong("bucketEnd(__time2, '" + type.name() + "')", bindings)
      );
      Assert.assertEquals(
          granularity.bucketEnd(time2),
          evalDateTime("bucketEndDateTime(__time2, '" + type.name() + "')", bindings)
      );
    }
  }

  @Test
  public void testArrayAccess()
  {
    Expr parsed = Parser.parse("\"x\"[0]");
    Assert.assertEquals(ImmutableList.of("x"), Parser.findRequiredBindings(parsed));

    Expr.NumericBinding bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", new double[]{1, 2, 3}));
    Assert.assertEquals(1, evalDouble("\"x\"[0]", bindings), 0.00001);
    Assert.assertEquals(2, evalDouble("\"x\"[1]", bindings), 0.00001);
    Assert.assertEquals(3, evalDouble("\"x\"[2]", bindings), 0.00001);

    bindings = Parser.withMap(ImmutableMap.<String, Object>of("x", Arrays.asList(1d, 2d, 3d)));
    Assert.assertEquals(1, evalDouble("\"x\"[0]", bindings), 0.00001);
    Assert.assertEquals(2, evalDouble("\"x\"[1]", bindings), 0.00001);
    Assert.assertEquals(3, evalDouble("\"x\"[2]", bindings), 0.00001);
  }

  @Test
  public void testFunctionOptimize()
  {
    Expr caseFn = Parser.parse("case(Category=='Office Supplies', 'O',Category=='Furniture', 'F',Category=='Technology', 'T', Category)");
    validate(caseFn, "(case [(Category == Office Supplies), O, (Category == Furniture), F, (Category == Technology), T, Category])");
    validate(Parser.optimizeFunction(caseFn), "(__map [Category, Category])");

    Expr switchFn = Parser.parse("switch(Category, 'Office Supplies', 'O','Furniture', 'F', 'Technology', 'T', Category)");
    validate(switchFn, "(switch [Category, Office Supplies, O, Furniture, F, Technology, T, Category])");
    validate(Parser.optimizeFunction(switchFn), "(__map [Category, Category])");
  }

  private void validate(Expr parsed, String expected)
  {
    Assert.assertEquals(expected, parsed.toString());
    Assert.assertEquals("O", Evals.evalString(parsed, Parser.withMap(ImmutableMap.of("Category", "Office Supplies"))));
    Assert.assertEquals("F", Evals.evalString(parsed, Parser.withMap(ImmutableMap.of("Category", "Furniture"))));
    Assert.assertEquals("T", Evals.evalString(parsed, Parser.withMap(ImmutableMap.of("Category", "Technology"))));
    Assert.assertEquals("Other", Evals.evalString(parsed, Parser.withMap(ImmutableMap.of("Category", "Other"))));
  }
}
