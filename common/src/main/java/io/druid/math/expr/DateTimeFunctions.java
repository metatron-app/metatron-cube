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

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import io.druid.common.utils.JodaUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 */
public interface DateTimeFunctions extends Function.Library
{
  class CurrentTime implements Function
  {

    @Override
    public String name()
    {
      return "current_time";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      return ExprEval.of(new DateTime(System.currentTimeMillis()));
    }
  }

  class Recent implements Function
  {
    @Override
    public String name()
    {
      return "recent";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs one or two arguments");
      }
      return ExprEval.of(toInterval(args, bindings), ExprType.STRING);
    }

    protected Interval toInterval(List<Expr> args, Expr.NumericBinding bindings)
    {
      final DateTime now = new DateTime(System.currentTimeMillis());
      Period beforeNow = JodaUtils.toPeriod(args.get(0).eval(bindings).asString());
      if (args.size() == 1) {
        return new Interval(now.minus(beforeNow), now);
      }
      Period afterNow = JodaUtils.toPeriod(args.get(1).eval(bindings).asString());
      return new Interval(now.minus(beforeNow), now.minus(afterNow));
    }
  }

  class AddTime implements Function
  {
    @Override
    public String name()
    {
      return "add_time";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs two or three arguments");
      }
      String timeZone = args.size() == 3 ? Evals.getConstantString(args.get(2)) : null;
      DateTime base = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
      Period period = JodaUtils.toPeriod(Evals.getConstantString(args.get(1)));
      return ExprEval.of(base.plus(period));
    }
  }

  class SubTime implements Function
  {
    @Override
    public String name()
    {
      return "sub_time";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs two or three arguments");
      }
      String timeZone = args.size() == 3 ? Evals.getConstantString(args.get(2)) : null;
      DateTime base = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
      Period period = JodaUtils.toPeriod(Evals.getConstantString(args.get(1)));
      return ExprEval.of(base.minus(period));
    }
  }

  abstract class DateTimeMath implements Function
  {
    @Override
    public final ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs one or two arguments");
      }
      String timeZone = args.size() == 2 ? Evals.getConstantString(args.get(1)) : null;
      DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
      return evaluate(time);
    }

    protected abstract ExprEval evaluate(DateTime time);
  }

  class DayName extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "dayname";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.dayOfWeek().getAsText());
    }
  }

  class DayOfMonth extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "dayofmonth";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getDayOfMonth());
    }
  }

  class DayOfWeek extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "dayofweek";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getDayOfWeek());
    }
  }

  class DayOfYear extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "dayofyear";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getDayOfYear());
    }
  }

  class Hour extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "hour";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getHourOfDay());
    }
  }

  class Month extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "month";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getMonthOfYear());
    }
  }

  class MonthName extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "monthname";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.monthOfYear().getAsText());
    }
  }

  class Year extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "year";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.getYear());
    }
  }

  class FirstDay extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "first_day";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.dayOfMonth().withMinimumValue());
    }
  }

  class LastDay extends DateTimeMath
  {
    @Override
    public String name()
    {
      return "last_day";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.dayOfMonth().withMaximumValue());
    }
  }

  class TimestampFromEpochFunc extends BuiltinFunctions.NamedParams
  {
    // yyyy-MM-ddThh:mm:ss[.sss][Z|[+-]hh:mm]
    static final DateFormat ISO8601 = new ISO8601DateFormat();  // thread-safe

    DateFormat formatter;

    @Override
    public String name()
    {
      return "timestamp";
    }

    @Override
    protected ExprEval eval(List<Expr> params, Map<String, Expr> namedParams, Expr.NumericBinding bindings)
    {
      if (params.isEmpty()) {
        throw new RuntimeException("function '" + name() + " needs at least 1 generic argument");
      }
      if (formatter == null) {
        initialize(params, namedParams, bindings);
      }
      ExprEval value = params.get(0).eval(bindings);
      if (value.type() != ExprType.STRING) {
        throw new IllegalArgumentException("first argument should be string type but got " + value.type() + " type");
      }

      Date date;
      try {
        date = formatter.parse(value.stringValue());
      }
      catch (ParseException e) {
        throw new IllegalArgumentException("invalid value " + value.stringValue() + " in " + e.getErrorOffset(), e);
      }
      return toValue(date);
    }

    protected void initialize(List<Expr> args, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      String format = args.size() > 1 ?
                      Evals.getConstantString(args.get(1)).trim() :
                      Evals.evalOptionalString(params.get("format"), bindings);
      String language = args.size() > 2 ?
                        Evals.getConstantString(args.get(2)).trim() :
                        Evals.evalOptionalString(params.get("locale"), bindings);
      String timezone = args.size() > 3 ?
                        Evals.getConstantString(args.get(3)).trim() :
                        Evals.evalOptionalString(params.get("timezone"), bindings);

      formatter = format == null ? ISO8601 : language == null ?
                                             new SimpleDateFormat(format) :
                                             new SimpleDateFormat(format, Locale.forLanguageTag(language));
      if (timezone != null) {
        formatter.setTimeZone(TimeZone.getTimeZone(timezone));
      }
    }

    protected ExprEval toValue(Date date)
    {
      return ExprEval.of(date.getTime(), ExprType.LONG);
    }

    @Override
    public Function get()
    {
      return new TimestampFromEpochFunc();
    }
  }

  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "unix_timestamp";
    }

    @Override
    protected final ExprEval toValue(Date date)
    {
      return ExprEval.of(date.getTime() / 1000, ExprType.LONG);
    }

    @Override
    public Function get()
    {
      return new UnixTimestampFunc();
    }
  }

  class DateTimeFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "datetime";
    }

    @Override
    protected final ExprEval toValue(Date date)
    {
      return ExprEval.of(new DateTime(date.getTime(), DateTimeZone.forTimeZone(formatter.getTimeZone())));
    }

    @Override
    public Function get()
    {
      return new DateTimeFunc();
    }
  }

  class TimestampValidateFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "timestamp_validate";
    }

    @Override
    protected final ExprEval eval(List<Expr> params, Map<String, Expr> namedParams, Expr.NumericBinding bindings)
    {
      try {
        super.eval(params, namedParams, bindings);
      }
      catch (Exception e) {
        return ExprEval.of(false);
      }
      return ExprEval.of(true);
    }

    @Override
    public Function get()
    {
      return new TimestampValidateFunc();
    }
  }

  class TimeExtractFunc extends TimestampFromEpochFunc
  {
    private DateFormat outputFormat;

    @Override
    public String name()
    {
      return "time_extract";
    }

    @Override
    protected void initialize(List<Expr> args, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      super.initialize(args, params, bindings);
      String format = Evals.evalOptionalString(params.get("out.format"), bindings);
      String language = Evals.evalOptionalString(params.get("out.locale"), bindings);
      String timezone = Evals.evalOptionalString(params.get("out.timezone"), bindings);

      outputFormat = format == null ? ISO8601 : language == null ?
                                                new SimpleDateFormat(format) :
                                                new SimpleDateFormat(format, Locale.forLanguageTag(language));
      if (timezone != null) {
        outputFormat.setTimeZone(TimeZone.getTimeZone(timezone));
      }
    }

    @Override
    protected final ExprEval toValue(Date date)
    {
      return ExprEval.of(outputFormat.format(date));
    }

    @Override
    public Function get()
    {
      return new TimeExtractFunc();
    }
  }
}

