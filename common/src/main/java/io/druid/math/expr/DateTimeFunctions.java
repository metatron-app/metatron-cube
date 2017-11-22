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

import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;

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
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DATETIME;
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
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs one or two arguments");
      }
      return ExprEval.of(toInterval(args, bindings), ExprType.UNKNOWN);
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

  public static abstract class GranularFunc extends Function.NewInstance
  {
    private Granularity granularity;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs two arguments");
      }
      if (granularity == null) {
        String string = args.get(1).eval(bindings).asString();
        granularity = Granularity.fromString(string);
      }
      return eval(args.get(0).eval(bindings), granularity);
    }

    protected abstract ExprEval eval(ExprEval param, Granularity granularity);
  }

  public static class BucketStart extends GranularFunc
  {
    @Override
    public String name()
    {
      return "bucketStart";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(ExprEval param, Granularity granularity)
    {
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(param.asLong())).getMillis());
    }
  }

  public static class BucketEnd extends GranularFunc
  {
    @Override
    public String name()
    {
      return "bucketEnd";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(ExprEval param, Granularity granularity)
    {
      return ExprEval.of(granularity.bucketEnd(DateTimes.utc(param.asLong())).getMillis());
    }
  }

  public static class BucketStartDT extends GranularFunc
  {
    @Override
    public String name()
    {
      return "bucketStartDateTime";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DATETIME;
    }

    @Override
    protected ExprEval eval(ExprEval param, Granularity granularity)
    {
      return ExprEval.of(granularity.bucketStart(param.asDateTime()));
    }
  }

  public static class BucketEndDT extends GranularFunc
  {
    @Override
    public String name()
    {
      return "bucketEndDateTime";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DATETIME;
    }

    @Override
    protected ExprEval eval(ExprEval param, Granularity granularity)
    {
      return ExprEval.of(granularity.bucketEnd(param.asDateTime()));
    }
  }

  abstract class UnaryTimeMath extends Function.NewInstance
  {
    boolean initialized;
    DateTimeZone timeZone;

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    public final ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 1 && args.size() != 2) {
          throw new IllegalArgumentException("function '" + name() + "' needs one or two arguments");
        }
        timeZone = args.size() == 2 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(1))) : null;
        initialized = true;
      }
      DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
      return evaluate(time);
    }

    protected abstract ExprEval evaluate(DateTime time);
  }

  abstract class BinaryTimeMath extends Function.NewInstance
  {
    boolean initialized;
    DateTimeZone timeZone;

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DATETIME;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2 && args.size() != 3) {
          throw new IllegalArgumentException("function '" + name() + "' needs two or three arguments");
        }
        timeZone = args.size() == 3 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(2))) : null;
        initialized = true;
      }
      DateTime base = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
      Period period = JodaUtils.toPeriod(Evals.getConstantString(args.get(1)));
      return evaluate(base, period);
    }

    protected abstract ExprEval evaluate(DateTime base, Period period);
  }

  class AddTime extends BinaryTimeMath
  {
    @Override
    public String name()
    {
      return "add_time";
    }

    @Override
    protected final ExprEval evaluate(DateTime base, Period period)
    {
      return ExprEval.of(base.plus(period));
    }
  }

  class SubTime extends BinaryTimeMath
  {
    @Override
    public String name()
    {
      return "sub_time";
    }

    @Override
    protected final ExprEval evaluate(DateTime base, Period period)
    {
      return ExprEval.of(base.minus(period));
    }
  }

  static enum Unit
  {
    MILLIS, EPOCH, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR, DOW, DOY, QUARTER
  }

  // whole time based
  class DiffTime extends ExprType.LongFunction implements Function.Factory
  {
    private Unit unit;
    private DateTimeZone timeZone;

    @Override
    public String name()
    {
      return "difftime";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (unit == null) {
        if (args.size() != 3 && args.size() != 4) {
          throw new IllegalArgumentException("function '" + name() + "' needs three or four arguments");
        }
        Expr param = args.get(0);
        if (Evals.isConstantString(param)) {
          unit = Unit.valueOf(Evals.getConstantString(param).toUpperCase());
        } else {
          unit = Unit.values()[Ints.checkedCast(Evals.getConstantLong(param))];
        }
        timeZone = args.size() == 4 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(3))) : null;
      }

      DateTime time1 = Evals.toDateTime(args.get(1).eval(bindings), timeZone);
      DateTime time2 = Evals.toDateTime(args.get(2).eval(bindings), timeZone);

      switch (unit) {
        case SECOND:
          return ExprEval.of(Seconds.secondsBetween(time1, time2).getSeconds());
        case MINUTE:
          return ExprEval.of(Minutes.minutesBetween(time1, time2).getMinutes());
        case HOUR:
          return ExprEval.of(Hours.hoursBetween(time1, time2).getHours());
        case DAY:
          return ExprEval.of(Days.daysBetween(time1, time2).getDays());
        case WEEK:
          return ExprEval.of(Weeks.weeksBetween(time1, time2).getWeeks());
        case MONTH:
          return ExprEval.of(Months.monthsBetween(time1, time2).getMonths());
        case YEAR:
          return ExprEval.of(Years.yearsBetween(time1, time2).getYears());
        case MILLIS:
        case EPOCH:
          return ExprEval.of(new Duration(time1, time2).getMillis());
        default:
          throw new IllegalArgumentException("invalid time unit " + unit);
      }
    }

    @Override
    public Function get()
    {
      return new DiffTime();
    }
  }

  // locale?
  class DayName extends UnaryTimeMath
  {
    @Override
    public String name()
    {
      return "dayname";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.STRING;
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.dayOfWeek().getAsText());
    }
  }

  class DayOfMonth extends UnaryTimeMath
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

  class LastDayOfMonth extends UnaryTimeMath
  {
    @Override
    public String name()
    {
      return "lastdayofmonth";
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.dayOfMonth().getMaximumValue());
    }
  }

  class DayOfWeek extends UnaryTimeMath
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

  class DayOfYear extends UnaryTimeMath
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

  class Hour extends UnaryTimeMath
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

  class Month extends UnaryTimeMath
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

  // locale?
  class MonthName extends UnaryTimeMath
  {
    @Override
    public String name()
    {
      return "monthname";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.STRING;
    }

    @Override
    protected final ExprEval evaluate(DateTime time)
    {
      return ExprEval.of(time.monthOfYear().getAsText());
    }
  }

  class Year extends UnaryTimeMath
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

  class FirstDay extends UnaryTimeMath
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

  class LastDay extends UnaryTimeMath
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

  abstract class DateTimeInput extends BuiltinFunctions.NamedParams implements Function.Factory
  {
    DateTimeFormatter formatter;

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
      return toValue(Evals.toDateTime(value, formatter));
    }

    protected void initialize(List<Expr> args, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      String format = args.size() > 1 ?
                      Evals.getConstantString(args.get(1)).trim() :
                      Evals.evalOptionalString(params.get("format"), bindings);
      String locale = args.size() > 2 ?
                      Evals.getConstantString(args.get(2)).trim() :
                      Evals.evalOptionalString(params.get("locale"), bindings);
      String timezone = args.size() > 3 ?
                        Evals.getConstantString(args.get(3)).trim() :
                        Evals.evalOptionalString(params.get("timezone"), bindings);

      formatter = format == null && locale == null && timezone == null ? JodaUtils.ISO8601 :
                  JodaUtils.toTimeFormatter(format, locale, timezone);
    }

    protected abstract ExprEval toValue(DateTime date);
  }

  // string/long to long
  class TimestampFromEpochFunc extends DateTimeInput
  {
    @Override
    public String name()
    {
      return "timestamp";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    protected ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis(), ExprType.LONG);
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
    protected final ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis() / 1000, ExprType.LONG);
    }

    @Override
    public Function get()
    {
      return new UnixTimestampFunc();
    }
  }

  class DateTimeFunc extends DateTimeInput
  {
    private DateTimeZone timeZone;

    @Override
    public String name()
    {
      return "datetime";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DATETIME;
    }

    protected void initialize(List<Expr> args, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      super.initialize(args, params, bindings);
      String timezone = Evals.evalOptionalString(params.get("out.timezone"), bindings);
      if (timezone != null) {
        DateTimeZone timeZone = JodaUtils.toTimeZone(timezone);
        if (!timeZone.equals(formatter.getZone())) {
          this.timeZone = timeZone;
        }
      }
    }

    @Override
    protected final ExprEval toValue(DateTime date)
    {
      return ExprEval.of(timeZone == null ? date : new DateTime(date.getMillis(), timeZone));
    }

    @Override
    public Function get()
    {
      return new DateTimeFunc();
    }
  }

  class DateTimeMillisFunc extends DateTimeInput
  {
    @Override
    public String name()
    {
      return "datetime_millis";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    protected final ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }

    @Override
    public Function get()
    {
      return new DateTimeMillisFunc();
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

  // string/long to string
  class TimeFormatFunc extends DateTimeInput
  {
    private JodaUtils.OutputFormatter outputFormat;

    // cached
    private long prevTime = -1;
    private String prevValue;

    @Override
    public String name()
    {
      return "time_format";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.STRING;
    }

    @Override
    protected void initialize(List<Expr> args, Map<String, Expr> params, Expr.NumericBinding bindings)
    {
      super.initialize(args, params, bindings);
      String format = Evals.evalOptionalString(params.get("out.format"), bindings);
      String locale = Evals.evalOptionalString(params.get("out.locale"), bindings);
      String timezone = Evals.evalOptionalString(params.get("out.timezone"), bindings);

      outputFormat = JodaUtils.toOutFormatter(format, locale, timezone);
    }

    @Override
    protected final ExprEval toValue(DateTime date)
    {
      if (prevValue == null || date.getMillis() != prevTime) {
        prevTime = date.getMillis();
        prevValue = outputFormat.format(date);
      }
      return ExprEval.of(prevValue);
    }

    @Override
    public Function get()
    {
      return new TimeFormatFunc();
    }
  }

  class DateTimeExtractFunc extends Function.NewInstance implements Function
  {
    private Unit unit;
    private DateTimeZone timeZone;

    @Override
    public String name()
    {
      return "datetime_extract";
    }

    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() < 2 || args.size() > 3) {
        throw new IAE("Function[%s] must have 2 to 3 arguments", name());
      }

      if (unit == null) {
        if (!Evals.isConstantString(args.get(0)) || Evals.getConstant(args.get(0)) == null) {
          throw new IAE("Function[%s] unit arg must be literal", name());
        }

        if (args.size() > 2 && !Evals.isConstantString(args.get(2))) {
          throw new IAE("Function[%s] timezone arg must be literal", name());
        }

        unit = Unit.valueOf(StringUtils.toUpperCase(Evals.getConstantString(args.get(0))));
        if (args.size() > 2) {
          timeZone = JodaUtils.toTimeZone(Evals.getConstantString(args.get(2)));
        }
      }

      final DateTime dateTime = Evals.toDateTime(args.get(1).eval(bindings), timeZone);

      switch (unit) {
        case EPOCH:
          return ExprEval.of(dateTime.getMillis());
        case SECOND:
          return ExprEval.of(dateTime.secondOfMinute().get());
        case MINUTE:
          return ExprEval.of(dateTime.minuteOfHour().get());
        case HOUR:
          return ExprEval.of(dateTime.hourOfDay().get());
        case DAY:
          return ExprEval.of(dateTime.dayOfMonth().get());
        case DOW:
          return ExprEval.of(dateTime.dayOfWeek().get());
        case DOY:
          return ExprEval.of(dateTime.dayOfYear().get());
        case WEEK:
          return ExprEval.of(dateTime.weekOfWeekyear().get());
        case MONTH:
          return ExprEval.of(dateTime.monthOfYear().get());
        case QUARTER:
          return ExprEval.of((dateTime.monthOfYear().get() - 1) / 3 + 1);
        case YEAR:
          return ExprEval.of(dateTime.year().get());
        default:
          throw new ISE("Unhandled unit[%s]", unit);
      }
    }
  }
}
