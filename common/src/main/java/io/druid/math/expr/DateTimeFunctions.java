/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.granularity.GranularityType;
import io.druid.granularity.PeriodGranularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Function.NamedFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
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
import java.util.Locale;
import java.util.Map;

/**
 */
public interface DateTimeFunctions extends Function.Library
{
  abstract class TimeBetween extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      atLeastTwo(args);
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          try {
            final DateTime t1 = Evals.toDateTime(args.get(0).eval(bindings), (DateTimeZone) null);
            final DateTime t2 = Evals.toDateTime(args.get(1).eval(bindings), (DateTimeZone) null);
            return t1 == null || t2 == null ? ExprEval.NULL_LONG : ExprEval.of(eval(t1, t2));
          }
          catch (Exception e) {
            return ExprEval.NULL_LONG;
          }
        }
      };
    }

    protected abstract int eval(DateTime t1, DateTime t2);
  }

  @Function.Named("seconds_between")
  final class SecondsBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Seconds.secondsBetween(t1, t2).getSeconds();
    }
  }

  @Function.Named("minutes_between")
  final class MinutesBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Minutes.minutesBetween(t1, t2).getMinutes();
    }
  }

  @Function.Named("hours_between")
  final class HoursBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Hours.hoursBetween(t1, t2).getHours();
    }
  }

  @Function.Named("days_between")
  final class DaysBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Days.daysBetween(t1, t2).getDays();
    }
  }

  @Function.Named("datediff")
  final class DateDiff extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Days.daysBetween(t1, t2).getDays();
    }
  }

  @Function.Named("weeks_between")
  final class WeeksBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Weeks.weeksBetween(t1, t2).getWeeks();
    }
  }

  @Function.Named("months_between")
  final class MonthsBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Months.monthsBetween(t1, t2).getMonths();
    }
  }

  @Function.Named("years_between")
  final class YearsBetween extends TimeBetween
  {
    @Override
    protected final int eval(DateTime t1, DateTime t2)
    {
      return Years.yearsBetween(t1, t2).getYears();
    }
  }

  @Function.Named("now")
  final class Now extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(System.currentTimeMillis());
        }
      };
    }
  }

  @Function.Named("current_time")
  final class CurrentTime extends NamedFactory.DateTimeType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      return new DateTimeChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(DateTimes.nowUtc());
        }
      };
    }
  }

  @Function.Named("recent")
  class Recent extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.INTERVAL;
    }

    static Interval toInterval(List<Expr> args, Expr.NumericBinding bindings)
    {
      final DateTime now = DateTimes.nowUtc();
      final Period beforeNow = JodaUtils.toPeriod(args.get(0).eval(bindings).asString());
      if (args.size() == 1) {
        return new Interval(now.minus(beforeNow), now);
      }
      final Period afterNow = JodaUtils.toPeriod(args.get(1).eval(bindings).asString());
      return new Interval(now.minus(beforeNow), now.minus(afterNow));
    }

    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      oneOrTwo(args);
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.INTERVAL;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(toInterval(args, bindings));
        }
      };
    }
  }

  @Function.Named("interval")
  final class IntervalFunc extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.INTERVAL;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      exactTwo(args);
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.INTERVAL;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime from = Evals.eval(args.get(0), bindings).asDateTime();
          if (from == null) {
            from = DateTimes.MIN;
          }
          Object to = Evals.evalValue(args.get(1), bindings);
          if (to == null) {
            return ExprEval.of(new Interval(from, DateTimes.MAX));
          }
          if (to instanceof String && ((String) to).startsWith("P")) {
            return ExprEval.of(new Interval(from, new Period(to)));
          }
          return ExprEval.of(new Interval(from, Evals.toDateTime(to, null)));
        }
      };
    }
  }

  public abstract static class GranularFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      exactTwo(args);
      String string = Evals.getConstantString(args.get(1));
      Granularity granularity;
      if (string.startsWith("P")) {
        granularity = new PeriodGranularity(Period.parse(string), null, null);
      } else {
        granularity = Granularity.fromString(string);
      }
      return newInstance(granularity);
    }

    protected abstract Function newInstance(Granularity granularity);
  }

  @Function.Named("bucketStart")
  public static class BucketStart extends GranularFunc implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketStart(DateTimes.utc(param.asLong())).getMillis());
        }
      };
    }
  }

  @Function.Named("bucketEnd")
  public static class BucketEnd extends GranularFunc implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketEnd(DateTimes.utc(param.asLong())).getMillis());
        }
      };
    }
  }

  @Function.Named("bucketStartDateTime")
  public static class BucketStartDT extends GranularFunc implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.DATETIME;
    }

    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new DateTimeChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final DateTime dateTime = args.get(0).eval(bindings).asDateTime();
          return ExprEval.of(dateTime == null ? null : granularity.bucketStart(dateTime), ValueDesc.DATETIME);
        }
      };
    }
  }

  @Function.Named("bucketEndDateTime")
  public static class BucketEndDT extends GranularFunc implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.DATETIME;
    }

    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new DateTimeChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final DateTime dateTime = args.get(0).eval(bindings).asDateTime();
          return ExprEval.of(dateTime == null ? null : granularity.bucketEnd(dateTime), ValueDesc.DATETIME);
        }
      };
    }
  }

  abstract class UnaryTimeMath extends NamedFactory
  {
    private static final ExprEval INVALID = ExprEval.of(-1, ValueDesc.LONG);

    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      if (args.size() != 1 && args.size() != 2 && args.size() != 3) {
        throw new IAE("function '%s' needs one to three arguments", name());
      }
      final DateTimeZone timeZone = args.size() > 1 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(1))) : null;
      final Locale locale = args.size() > 2 ? JodaUtils.toLocale(Evals.getConstantString(args.get(2))) : null;
      return evaluate(timeZone, locale);
    }

    protected abstract Function evaluate(DateTimeZone timeZone, Locale locale);

    public static abstract class LongType extends UnaryTimeMath implements Function.FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      protected final Function evaluate(final DateTimeZone timeZone, final Locale locale)
      {
        return new LongChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
          {
            final DateTime dateTime = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
            try {
              return dateTime == null ? INVALID : ExprEval.of(eval(dateTime));
            }
            catch (Exception e) {
              return INVALID;
            }
          }
        };
      }

      protected abstract int eval(DateTime dateTime);
    }

    public static abstract class StringType extends UnaryTimeMath implements Function.FixedTyped
    {
      @Override
      public final ValueDesc returns()
      {
        return ValueDesc.STRING;
      }

      @Override
      protected final Function evaluate(final DateTimeZone timeZone, final Locale locale)
      {
        return new StringChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
          {
            final DateTime dateTime = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
            try {
              return dateTime == null ? ExprEval.NULL_STRING : ExprEval.of(eval(dateTime, locale));
            }
            catch (Exception e) {
              return ExprEval.NULL_STRING;
            }
          }
        };
      }

      protected abstract String eval(DateTime dateTime, Locale locale);
    }
  }

  abstract class BinaryTimeMath extends NamedFactory.DateTimeType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      twoOrThree(args);
      final DateTimeZone timeZone = args.size() == 3
                                    ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(2)))
                                    : null;
      final Period period = JodaUtils.toPeriod(Evals.getConstantString(args.get(1)));
      return new DateTimeChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final DateTime base = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return base == null ? ExprEval.NULL_DATETIME : eval(base, period);
        }
      };
    }

    protected abstract ExprEval eval(DateTime base, Period period);
  }

  @Function.Named("add_time")
  final class AddTime extends BinaryTimeMath
  {
    @Override
    protected final ExprEval eval(DateTime base, Period period)
    {
      return ExprEval.of(base.plus(period));
    }
  }

  @Function.Named("sub_time")
  final class SubTime extends BinaryTimeMath
  {
    @Override
    protected final ExprEval eval(DateTime base, Period period)
    {
      return ExprEval.of(base.minus(period));
    }
  }

  enum Unit
  {
    MILLIS, EPOCH, SECOND, MINUTE, HOUR, DAY, WEEK, WEEKOFWEEKYEAR, MONTH, YEAR, WEEKYEAR, DOW, DOY, QUARTER;

    public Period asPeriod()
    {
      GranularityType type = GranularityType.of(name());
      return type == null ? null : type.getPeriod();
    }
  }

  // whole time based
  @Function.Named("difftime")
  final class DiffTime extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      if (args.size() != 3 && args.size() != 4) {
        throw new IAE("function '%s' needs three or four arguments", name());
      }
      final Unit unit;
      Expr param = args.get(0);
      if (Evals.isConstantString(param)) {
        unit = Unit.valueOf(Evals.getConstantString(param).toUpperCase());
      } else {
        unit = Unit.values()[Evals.getConstantInt(param)];
      }
      final DateTimeZone timeZone =
          args.size() == 4 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(3))) : null;
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
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
              return ExprEval.of(new Duration(time1, time2).getMillis());
            case EPOCH:
              return ExprEval.of(new Duration(time1, time2).getMillis() / 1000);
            default:
              throw new IAE("invalid time unit %s", unit);
          }
        }
      };
    }
  }

  @Function.Named("dayname")
  final class DayName extends UnaryTimeMath.StringType
  {
    @Override
    protected String eval(final DateTime dateTime, final Locale locale)
    {
      return dateTime.dayOfWeek().getAsText(locale);
    }
  }

  @Function.Named("dayofmonth")
  final class DayOfMonth extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getDayOfMonth();
    }
  }

  @Function.Named("lastdayofmonth")
  final class LastDayOfMonth extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.dayOfMonth().getMaximumValue();
    }
  }

  @Function.Named("dayofweek")
  final class DayOfWeek extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getDayOfWeek();
    }
  }

  @Function.Named("dayofyear")
  final class DayOfYear extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getDayOfYear();
    }
  }

  @Function.Named("weekofweekyear")
  final class WeekOfWeekYear extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getWeekOfWeekyear();
    }
  }

  @Function.Named("weekyear")
  final class WeekYear extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getWeekyear();
    }
  }

  @Function.Named("hour")
  class Hour extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getHourOfDay();
    }
  }

  @Function.Named("month")
  final class Month extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getMonthOfYear();
    }
  }

  @Function.Named("monthname")
  final class MonthName extends UnaryTimeMath.StringType
  {
    @Override
    protected String eval(DateTime dateTime, Locale locale)
    {
      return dateTime.monthOfYear().getAsText(locale);
    }
  }

  @Function.Named("year")
  final class Year extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.getYear();
    }
  }

  @Function.Named("first_day")
  final class FirstDay extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.dayOfMonth().withMinimumValue().getDayOfMonth();
    }
  }

  @Function.Named("last_day")
  final class LastDay extends UnaryTimeMath.LongType
  {
    @Override
    protected int eval(DateTime dateTime)
    {
      return dateTime.dayOfMonth().withMaximumValue().getDayOfMonth();
    }
  }

  abstract class DateTimeParser extends BuiltinFunctions.ParameterizingNamedParams
  {
    @Override
    protected Map<String, Object> parameterize(List<Expr> exprs, final Map<String, ExprEval> namedParam)
    {
      Map<String, Object> parameter = super.parameterize(exprs, namedParam);
      int index = 1;
      ExprEval formatEval = namedParam.get("format");
      if (formatEval == null && exprs.size() > index && Evals.isConstant(exprs.get(index))) {
        formatEval = Evals.getConstantEval(exprs.get(index++));
      }
      ExprEval timezoneEval = namedParam.get("timezone");
      if (timezoneEval == null && exprs.size() > index && Evals.isConstant(exprs.get(index))) {
        timezoneEval = Evals.getConstantEval(exprs.get(index++));
      }
      ExprEval localeEval = namedParam.get("locale");
      if (localeEval == null && exprs.size() > index && Evals.isConstant(exprs.get(index))) {
        localeEval = Evals.getConstantEval(exprs.get(index));
      }
      String format = formatEval == null ? null : formatEval.asString();
      String locale = localeEval == null ? null : localeEval.asString();
      String timezone = timezoneEval == null ? null : timezoneEval.asString();

      DateTimeFormatter formatter =
          format == null && locale == null && timezone == null ? JodaUtils.STANDARD_PARSER :
          JodaUtils.toTimeFormatter(format, timezone, locale);

      parameter.put("formatter", formatter);
      parameter.put("format", format);
      parameter.put("locale", locale);
      parameter.put("timezone", timezone);

      return parameter;
    }
  }

  abstract class DateTimeInput extends DateTimeParser
  {
    @Override
    protected Function toFunction(final Map<String, Object> parameter)
    {
      final DateTimeFormatter formatter = (DateTimeFormatter) parameter.get("formatter");
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return DateTimeInput.this.returns();
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval value = args.get(0).eval(bindings);
          final DateTime dateTime = Evals.toDateTime(value, formatter);
          try {
            return dateTime == null ? invalid(value) : eval(dateTime);
          }
          catch (Exception e) {
            return invalid(value);
          }
        }
      };
    }

    protected abstract ValueDesc returns();

    protected abstract ExprEval eval(DateTime date);

    protected ExprEval invalid(ExprEval value)
    {
      return ExprEval.of(null, value.type());
    }

    static abstract class LongType extends DateTimeInput
    {
      @Override
      protected final ValueDesc returns() { return ValueDesc.LONG;}
    }

    static abstract class BooleanType extends DateTimeInput
    {
      @Override
      protected final ValueDesc returns() { return ValueDesc.BOOLEAN;}
    }
  }

  // string/long to long
  @Function.Named("timestamp")
  class TimestampFromEpochFunc extends DateTimeInput.LongType
  {
    @Override
    protected ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }
  }

  @Function.Named("unix_timestamp")
  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    protected final ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis() / 1000);
    }
  }

  @Function.Named("datetime")
  class DateTimeFunc extends DateTimeParser implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.DATETIME;
    }

    @Override
    protected Map<String, Object> parameterize(List<Expr> exprs, final Map<String, ExprEval> namedParam)
    {
      Map<String, Object> parameter = super.parameterize(exprs, namedParam);
      DateTimeFormatter formatter = (DateTimeFormatter) parameter.get("formatter");
      String timezone = getString(namedParam, "out.timezone", parameter.get("timezone"));
      if (timezone != null) {
        DateTimeZone timeZone = JodaUtils.toTimeZone(timezone);
        if (!timeZone.equals(formatter.getZone())) {
          parameter.put("out.timezone", timeZone);
        }
      }
      return parameter;
    }

    @Override
    protected Function toFunction(Map<String, Object> parameter)
    {
      final DateTimeFormatter formatter = (DateTimeFormatter) parameter.get("formatter");
      final DateTimeZone timeZone = (DateTimeZone) parameter.get("out.timezone");
      return new DateTimeChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final DateTime dateTime = Evals.toDateTime(args.get(0).eval(bindings), formatter);
          if (dateTime == null) {
            return ExprEval.NULL_DATETIME;
          }
          return ExprEval.of(timeZone == null ? dateTime : new DateTime(dateTime.getMillis(), timeZone));
        }
      };
    }
  }

  @Function.Named("datetime_millis")
  class DateTimeMillisFunc extends DateTimeInput.LongType
  {
    @Override
    protected final ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }
  }

  @Function.Named("timestamp_validate")
  class TimestampValidateFunc extends DateTimeInput.BooleanType
  {
    @Override
    protected ExprEval eval(DateTime date)
    {
      return ExprEval.of(true);
    }

    @Override
    protected ExprEval invalid(ExprEval value)
    {
      return ExprEval.of(false);
    }
  }

  // string/long to string
  @Function.Named("time_format")
  class TimeFormatFunc extends DateTimeParser implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.STRING;
    }

    @Override
    protected Map<String, Object> parameterize(List<Expr> exprs, final Map<String, ExprEval> namedParam)
    {
      Map<String, Object> parameter = super.parameterize(exprs, namedParam);

      String format = getString(namedParam, "out.format", parameter.get("format"));
      String locale = getString(namedParam, "out.locale", parameter.get("locale"));
      String timezone = getString(namedParam, "out.timezone", parameter.get("timezone"));
      parameter.put("output.formatter", JodaUtils.toTimeFormatter(format, timezone, locale));

      return parameter;
    }

    @Override
    protected Function toFunction(Map<String, Object> parameter)
    {
      final DateTimeFormatter formatter = (DateTimeFormatter) parameter.get("formatter");
      final DateTimeFormatter outputFormat = (DateTimeFormatter) parameter.get("output.formatter");
      return new StringChild()
      {
        private StringBuilder builder = new StringBuilder();
        private long prevTime = -1;
        private String prevValue;

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = args.get(0).eval(bindings);
          if (!eval.isNull() && eval.isLong()) {
            // quick path for __time
            final long instant = eval.asLong();
            if (prevValue == null || instant != prevTime) {
              prevTime = instant;
              prevValue = JodaUtils.printTo(outputFormat, builder, instant);
            }
            return ExprEval.of(prevValue);
          }
          final DateTime dateTime = Evals.toDateTime(eval, formatter);
          if (dateTime == null) {
            return ExprEval.of((String) null);
          }
          if (prevValue == null || dateTime.getMillis() != prevTime) {
            prevTime = dateTime.getMillis();
            prevValue = JodaUtils.printTo(outputFormat, builder, dateTime);
          }
          return ExprEval.of(prevValue);
        }
      };
    }
  }

  @Function.Named("datetime_extract")
  class DateTimeExtractFunc extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver context)
    {
      if (args.size() < 2 || args.size() > 3) {
        throw new IAE("Function[%s] must have 2 to 3 arguments", name());
      }
      if (!Evals.isConstantString(args.get(0)) || Evals.getConstant(args.get(0)) == null) {
        throw new IAE("Function[%s] unit arg must be literal", name());
      }

      if (args.size() > 2 && !Evals.isConstantString(args.get(2))) {
        throw new IAE("Function[%s] timezone arg must be literal", name());
      }

      final Unit unit = Unit.valueOf(StringUtils.toUpperCase(Evals.getConstantString(args.get(0))));
      final DateTimeZone timeZone = args.size() > 2 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(2))) : null;

      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval param = args.get(1).eval(bindings);
          final DateTime dateTime = Evals.toDateTime(param, timeZone);
          if (dateTime == null) {
            if (unit == Unit.MINUTE || unit == Unit.EPOCH) {
              throw new IAE("Invalid value %s", param.value());
            }
            return ExprEval.of(-1);
          }

          switch (unit) {
            case MILLIS:
              return ExprEval.of(dateTime.getMillis());
            case EPOCH:
              return ExprEval.of(dateTime.getMillis() / 1000);
            case SECOND:
              return ExprEval.of(dateTime.getSecondOfMinute());
            case MINUTE:
              return ExprEval.of(dateTime.getMinuteOfHour());
            case HOUR:
              return ExprEval.of(dateTime.getHourOfDay());
            case DAY:
              return ExprEval.of(dateTime.getDayOfMonth());
            case DOW:
              return ExprEval.of(dateTime.getDayOfWeek());
            case DOY:
              return ExprEval.of(dateTime.getDayOfYear());
            case WEEK:
              if (dateTime.getWeekyear() != dateTime.getYear()) {
                return ExprEval.of(1);
              }
              // wish it's ISOChronology
              DateTime firstDay = dateTime.withDate(dateTime.getYear(), DateTimeConstants.JANUARY, 1);
              if (firstDay.getDayOfWeek() >= DateTimeConstants.FRIDAY) {
                return ExprEval.of(dateTime.getWeekOfWeekyear() + 1);
              }
              return ExprEval.of(dateTime.getWeekOfWeekyear());
            case WEEKOFWEEKYEAR:
              return ExprEval.of(dateTime.getWeekOfWeekyear());
            case MONTH:
              return ExprEval.of(dateTime.getMonthOfYear());
            case QUARTER:
              return ExprEval.of((dateTime.getMonthOfYear() - 1) / 3 + 1);
            case YEAR:
              return ExprEval.of(dateTime.getYear());
            case WEEKYEAR:
              return ExprEval.of(dateTime.getWeekyear());
            default:
              throw new ISE("Unhandled unit[%s]", unit);
          }
        }
      };
    }
  }
}
