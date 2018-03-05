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

import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.common.DateTimes;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
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
import java.util.Locale;
import java.util.Map;

/**
 */
public interface DateTimeFunctions extends Function.Library
{
  @Function.Named("now")
  class Now extends Function.LongOut
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      return ExprEval.of(System.currentTimeMillis());
    }
  }

  @Function.Named("current_time")
  class CurrentTime extends Function.DateTimeOut
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      return ExprEval.of(new DateTime(System.currentTimeMillis()));
    }
  }

  @Function.Named("recent")
  class Recent extends Function.IndecisiveOut
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs one or two arguments");
      }
      return ExprEval.of(toInterval(args, bindings), ExprType.UNKNOWN);
    }

    public Interval toInterval(List<Expr> args, Expr.NumericBinding bindings)
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

  public abstract static class GranularFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new IllegalArgumentException("function '" + name() + "' needs two arguments");
      }
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
  public static class BucketStart extends GranularFunc
  {
    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketStart(DateTimes.utc(param.asLong())).getMillis());
        }
      };
    }
  }

  @Function.Named("bucketEnd")
  public static class BucketEnd extends GranularFunc
  {
    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketEnd(DateTimes.utc(param.asLong())).getMillis());
        }
      };
    }
  }

  @Function.Named("bucketStartDateTime")
  public static class BucketStartDT extends GranularFunc
  {
    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new Child()
      {
        @Override
        public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
        {
          return ExprType.DATETIME;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketStart(param.asDateTime()));
        }
      };
    }
  }

  @Function.Named("bucketEndDateTime")
  public static class BucketEndDT extends GranularFunc
  {
    @Override
    protected Function newInstance(final Granularity granularity)
    {
      return new Child()
      {
        @Override
        public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
        {
          return ExprType.DATETIME;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval param = args.get(0).eval(bindings);
          return ExprEval.of(granularity.bucketEnd(param.asDateTime()));
        }
      };
    }
  }

  abstract class UnaryTimeMath extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 1 && args.size() != 2 && args.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs one to three arguments");
      }
      final DateTimeZone timeZone = args.size() > 1 ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(1))) : null;
      final Locale locale = args.size() > 2 ? JodaUtils.toLocale(Evals.getConstantString(args.get(2))) : null;
      return evaluate(timeZone, locale);
    }

    protected abstract Function evaluate(DateTimeZone timeZone, Locale locale);
  }

  abstract class BinaryTimeMath extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IllegalArgumentException("function '" + name() + "' needs two or three arguments");
      }
      final DateTimeZone timeZone = args.size() == 3
                                    ? JodaUtils.toTimeZone(Evals.getConstantString(args.get(2)))
                                    : null;
      return new Child()
      {
        @Override
        public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
        {
          return ExprType.DATETIME;
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime base = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          Period period = JodaUtils.toPeriod(Evals.getConstantString(args.get(1)));
          return evaluate(base, period);
        }
      };
    }

    protected abstract ExprEval evaluate(DateTime base, Period period);
  }

  @Function.Named("add_time")
  class AddTime extends BinaryTimeMath
  {
    @Override
    protected final ExprEval evaluate(DateTime base, Period period)
    {
      return ExprEval.of(base.plus(period));
    }
  }

  @Function.Named("sub_time")
  class SubTime extends BinaryTimeMath
  {
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
  @Function.Named("difftime")
  class DiffTime extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 3 && args.size() != 4) {
        throw new IllegalArgumentException("function '" + name() + "' needs three or four arguments");
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
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
            case EPOCH:
              return ExprEval.of(new Duration(time1, time2).getMillis());
            default:
              throw new IllegalArgumentException("invalid time unit " + unit);
          }
        }
      };
    }
  }

  @Function.Named("dayname")
  final class DayName extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new StringChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.dayOfWeek().getAsText(locale));
        }
      };
    }
  }

  @Function.Named("dayofmonth")
  class DayOfMonth extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getDayOfMonth());
        }
      };
    }
  }

  @Function.Named("lastdayofmonth")
  class LastDayOfMonth extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.dayOfMonth().getMaximumValue());
        }
      };
    }
  }

  @Function.Named("dayofweek")
  class DayOfWeek extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getDayOfWeek());
        }
      };
    }
  }

  @Function.Named("dayofyear")
  class DayOfYear extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getDayOfYear());
        }
      };
    }
  }

  @Function.Named("hour")
  class Hour extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getHourOfDay());
        }
      };
    }
  }

  @Function.Named("month")
  class Month extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getMonthOfYear());
        }
      };
    }
  }

  @Function.Named("monthname")
  class MonthName extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new StringChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.monthOfYear().getAsText(locale));
        }
      };
    }
  }

  @Function.Named("year")
  class Year extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.getYear());
        }
      };
    }
  }

  @Function.Named("first_day")
  class FirstDay extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.dayOfMonth().withMinimumValue());
        }
      };
    }
  }

  @Function.Named("last_day")
  class LastDay extends UnaryTimeMath
  {
    @Override
    protected Function evaluate(final DateTimeZone timeZone, final Locale locale)
    {
      return new LongChild() {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime time = Evals.toDateTime(args.get(0).eval(bindings), timeZone);
          return ExprEval.of(time.dayOfMonth().withMaximumValue());
        }
      };
    }
  }

  abstract class DateTimeParser extends BuiltinFunctions.NamedParams
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
        public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
        {
          return eval(args, bindings);
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval value = args.get(0).eval(bindings);
          try {
            return eval(Evals.toDateTime(value, formatter));
          }
          catch (Exception e) {
            return failed(e);
          }
        }
      };
    }

    protected abstract ExprType eval(List<Expr> args, Expr.TypeBinding bindings);

    protected abstract ExprEval eval(DateTime date);

    protected ExprEval failed(Exception ex)
    {
      throw Throwables.propagate(ex);
    }
  }

  // string/long to long
  @Function.Named("timestamp")
  class TimestampFromEpochFunc extends DateTimeInput
  {
    @Override
    public ExprType eval(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis(), ExprType.LONG);
    }
  }

  @Function.Named("unix_timestamp")
  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    protected final ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis() / 1000, ExprType.LONG);
    }
  }

  @Function.Named("datetime")
  class DateTimeFunc extends DateTimeParser
  {
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
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime dateTime = Evals.toDateTime(args.get(0).eval(bindings), formatter);
          return ExprEval.of(timeZone == null ? dateTime : new DateTime(dateTime.getMillis(), timeZone));
        }
      };
    }
  }

  @Function.Named("datetime_millis")
  class DateTimeMillisFunc extends DateTimeInput
  {
    @Override
    public ExprType eval(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }

    @Override
    protected final ExprEval eval(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }
  }

  @Function.Named("timestamp_validate")
  class TimestampValidateFunc extends TimestampFromEpochFunc
  {
    @Override
    protected ExprEval eval(DateTime date)
    {
      return ExprEval.of(true);
    }

    @Override
    protected final ExprEval failed(Exception ex)
    {
      return ExprEval.of(false);
    }
  }

  // string/long to string
  @Function.Named("time_format")
  class TimeFormatFunc extends DateTimeParser
  {
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
        private long prevTime = -1;
        private String prevValue;
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          DateTime dateTime = Evals.toDateTime(args.get(0).eval(bindings), formatter);
          if (prevValue == null || dateTime.getMillis() != prevTime) {
            prevTime = dateTime.getMillis();
            prevValue = outputFormat.print(dateTime);
          }
          return ExprEval.of(prevValue);
        }
      };
    }
  }

  @Function.Named("datetime_extract")
  class DateTimeExtractFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
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
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
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
      };
    }
  }
}
