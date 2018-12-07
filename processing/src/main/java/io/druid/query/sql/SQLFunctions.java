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

package io.druid.query.sql;

import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import io.druid.common.DateTimes;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.granularity.PeriodGranularity;
import io.druid.math.expr.DateTimeFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import org.joda.time.Chronology;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.List;

/**
 */
public interface SQLFunctions extends Function.Library
{
  @Function.Named("timestamp_ceil")
  class TimestampCeilExprMacro extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 2 || args.size() > 4) {
        throw new IAE("Function[%s] must have 2 to 4 arguments", name());
      }
      if (!Evals.isAllConstants(args.subList(1, args.size()))) {
        throw new IAE("granularity should be constant value", name());
      }
      final Granularity granularity = ExprUtils.toPeriodGranularity(args, 1);
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(granularity.bucketEnd(DateTimes.utc(args.get(0).eval(bindings).asLong())).getMillis());
        }
      };
    }
  }

  @Function.Named("timestamp_extract")
  class TimestampExtractFunc extends DateTimeFunctions.DateTimeExtractFunc
  {
    @Override
    public Function create(final List<Expr> args)
    {
      final Function function = super.create(args);
      final Function parameter = Evals.getFunction(args.get(1));
      if (parameter instanceof HoldingChild) {
        return new HoldingChild<Object>(((HoldingChild) parameter).getHolder())
        {
          @Override
          public ValueDesc apply(List<Expr> args, TypeResolver bindings)
          {
            return ValueDesc.LONG;
          }

          @Override
          public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
          {
            return function.apply(args, bindings);
          }
        };
      }
      return function;
    }
  }

  @Function.Named("timestamp_floor")
  class TimestampFloorExprMacro extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 2 || args.size() > 4) {
        throw new IAE("Function[%s] must have 2 to 4 arguments", name());
      }
      if (!Evals.isAllConstants(args.subList(1, args.size()))) {
        throw new IAE("granularity should be constant value", name());
      }
      final Expr timeParam = args.get(0);
      final PeriodGranularity granularity = ExprUtils.toPeriodGranularity(args, 1);
      if (Evals.isIdentifier(timeParam) && Row.TIME_COLUMN_NAME.equals(Evals.getIdentifier(timeParam))) {
        return new HoldingChild<PeriodGranularity>(granularity)
        {
          @Override
          public ValueDesc apply(List<Expr> args, TypeResolver bindings)
          {
            return ValueDesc.LONG;
          }

          @Override
          public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
          {
            return evaluate(args.get(0).eval(bindings), granularity);
          }
        };
      }
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return evaluate(args.get(0).eval(bindings), granularity);
        }
      };
    }

    private ExprEval evaluate(ExprEval timeParam, PeriodGranularity granularity)
    {
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(timeParam.asLong())).getMillis());
    }
  }

  @Function.Named("timestamp_format")
  public class TimestampFormatExprMacro extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 1 || args.size() > 3) {
        throw new IAE("Function[%s] must have 1 to 3 arguments", name());
      }

      final Expr arg = args.get(0);
      final String formatString;
      final DateTimeZone timeZone;

      if (args.size() > 1) {
        Preconditions.checkArgument(Evals.isConstant(args.get(1)), "Function[%s] format arg must be a literal", name());
        formatString = Evals.getConstantString(args.get(1));
      } else {
        formatString = null;
      }

      if (args.size() > 2) {
        timeZone = ExprUtils.toTimeZone(args.get(2));
      } else {
        timeZone = DateTimeZone.UTC;
      }

      final DateTimeFormatter formatter = formatString == null
                                          ? ISODateTimeFormat.dateTime()
                                          : DateTimeFormat.forPattern(formatString).withZone(timeZone);

      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(formatter.print(arg.eval(bindings).asLong()));
        }
      };
    }
  }

  @Function.Named("timestamp_parse")
  public class TimestampParseExprMacro extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 1 || args.size() > 3) {
        throw new IAE("Function[%s] must have 1 to 3 arguments", name());
      }

      final Expr arg = args.get(0);
      final String formatString = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
      final DateTimeZone timeZone;

      if (args.size() > 2) {
        timeZone = DateTimeZone.forID(Evals.getConstantString(args.get(2)));
      } else {
        timeZone = DateTimeZone.UTC;
      }

      final DateTimes.UtcFormatter formatter =
          StringUtils.isNullOrEmpty(formatString)
          ? DateTimes.ISO_DATE_OR_TIME
          : DateTimes.wrapFormatter(DateTimeFormat.forPattern(formatString).withZone(timeZone));

      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          final String value = arg.eval(bindings).asString();
          if (value == null) {
            return ExprEval.of(null, ValueDesc.LONG);
          }

          try {
            return ExprEval.of(formatter.parse(value).getMillis());
          }
          catch (IllegalArgumentException e) {
            // Catch exceptions potentially thrown by formatter.parseDateTime. Our docs say that unparseable timestamps
            // are returned as nulls.
            return ExprEval.of(null, ValueDesc.LONG);
          }
        }
      };
    }
  }

  @Function.Named("timestamp_shift")
  public class TimestampShiftExprMacro extends Function.AbstractFactory
  {
    @Override
    public Function create(final List<Expr> args)
    {
      if (args.size() < 3 || args.size() > 4) {
        throw new IAE("Function[%s] must have 3 to 4 arguments", name());
      }
      if (!Evals.isAllConstants(args.subList(1, args.size()))) {
        throw new IAE("granularity should be constant value", name());
      }
      final PeriodGranularity granularity = ExprUtils.toPeriodGranularity(
          Evals.getConstantString(args, 1), null, Evals.getConstantString(args, 3)
      );
      final Period period = granularity.getPeriod();
      final Chronology chronology = ISOChronology.getInstance(granularity.getTimeZone());
      final int step = Evals.getConstantInt(args.get(2));

      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(chronology.add(period, args.get(0).eval(bindings).asLong(), step));
        }
      };
    }

  }
}
