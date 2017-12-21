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

import com.google.common.base.Strings;
import com.metamx.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.PeriodGranularity;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ExprUtils
{
  private static final Expr.NumericBinding NIL_BINDINGS = new Expr.NumericBinding()
  {
    @Override
    public Collection<String> names()
    {
      return Collections.emptyList();
    }

    @Override
    public Object get(String name)
    {
      throw new IllegalArgumentException(name + " is not a constant");
    }
  };

  public static Expr.NumericBinding nilBindings()
  {
    return NIL_BINDINGS;
  }

  public static DateTimeZone toTimeZone(final Expr timeZoneArg)
  {
    if (!Evals.isConstant(timeZoneArg)) {
      throw new IAE("Time zone must be a literal");
    }

    final String literalValue = Evals.getConstantString(timeZoneArg);
    return literalValue == null ? DateTimeZone.UTC : DateTimeZone.forID(literalValue);
  }

  public static PeriodGranularity toPeriodGranularity(List<Expr> args, int index)
  {
    return ExprUtils.toPeriodGranularity(
          Evals.getConstantString(args, index++),
          Evals.getConstantString(args, index++),
          Evals.getConstantString(args, index)
      );
  }

  public static PeriodGranularity toPeriodGranularity(String periodArg, String originArg, String timeZoneArg)
  {
    Period period = new Period(periodArg);
    DateTimeZone timeZone = null;
    if (!Strings.isNullOrEmpty(timeZoneArg)) {
      timeZone = DateTimeZone.forID(timeZoneArg);
    }

    DateTime origin = null;
    if (!StringUtils.isNullOrEmpty(originArg)) {
      Chronology chronology = timeZone == null ? ISOChronology.getInstanceUTC() : ISOChronology.getInstance(timeZone);
      origin = new DateTime(originArg, chronology);
    }

    return new PeriodGranularity(period, origin, timeZone);
  }
}
