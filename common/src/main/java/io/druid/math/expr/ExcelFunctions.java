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

import io.druid.java.util.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr.NumericBinding;
import org.apache.poi.ss.formula.functions.Finance;
import org.apache.poi.ss.formula.functions.FinanceLib;
import org.apache.poi.ss.formula.functions.Irr;

import java.util.Arrays;
import java.util.List;

/**
 */
public interface ExcelFunctions extends Function.Library
{
  // fv(double r, double n, double y, double p, boolean t = false)
  @Function.Named("fv")
  class ExcelFV extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 4) {
        throw new IAE("function 'fv' needs at least 4 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              FinanceLib.fv(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asDouble(),
                  args.get(2).eval(bindings).asDouble(),
                  args.get(3).eval(bindings).asDouble(),
                  args.size() > 4 && args.get(4).eval(bindings).asBoolean()
              )
          );
        }
      };
    }
  }

  // pv(double r, double n, double y, double f, boolean t = false)
  @Function.Named("pv")
  class ExcelPV extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 4) {
        throw new IAE("function 'pv' needs at least 4 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              FinanceLib.pv(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asDouble(),
                  args.get(2).eval(bindings).asDouble(),
                  args.get(3).eval(bindings).asDouble(),
                  args.size() > 4 && args.get(4).eval(bindings).asBoolean()
              )
          );
        }
      };
    }
  }

  // nper(double r, double y, double p, double f, boolean t = false)
  @Function.Named("nper")
  class ExcelNPER extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 4) {
        throw new IAE("function 'nper' needs at least 4 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              FinanceLib.nper(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asDouble(),
                  args.get(2).eval(bindings).asDouble(),
                  args.get(3).eval(bindings).asDouble(),
                  args.size() > 4 && args.get(4).eval(bindings).asBoolean()
              )
          );
        }
      };
    }
  }

  // pmt(double r, double n, double p, double f = 0D, boolean t = false)
  @Function.Named("pmt")
  class ExcelPMT extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 3) {
        throw new IAE("function 'nper' needs at least 3 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              FinanceLib.pmt(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asDouble(),
                  args.get(2).eval(bindings).asDouble(),
                  args.size() > 3 ? args.get(3).eval(bindings).asDouble() : 0D,
                  args.size() > 4 && args.get(4).eval(bindings).asBoolean()
              )
          );
        }
      };
    }
  }

  // ipmt(double r, int per, int nper, double pv, double fv = 0D, int type = 0)
  @Function.Named("ipmt")
  class ExcelIPMT extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 4) {
        throw new IAE("function 'ipmt' needs at least 4 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              Finance.ipmt(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asInt(),
                  args.get(2).eval(bindings).asInt(),
                  args.get(3).eval(bindings).asDouble(),
                  args.size() > 4 ? args.get(4).eval(bindings).asDouble() : 0D,
                  args.size() > 5 && args.get(5).eval(bindings).asBoolean() ? 1 : 0
              )
          );
        }
      };
    }
  }

  // ppmt(double r, int per, int nper, double pv, double fv = 0D, int type = 0)
  @Function.Named("ppmt")
  class ExcelPPMT extends Function.NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 4) {
        throw new IAE("function 'ppmt' needs at least 4 arguments");
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(
              Finance.ppmt(
                  args.get(0).eval(bindings).asDouble(),
                  args.get(1).eval(bindings).asInt(),
                  args.get(2).eval(bindings).asInt(),
                  args.get(3).eval(bindings).asDouble(),
                  args.size() > 4 ? args.get(4).eval(bindings).asDouble() : 0D,
                  args.size() > 5 && args.get(5).eval(bindings).asBoolean() ? 1 : 0
              )
          );
        }
      };
    }
  }

  // npv(double[] cfs, double discountRate)
  @Function.Named("$npv")
  class ExcelNPV extends BuiltinFunctions.WindowFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, Expr.WindowContext context)
    {
      return new NPVFunction(args, context);
    }

    private class NPVFunction extends WindowFunction
    {
      private final double discountRate;
      private double[] values;

      public NPVFunction(List<Expr> args, Expr.WindowContext context)
      {
        super(args, context);
        if (parameters.length == 0) {
          throw new RuntimeException("partition function '$npv' needs 1 more argument (discountRate)");
        }
        discountRate = ((Number) parameters[0]).doubleValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        if (values == null || values.length < context.size()) {
          values = new double[context.size()];
        }
        values[context.index()] = (Double) context.get(inputField);
        if (!context.hasMore()) {
          return ExprEval.of(FinanceLib.npv(discountRate, Arrays.copyOfRange(values, 0, context.size())));
        }
        return ExprEval.of(null, ValueDesc.DOUBLE);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }
  }

  // irr(double[] income) = irr(double[] income, 0.1D)
  @Function.Named("$irr")
  class ExcelIRR extends BuiltinFunctions.WindowFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, Expr.WindowContext context)
    {
      return new IRRFunction(args, context);
    }

    private class IRRFunction extends WindowFunction
    {
      private final double guess;
      private double[] values;

      public IRRFunction(List<Expr> args, Expr.WindowContext context)
      {
        super(args, context);
        guess = parameters.length > 0 ? ((Number) parameters[0]).doubleValue() : 0.1D;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        if (values == null || values.length < context.size()) {
          values = new double[context.size()];
        }
        values[context.index()] = (Double) context.get(inputField);
        if (!context.hasMore()) {
          return ExprEval.of(Irr.irr(Arrays.copyOfRange(values, 0, context.size()), guess));
        }
        return ExprEval.of(null, ValueDesc.DOUBLE);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }
  }

  // mirr(double[] in, double financeRate, double reinvestRate)
  @Function.Named("$mirr")
  class ExcelMIRR extends BuiltinFunctions.WindowFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, Expr.WindowContext context)
    {
      if (args.size() < 3) {
        throw new RuntimeException("partition function '$mirr' needs 3 (fieldName, financeRate, reinvestRate)");
      }
      return new MIRRFunction(args, context);
    }

    private class MIRRFunction extends WindowFunction
    {
      private final double financeRate;
      private final double reinvestRate;
      private double[] values;

      public MIRRFunction(List<Expr> args, Expr.WindowContext context)
      {
        super(args, context);
        financeRate = ((Number) parameters[0]).doubleValue();
        reinvestRate = ((Number) parameters[1]).doubleValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        if (values == null || values.length < context.size()) {
          values = new double[context.size()];
        }
        values[context.index()] = (Double) context.get(inputField);
        if (!context.hasMore()) {
          return ExprEval.of(mirr(Arrays.copyOfRange(values, 0, context.size()), financeRate, reinvestRate));
        }
        return ExprEval.of(null, ValueDesc.DOUBLE);
      }

      // copied from org.apache.poi.ss.formula.functions.Mirr (private method)
      private double mirr(double[] in, double financeRate, double reinvestRate)
      {
        double value = 0;
        int numOfYears = in.length - 1;
        double pv = 0;
        double fv = 0;

        int indexN = 0;
        for (double anIn : in) {
          if (anIn < 0) {
            pv += anIn / Math.pow(1 + financeRate + reinvestRate, indexN++);
          }
        }

        for (double anIn : in) {
          if (anIn > 0) {
            fv += anIn * Math.pow(1 + financeRate, numOfYears - indexN++);
          }
        }

        if (fv != 0 && pv != 0) {
          value = Math.pow(-fv / pv, 1d / numOfYears) - 1;
        }
        return value;
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }
    }
  }
}
