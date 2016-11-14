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
  class ExcelFV implements Function
  {
    @Override
    public String name()
    {
      return "fv";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 4) {
        throw new RuntimeException("function 'fv' needs at least 4 arguments");
      }
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
  }

  // pv(double r, double n, double y, double f, boolean t = false)
  class ExcelPV implements Function
  {
    @Override
    public String name()
    {
      return "pv";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 4) {
        throw new RuntimeException("function 'pv' needs at least 4 arguments");
      }
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
  }

  // nper(double r, double y, double p, double f, boolean t = false)
  class ExcelNPER implements Function
  {
    @Override
    public String name()
    {
      return "nper";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 5) {
        throw new RuntimeException("function 'nper' at least 4 arguments");
      }
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
  }

  // npv(double[] cfs, double discountRate)
  class ExcelNPV extends BuiltinFunctions.PartitionFunction
  {
    private double discountRate;
    private double[] values;

    @Override
    public String name()
    {
      return "$npv";
    }

    @Override
    protected void initialize(Expr.WindowContext context, Object[] parameters)
    {
      if (parameters.length == 0) {
        throw new RuntimeException("partition function '$npv' needs 1 more argument (discountRate)");
      }
      discountRate = ((Number)parameters[0]).doubleValue();
    }

    @Override
    protected Object invoke(Expr.WindowContext context)
    {
      if (values == null || values.length < context.size()) {
        values = new double[context.size()];
      }
      values[context.index()] = (Double)context.get(fieldName);
      if (context.index() == context.size() - 1) {
        return FinanceLib.npv(discountRate, Arrays.copyOfRange(values, 0, context.size()));
      }
      return null;
    }

    @Override
    public Function get()
    {
      return new ExcelNPV();
    }
  }

  // pmt(double r, double n, double p, double f = 0D, boolean t = false)
  class ExcelPMT implements Function
  {
    @Override
    public String name()
    {
      return "pmt";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'pmt' needs at least 3 arguments");
      }
      return ExprEval.of(
          FinanceLib.pmt(
              args.get(0).eval(bindings).asDouble(),
              args.get(1).eval(bindings).asDouble(),
              args.get(2).eval(bindings).asDouble(),
              args.size() > 3 ? args.get(3).eval(bindings).asDouble() : 0D,
              args.size() > 4 ? args.get(4).eval(bindings).asBoolean() : false
          )
      );
    }
  }

  // ipmt(double r, int per, int nper, double pv, double fv = 0D, int type = 0)
  class ExcelIPMT implements Function
  {
    @Override
    public String name()
    {
      return "ipmt";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 4) {
        throw new RuntimeException("function 'ipmt' needs at least 4 arguments");
      }
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
  }

  // ppmt(double r, int per, int nper, double pv, double fv = 0D, int type = 0)
  class ExcelPPMT implements Function
  {
    @Override
    public String name()
    {
      return "ppmt";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 4) {
        throw new RuntimeException("function 'ppmt' needs at least 4 arguments");
      }
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
  }

  // irr(double[] income) = irr(double[] income, 0.1D)
  class ExcelIRR extends BuiltinFunctions.PartitionFunction
  {
    private double guess = 0.1D;
    private double[] values;

    @Override
    public String name()
    {
      return "$irr";
    }

    @Override
    protected void initialize(Expr.WindowContext context, Object[] parameters)
    {
      if (parameters.length > 0) {
        guess = ((Number)parameters[0]).doubleValue();
      }
    }

    @Override
    protected Object invoke(Expr.WindowContext context)
    {
      if (values == null || values.length < context.size()) {
        values = new double[context.size()];
      }
      values[context.index()] = (Double)context.get(fieldName);
      if (context.index() == context.size() - 1) {
        return Irr.irr(Arrays.copyOfRange(values, 0, context.size()), guess);
      }
      return null;
    }

    @Override
    public Function get()
    {
      return new ExcelIRR();
    }
  }

  // mirr(double[] in, double financeRate, double reinvestRate)
  class ExcelMIRR extends BuiltinFunctions.PartitionFunction
  {
    private double financeRate;
    private double reinvestRate;
    private double[] values;

    @Override
    public String name()
    {
      return "$mirr";
    }

    @Override
    protected void initialize(Expr.WindowContext context, Object[] parameters)
    {
      if (parameters.length != 2) {
        throw new RuntimeException("partition function '$mirr' needs 2 more arguments (financeRate and reinvestRate)");
      }
      financeRate = ((Number)parameters[0]).doubleValue();
      reinvestRate = ((Number)parameters[1]).doubleValue();
    }

    @Override
    protected Object invoke(Expr.WindowContext context)
    {
      if (values == null || values.length < context.size()) {
        values = new double[context.size()];
      }
      values[context.index()] = (Double)context.get(fieldName);
      if (context.index() == context.size() - 1) {
        return mirr(Arrays.copyOfRange(values, 0, context.size()), financeRate, reinvestRate);
      }
      return null;
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
    public Function get()
    {
      return new ExcelMIRR();
    }
  }
}
