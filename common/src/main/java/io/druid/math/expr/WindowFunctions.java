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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Expr.WindowContext;
import io.druid.math.expr.Function.FixedTyped;
import io.druid.math.expr.Function.NamedFactory;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Variance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface WindowFunctions extends Function.Library
{
  abstract class Factory extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (!(resolver instanceof WindowContext)) {
        throw new ISE("window function '%s' needs window context", name());
      }
      return newInstance(args, (WindowContext) resolver);
    }

    protected abstract WindowFunction newInstance(List<Expr> args, WindowContext context);

    protected abstract static class WindowFunction implements Function
    {
      protected final WindowContext context;

      protected final Expr fieldExpr;
      protected final ValueDesc fieldType;
      protected final List<Expr> parameters;

      protected WindowFunction(List<Expr> args, WindowContext context)
      {
        this.context = context;
        if (!args.isEmpty()) {
          fieldExpr = args.get(0);
          fieldType = fieldExpr.returns();
          parameters = args.subList(1, args.size());
        } else {
          fieldExpr = Evals.identifierExpr("$$$", ValueDesc.UNKNOWN);
          fieldType = ValueDesc.UNKNOWN;
          parameters = ImmutableList.of();
        }
      }

      @Override
      public ValueDesc returns()
      {
        return fieldType;
      }

      protected void init() { }
    }
  }

  abstract class StatelessFactory extends Factory
  {
    private final ValueDesc outputType;

    public StatelessFactory()
    {
      this(null);
    }

    public StatelessFactory(ValueDesc outputType)
    {
      this.outputType = outputType;
    }

    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatelessWindowFunction(args, context);
    }

    protected final class StatelessWindowFunction extends WindowFunction
    {
      private StatelessWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return ExprEval.of(invoke(context, fieldExpr), returns());
      }

      @Override
      public ValueDesc returns()
      {
        return outputType != null ? outputType : fieldType;
      }
    }

    protected abstract ExprEval invoke(WindowContext context, Expr inputExpr);
  }

  abstract class FrameFunctionFactory extends Factory
  {
    protected abstract static class FrameFunction extends WindowFunction
    {
      protected final int[] window;

      protected FrameFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        if (parameters.size() >= 2) {
          final Object param1 = Evals.getConstant(parameters.get(parameters.size() - 2));
          final Object param2 = Evals.getConstant(parameters.get(parameters.size() - 1));
          window = new int[]{Integer.MIN_VALUE, 0};   // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          if (!"?".equals(param1)) {
            window[0] = ((Number) param1).intValue();
          }
          if (!"?".equals(param2)) {
            window[1] = ((Number) param2).intValue();
          }
        } else {
          window = null;
        }
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        if (window != null) {
          init();
          for (Object object : context.iterator(window[0], window[1], fieldExpr)) {
            if (object != null) {
              invoke(object, context);
            }
          }
        } else {
          Object current = Evals.evalValue(fieldExpr, context);
          if (current != null) {
            invoke(current, context);
          }
        }
        return current(context);
      }

      protected final int sizeOfWindow()
      {
        return window == null ? -1 : Math.abs(window[0] - window[1]) + 1;
      }

      protected abstract void invoke(Object current, WindowContext context);

      protected abstract ExprEval current(WindowContext context);
    }
  }

  @Function.Named("$prev")
  final class Prev extends StatelessFactory
  {
    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return context.evaluate(context.index() - 1, inputExpr);
    }
  }

  @Function.Named("$prevNotNull")
  final class PrevNotNull extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private ExprEval prevNotNull = ExprEval.nullOf(fieldType);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final ExprEval prev = context.evaluate(context.index() - 1, fieldExpr);
          if (prev.isNull()) {
            return prevNotNull;
          }
          return prevNotNull = prev;
        }
      };
    }
  }

  @Function.Named("$nvlPrev")
  final class NvlPrev extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private ExprEval prev = ExprEval.nullOf(fieldType);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final ExprEval current = context.evaluate(context.index(), fieldExpr);
          if (current.isNull() && !prev.isNull()) {
            return prev;
          }
          return prev = current;
        }
      };
    }
  }

  @Function.Named("$next")
  final class Next extends StatelessFactory
  {
    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return context.evaluate(context.index() + 1, inputExpr);
    }
  }

  @Function.Named("$nextNotNull")
  final class NextNotNull extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private int index;
        private ExprEval next = ExprEval.nullOf(fieldType);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          int ix = context.index();
          if (index > 0 && ix < index) {
            return next;
          }
          for (ix++; ix < context.size(); ix++) {
            ExprEval current = context.evaluate(ix, fieldExpr);
            if (!current.isNull()) {
              index = ix;
              return next = current;
            }
          }
          index = context.size();
          return next = ExprEval.nullOf(fieldType);
        }
      };
    }
  }

  @Function.Named("$nvlNext")
  final class NvlNext extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private int index;
        private ExprEval next = ExprEval.UNKNOWN;

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          int ix = context.index();
          if (index > 0 && ix < index) {
            return next;
          }
          ExprEval current = context.evaluate(ix++, fieldExpr);
          if (!current.isNull()) {
            return current;
          }
          for (; ix < context.size(); ix++) {
            current = context.evaluate(ix, fieldExpr);
            if (!current.isNull()) {
              index = ix;
              return next = current;
            }
          }
          index = context.size();
          return next = ExprEval.nullOf(current.type());
        }
      };
    }
  }

  @Function.Named("$last")
  final class Last extends StatelessFactory
  {
    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return context.evaluate(context.size() - 1, inputExpr);
    }
  }

  @Function.Named("$lastOf")
  final class LastOf extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private final Expr predicate = parameters.get(0);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          for (int i = context.size() - 1; i >= 0; i--) {
            if (context.evaluate(i, predicate).asBoolean()) {
              return context.evaluate(i, fieldExpr);
            }
          }
          return ExprEval.nullOf(fieldType);
        }
      };
    }
  }

  @Function.Named("$first")
  final class First extends StatelessFactory
  {
    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return context.evaluate(0, inputExpr);
    }
  }

  @Function.Named("$firstOf")
  final class FirstOf extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new WindowFunction(args, context)
      {
        private final Expr predicate = parameters.get(0);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final int size = context.size();
          for (int i = 0; i < size; i++) {
            if (context.evaluate(i, predicate).asBoolean()) {
              return context.evaluate(i, fieldExpr);
            }
          }
          return ExprEval.nullOf(fieldType);
        }
      };
    }
  }

  @Function.Named("$nth")
  final class Nth extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      atLeastTwo(args);
      return new NthWindowFunction(args, context);
    }

    protected static final class NthWindowFunction extends WindowFunction
    {
      private final int nth;

      private NthWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        nth = Evals.getConstantNumber(parameters.get(0)).intValue() - 1;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return context.evaluate(nth, fieldExpr);
      }
    }
  }

  @Function.Named("$lag")
  final class Lag extends Factory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      atLeastTwo(args);
      return new LagWindowFunction(args, context);
    }

    protected static final class LagWindowFunction extends WindowFunction
    {
      private final int delta;

      private LagWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        delta = Evals.getConstantNumber(parameters.get(0)).intValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return context.evaluate(context.index() - delta, fieldExpr);
      }
    }
  }

  @Function.Named("$lead")
  final class Lead extends Factory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      atLeastTwo(args);
      return new LeadWindowFunction(args, context);
    }

    protected static final class LeadWindowFunction extends WindowFunction
    {
      private final int delta;

      private LeadWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        delta = Evals.getConstantNumber(parameters.get(0)).intValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return context.evaluate(context.index() + delta, fieldExpr);
      }
    }
  }

  @Function.Named("$delta")
  final class RunningDelta extends Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      atLeastOne(args);
      return new DeltaWindowFunction(args, context);
    }

    protected static final class DeltaWindowFunction extends WindowFunction
    {
      private long longPrev;
      private float floatPrev;
      private double doublePrev;

      private DeltaWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected void init()
      {
        longPrev = 0;
        floatPrev = 0;
        doublePrev = 0;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        Object current = Evals.evalValue(fieldExpr, context);
        if (current == null) {
          return ExprEval.nullOf(fieldType);
        }
        if (context.index() == 0) {
          switch (fieldType.type()) {
            case LONG:
              longPrev = ((Number) current).longValue();
              return ExprEval.of(0L);
            case FLOAT:
              floatPrev = ((Number) current).floatValue();
              return ExprEval.of(0F);
            case DOUBLE:
              doublePrev = ((Number) current).doubleValue();
              return ExprEval.of(0D);
            default:
              throw new ISE("unsupported type %s", fieldType);
          }
        }
        switch (fieldType.type()) {
          case LONG:
            long currentLong = ((Number) current).longValue();
            long deltaLong = currentLong - longPrev;
            longPrev = currentLong;
            return ExprEval.of(deltaLong);
          case FLOAT:
            float currentFloat = ((Number) current).floatValue();
            float deltaFloat = currentFloat - floatPrev;
            floatPrev = currentFloat;
            return ExprEval.of(deltaFloat);
          case DOUBLE:
            double currentDouble = ((Number) current).doubleValue();
            double deltaDouble = currentDouble - doublePrev;
            doublePrev = currentDouble;
            return ExprEval.of(deltaDouble);
          default:
            throw new ISE("unsupported type %s", fieldType);
        }
      }
    }
  }

  @Function.Named("$count")
  class RunningCount extends FrameFunctionFactory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new FrameFunction(args, context)
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(window == null ? context.size() : context.size(window[0], window[1]));
        }

        @Override
        public ValueDesc returns()
        {
          return ValueDesc.LONG;
        }

        @Override
        protected void invoke(Object current, WindowContext context)
        {
          throw new ISE("invoke");
        }

        @Override
        protected ExprEval current(WindowContext context)
        {
          throw new ISE("current");
        }
      };
    }
  }

  @Function.Named("$sum")
  class RunningSum extends RunningSum0
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningSumFunction(args, context);
    }

    static class RunningSumFunction extends RunningSum0Function
    {
      int counter;

      protected RunningSumFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return fieldType.type() == ValueType.LONG ? ValueDesc.LONG : ValueDesc.DOUBLE;
      }

      @Override
      protected void init()
      {
        counter = 0;
        super.init();
      }

      protected void invoke(Object current, WindowContext context)
      {
        counter++;
        super.invoke(current, context);
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return counter == 0 ? ExprEval.nullOf(fieldType) : super.current(context);
      }
    }
  }

  @Function.Named("$sum0")
  class RunningSum0 extends FrameFunctionFactory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningSum0Function(args, context);
    }

    static class RunningSum0Function extends FrameFunction
    {
      private long longSum;
      private double doubleSum;

      protected RunningSum0Function(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return fieldType.type() == ValueType.LONG ? ValueDesc.LONG : ValueDesc.DOUBLE;
      }

      @Override
      protected void init()
      {
        longSum = 0;
        doubleSum = 0;
      }

      @Override
      protected void invoke(Object current, WindowContext context)
      {
        switch (fieldType.type()) {
          case LONG:
            longSum += ((Number) current).longValue();
            break;
          case FLOAT:
          case DOUBLE:
            doubleSum += ((Number) current).doubleValue();
            break;
          default:
            throw new ISE("unsupported type %s", fieldType);
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        if (fieldType.isLong()) {
          return ExprEval.of(longSum);
        } else {
          return ExprEval.of(doubleSum);
        }
      }
    }
  }

  @Function.Named("$min")
  final class RunningMin extends FrameFunctionFactory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMinFunction(args, context);
    }

    private static class RunningMinFunction extends FrameFunction
    {
      private Comparable prev;

      protected RunningMinFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      @SuppressWarnings("unchecked")
      protected void invoke(Object current, WindowContext context)
      {
        Comparable comparable = (Comparable) current;
        if (prev == null || comparable.compareTo(prev) < 0) {
          prev = comparable;
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(prev, fieldType);
      }

      @Override
      protected void init()
      {
        prev = null;
      }
    }
  }

  @Function.Named("$max")
  final class RunningMax extends FrameFunctionFactory implements Function.Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMaxFunction(args, context);
    }

    private static class RunningMaxFunction extends FrameFunction
    {
      private Comparable prev;

      protected RunningMaxFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      @SuppressWarnings("unchecked")
      protected void invoke(Object current, WindowContext context)
      {
        Comparable comparable = (Comparable) current;
        if (prev == null || comparable.compareTo(prev) > 0) {
          prev = comparable;
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(prev, fieldType);
      }

      @Override
      protected void init()
      {
        prev = null;
      }
    }
  }

  @Function.Named("$row_num")
  final class RowNum extends StatelessFactory implements FixedTyped.LongType
  {
    public RowNum()
    {
      super(ValueDesc.LONG);
    }

    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return ExprEval.of(context.index() + 1L);
    }
  }

  @Function.Named("$rank")
  final class Rank extends Factory implements FixedTyped.LongType
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RankFunction(args, context);
    }

    private static class RankFunction extends WindowFunction implements FixedTyped.LongType
    {
      private long prevRank;
      private Object prev;

      protected RankFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        Object current = Evals.evalValue(fieldExpr, context);
        if (context.index() == 0 || !Objects.equals(prev, current)) {
          prev = current;
          prevRank = context.index() + 1;
        }
        return ExprEval.of(prevRank);
      }

      @Override
      protected void init()
      {
        prevRank = 0L;
        prev = null;
      }
    }
  }

  @Function.Named("$dense_rank")
  final class DenseRank extends Factory implements FixedTyped.LongType
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new DenseRankFunction(args, context);
    }

    private static class DenseRankFunction extends WindowFunction implements FixedTyped.LongType
    {
      private long prevRank;
      private Object prev;

      protected DenseRankFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected void init()
      {
        prevRank = 0L;
        prev = null;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        final Object current = Evals.evalValue(fieldExpr, context);
        if (context.index() == 0 || !Objects.equals(prev, current)) {
          prev = current;
          prevRank++;
        }
        return ExprEval.of(prevRank);
      }
    }
  }

  @Function.Named("$mean")
  class RunningMean extends RunningSum implements FixedTyped.DoubleType
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMeanFunction(args, context);
    }

    static class RunningMeanFunction extends RunningSumFunction implements FixedTyped.DoubleType
    {
      protected RunningMeanFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(counter == 0 ? null : super.current(context).asDouble() / counter, ValueDesc.DOUBLE);
      }
    }
  }

  @Function.Named("$avg")
  class RunningAvg extends RunningMean
  {
  }

  abstract class RunningStats extends FrameFunctionFactory implements FixedTyped.DoubleType
  {
    static class StatsFunction extends FrameFunction implements FixedTyped.DoubleType
    {
      final StorelessUnivariateStatistic statistic;

      protected StatsFunction(List<Expr> args, WindowContext context, StorelessUnivariateStatistic statistic)
      {
        super(args, context);
        this.statistic = statistic;
      }

      @Override
      public void init()
      {
        statistic.clear();
      }

      @Override
      public void invoke(Object current, WindowContext context)
      {
        statistic.increment(((Number) current).doubleValue());
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        final double result = statistic.getResult();
        return Double.isNaN(result) ? ExprEval.NULL_DOUBLE : ExprEval.of(result, ValueDesc.DOUBLE);
      }
    }
  }

  @Function.Named("$variance")
  final class RunningVariance extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new Variance(true));
    }
  }

  @Function.Named("$stddev")
  final class RunningStandardDeviation extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new StandardDeviation(true));
    }
  }

  @Function.Named("$variancePop")
  final class RunningVariancePop extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new Variance(false));
    }
  }

  @Function.Named("$stddevPop")
  final class RunningStandardDeviationPop extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new StandardDeviation(false));
    }
  }

  @Function.Named("$kurtosis")
  final class RunningKurtosis extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new Kurtosis());
    }
  }

  @Function.Named("$skewness")
  final class RunningSkewness extends RunningStats
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StatsFunction(args, context, new Skewness());
    }
  }

  @Function.Named("$percentile")
  class RunningPercentile extends FrameFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new PercentileFunction(args, context)
      {
        private final float percentile;
        {
          this.percentile = Evals.getConstantNumber(parameters.get(0)).floatValue();
          if (percentile < 0 || percentile > 1) {
            throw new IAE("percentile should be in [0 ~ 1]");
          }
        }

        @Override
        protected ExprEval current(WindowContext context)
        {
          return evaluate(percentile);
        }
      };
    }

    abstract static class PercentileFunction extends FrameFunction
    {
      protected final ValueType type;

      protected int index;
      protected double[] values;

      protected PercentileFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        Preconditions.checkArgument(fieldType.isPrimitiveNumeric());
        this.type = fieldType.type();
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }

      @Override
      public void init()
      {
        int limit = window == null ? context.size() : sizeOfWindow();
        values = values != null && values.length >= limit ? values : new double[limit];
        index = 0;
      }

      @Override
      protected final void invoke(Object current, WindowContext context)
      {
        values[index++] = ((Number) current).doubleValue();
      }

      protected final ExprEval evaluate(float percentile)
      {
        if (index == 0) {
          return ExprEval.NULL_DOUBLE;
        }
        Arrays.sort(values, 0, index);
        return ExprEval.of(values[(int) (index * percentile)]);
      }
    }
  }

  @Function.Named("$median")
  final class RunningMedian extends RunningPercentile
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new PercentileFunction(args, context)
      {
        @Override
        protected ExprEval current(WindowContext context)
        {
          if (index == 0) {
            return ExprEval.nullOf(ValueDesc.of(type));
          }
          Arrays.sort(values, 0, index);
          final int x = (int) (index * 0.5);
          if (index % 2 == 0) {
            return ExprEval.of((values[x - 1] + values[x]) / 2);
          }
          return ExprEval.of(values[x]);
        }
      };
    }
  }

  @Function.Named("$histogram")
  final class Histogram extends Factory implements FixedTyped
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new HistogramFunction(args, context);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.MAP;
    }

    private class HistogramFunction extends WindowFunction
    {
      private final ValueType type;
      private final int binCount;

      private final double from;
      private final double step;

      private int index;
      private long[] longs;
      private float[] floats;
      private double[] doubles;

      public HistogramFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);

        if (parameters.isEmpty()) {
          throw new IAE("%s should have at least one argument (binCount)", name);
        }
        Preconditions.checkArgument(fieldType.isPrimitiveNumeric());
        type = fieldType.type();

        binCount = Evals.getConstantNumber(parameters.get(0)).intValue();

        from = parameters.size() > 1 ? (Evals.getConstantNumber(parameters.get(1))).doubleValue() : Double.MAX_VALUE;
        step = parameters.size() > 2 ? (Evals.getConstantNumber(parameters.get(2))).doubleValue() : Double.MAX_VALUE;
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.MAP;
      }

      @Override
      public void init()
      {
        final int limit = context.size();
        if (type == ValueType.LONG) {
          longs = longs != null && longs.length >= limit ? longs : new long[limit];
        } else if (type == ValueType.FLOAT) {
          floats = floats != null && floats.length >= limit ? floats : new float[limit];
        } else {
          doubles = doubles != null && doubles.length >= limit ? doubles : new double[limit];
        }
        index = 0;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        final Object current = Evals.evalValue(fieldExpr, context);
        if (current == null) {
          return ExprEval.nullOf(ValueDesc.MAP);
        }
        final Number number = (Number) current;
        if (type == ValueType.LONG) {
          longs[index] = number.longValue();
        } else if (type == ValueType.FLOAT) {
          floats[index] = number.floatValue();
        } else {
          doubles[index] = number.doubleValue();
        }
        index++;
        return context.hasMore() ? ExprEval.NULL_MAP : ExprEval.of(toHistogram(), ValueDesc.MAP);
      }

      private Map<String, Object> toHistogram()
      {
        if (type == ValueType.LONG) {
          Arrays.sort(longs, 0, index);
        } else if (type == ValueType.FLOAT) {
          Arrays.sort(floats, 0, index);
        } else {
          Arrays.sort(doubles, 0, index);
        }
        if (type == ValueType.LONG) {
          long min = longs[0];
          long max = longs[index - 1];

          double start = from == Double.MAX_VALUE ? min : from;
          double delta = step == Double.MAX_VALUE ? (max - start) / binCount : step;

          long[] breaks = new long[binCount + 1];
          int[] counts = new int[binCount];
          for (int i = 0; i < breaks.length; i++) {
            breaks[i] = (long) (start + (delta * i));
          }
          for (long longVal : longs) {
            if (longVal < min) {
              continue;
            }
            if (longVal > max) {
              break;
            }
            int index = Arrays.binarySearch(breaks, longVal);
            if (index < 0) {
              index = -index - 1;
            }
            // inclusive for max
            counts[index == counts.length ? index - 1 : index]++;
          }
          return ImmutableMap.of(
              "min", min, "max", max, "breaks", Longs.asList(breaks), "counts", Ints.asList(counts)
          );
        } else if (type == ValueType.FLOAT) {
          float min = floats[0];
          float max = floats[index - 1];

          double start = from == Double.MAX_VALUE ? min : from;
          double delta = step == Double.MAX_VALUE ? (max - start) / binCount : step;

          float[] breaks = new float[binCount + 1];
          int[] counts = new int[binCount];
          for (int i = 0; i < breaks.length; i++) {
            breaks[i] = (float) (start + (delta * i));
          }
          for (float floatVal : floats) {
            if (floatVal < breaks[0]) {
              continue;
            }
            if (floatVal > breaks[binCount]) {
              break;
            }
            int index = Arrays.binarySearch(breaks, floatVal);
            if (index < 0) {
              counts[-index - 2]++;
            } else {
              counts[index == counts.length ? index - 1 : index]++;
            }
          }
          return ImmutableMap.of(
              "min", min, "max", max, "breaks", Floats.asList(breaks), "counts", Ints.asList(counts)
          );
        } else {
          double min = doubles[0];
          double max = doubles[index - 1];

          double start = from == Double.MAX_VALUE ? min : from;
          double delta = step == Double.MAX_VALUE ? (max - start) / binCount : step;

          double[] breaks = new double[binCount + 1];
          int[] counts = new int[binCount];
          for (int i = 0; i < breaks.length; i++) {
            breaks[i] = start + (delta * i);
          }
          for (double doubleVal : doubles) {
            if (doubleVal < breaks[0]) {
              continue;
            }
            if (doubleVal > breaks[binCount]) {
              break;
            }
            int index = Arrays.binarySearch(breaks, doubleVal);
            if (index < 0) {
              counts[-index - 2]++;
            } else {
              counts[index == counts.length ? index - 1 : index]++;
            }
          }
          return ImmutableMap.of(
              "min", min, "max", max, "breaks", Doubles.asList(breaks), "counts", Ints.asList(counts)
          );
        }
      }
    }
  }

  @Function.Named("$size")
  final class PartitionSize extends StatelessFactory implements FixedTyped.LongType
  {
    public PartitionSize()
    {
      super(ValueDesc.LONG);
    }

    @Override
    protected ExprEval invoke(WindowContext context, Expr inputExpr)
    {
      return ExprEval.of(context.size());
    }
  }

  @Function.Named("$assign")
  final class PartitionEval extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new UnknownFunc()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          if (args.isEmpty()) {
            throw new IAE("%s should have at least output field name", name);
          }
          Object[] result = new Object[]{null, 0, 1};
          result[0] = Evals.evalString(args.get(0), bindings);
          for (int i = 1; i < args.size(); i++) {
            result[i] = Evals.evalInt(args.get(i), bindings);
          }
          return ExprEval.of(result, ValueDesc.STRUCT);
        }
      };
    }
  }

  @Function.Named("$assignFirst")
  final class AssignFirst extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new UnknownFunc()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          if (args.size() != 1) {
            throw new IAE("%s should have at least output field name", name);
          }
          return ExprEval.of(new Object[]{Evals.evalString(args.get(0), bindings), 0, 1}, ValueDesc.STRUCT);
        }
      };
    }
  }
}
