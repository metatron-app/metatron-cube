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

package hivemall.anomaly;

import com.google.common.base.Throwables;
import io.druid.java.util.common.IAE;
import hivemall.utils.lang.Preconditions;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.BuiltinFunctions;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import io.druid.math.expr.Function.NamedFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static hivemall.anomaly.ChangeFinderUDF.ChangeFinder;
import static hivemall.anomaly.ChangeFinderUDF.LossFunction;
import static hivemall.anomaly.ChangeFinderUDF.Parameters;

/**
 */
public interface HivemallFunctions extends Function.Library
{
  @Function.Named("changefinder")
  class ChangeFinderFunc extends BuiltinFunctions.NamedParams
  {
    @Override
    protected Function toFunction(List<Expr> args, final int start, final Map<String, ExprEval> parameter)
    {
      if (start == 0) {
        throw new IAE("Function[%s] must have more than 1 argument", name());
      }
      final Parameters params = new Parameters();
      int k = getInt(parameter, "k", -1);
      if (k > 0) {
        params.k = k;
      }
      double r1 = getDouble(parameter, "r1", -1);
      if (r1 > 0) {
        params.r1 = r1;
      }
      double r2 = getDouble(parameter, "r2", -1);
      if (r2 > 0) {
        params.r2 = r2;
      }
      int T1 = getInt(parameter, "T1", -1);
      if (T1 > 0) {
        params.T1 = T1;
      }
      int T2 = getInt(parameter, "T2", -1);
      if (T2 > 0) {
        params.T2 = T2;
      }
      double outlierThreshold = getDouble(parameter, "outlier_threshold", -1);
      if (outlierThreshold > 0) {
        params.outlierThreshold = outlierThreshold;
      }
      double changepointThreshold = getDouble(parameter, "changepoint_threshold", -1);
      if (changepointThreshold > 0) {
        params.changepointThreshold = changepointThreshold;
      }
      String lossFunc = getString(parameter, "lossFunc", null);
      String lossFunc1 = getString(parameter, "lossFunc1", lossFunc);
      if (lossFunc1 != null) {
        params.lossFunc1 = LossFunction.resolve(lossFunc1);
      }
      String lossFunc2 = getString(parameter, "lossFunc2", lossFunc);
      if (lossFunc2 != null) {
        params.lossFunc2 = LossFunction.resolve(lossFunc2);
      }
      StringBuilder builder = new StringBuilder("struct(");
      builder.append("outlier_score:double,changepoint_score:double");
      if (params.outlierThreshold != -1d) {
        builder.append(",is_outlier:boolean");
        if (params.changepointThreshold != -1d) {
          builder.append(",is_changepoint:boolean");
        }
      }
      final ValueDesc output = ValueDesc.of(builder.append(')').toString());
      final ChangeFinder finder = createFinder(start, params);
      final double[] _scores = new double[2];
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return output;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          Object arg;
          if (start == 1) {
            arg = Evals.evalDouble(args.get(0), bindings);
          } else {
            arg = Arrays.asList(Evals.evalDouble(args.get(0), bindings), Evals.evalDouble(args.get(1), bindings));
          }
          try {
            finder.update(arg, _scores);
            return makeOutput(_scores);
          }
          catch (HiveException e) {
            throw Throwables.propagate(e);
          }
        }

        private ExprEval makeOutput(final double[] scores)
        {
          List value;
          if (params.outlierThreshold == -1d) {
            value = Arrays.asList(scores[0], scores[1]);
          } else if (params.changepointThreshold == -1d) {
            value = Arrays.asList(scores[0], scores[1], scores[0] > params.outlierThreshold);
          } else {
            value = Arrays.asList(
                scores[0],
                scores[1],
                scores[0] > params.outlierThreshold,
                scores[1] > params.changepointThreshold
            );
          }
          return ExprEval.of(value, output);
        }
      };
    }

    private ChangeFinder createFinder(int start, Parameters params)
    {
      if (start == 1) {
        return new ChangeFinder1D(params, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
      }
      ListObjectInspector inspector = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
      );
      try {
        return new ChangeFinder2D(params, inspector);
      }
      catch (UDFArgumentTypeException e) {
        throw Throwables.propagate(e);  // never
      }
    }
  }

  @Function.Named("sst")
  class SingularSpectrumTransformFunc extends BuiltinFunctions.NamedParams
  {
    @Override
    protected Function toFunction(List<Expr> args, final int start, final Map<String, ExprEval> parameter)
    {
      if (start == 0) {
        throw new IAE("Function[%s] must have more than 1 argument", name());
      }
      final SingularSpectrumTransformUDF.Parameters params = new SingularSpectrumTransformUDF.Parameters();
      params.w = getInt(parameter, "w", params.w);
      params.n = getInt(parameter, "n", params.w);
      params.m = getInt(parameter, "m", params.w);
      params.g = getInt(parameter, "g", -1 * params.w);
      params.r = getInt(parameter, "r", params.r);
      params.k = getInt(parameter, "k", (params.r % 2 == 0) ? (2 * params.r) : (2 * params.r - 1));

      try {
        params.scoreFunc = SingularSpectrumTransformUDF.ScoreFunction.resolve(
            getString(parameter, "scorefunc", SingularSpectrumTransformUDF.ScoreFunction.svd.name())
        );
      }
      catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
      if ((params.w != params.n || params.w != params.m)
          && params.scoreFunc == SingularSpectrumTransformUDF.ScoreFunction.ika) {
        throw new IllegalArgumentException("IKA-based efficient SST requires w = n = m");
      }

      params.changepointThreshold = getDouble(parameter, "th", params.changepointThreshold);

      Preconditions.checkArgument(
          params.w >= 2, IllegalArgumentException.class, "w must be greather than 1: " + params.w
      );
      Preconditions.checkArgument(
          params.r >= 1, IllegalArgumentException.class, "r must be greater than 0: " + params.r
      );
      Preconditions.checkArgument(
          params.k >= 1, IllegalArgumentException.class, "k must be greater than 0: " + params.k
      );
      Preconditions.checkArgument(
          params.k >= params.r, IllegalArgumentException.class,
          "k must be equals to or greather than r: k=" + params.k + ", r" + params.r
      );
      Preconditions.checkArgument(
          params.changepointThreshold < 0 || params.changepointThreshold > 0.d && params.changepointThreshold < 1.d,
          IllegalArgumentException.class,
          "changepointThreshold must be in range (0, 1): " + params.changepointThreshold
      );

      final ValueDesc output;
      if (params.changepointThreshold < 0) {
        output = ValueDesc.DOUBLE;
      } else {
        output = ValueDesc.of("struct(changepoint_score:double,is_changepoint:boolean)");
      }
      final SingularSpectrumTransform sst = new SingularSpectrumTransform(
          params, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
      );
      final double[] _scores = new double[1];
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return output;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          try {
            sst.update(Evals.evalDouble(args.get(0), bindings), _scores);
            return makeOutput(_scores);
          }
          catch (HiveException e) {
            throw Throwables.propagate(e);
          }
        }

        private ExprEval makeOutput(final double[] scores)
        {
          Object value;
          if (params.changepointThreshold == -1d) {
            value = scores[0];
          } else {
            value = Arrays.asList(scores[0], scores[0] > params.changepointThreshold);
          }
          return ExprEval.of(value, output);
        }
      };
    }

    private ChangeFinder createFinder(int start, Parameters params)
    {
      if (start == 1) {
        return new ChangeFinder1D(params, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
      }
      ListObjectInspector inspector = ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
      );
      try {
        return new ChangeFinder2D(params, inspector);
      }
      catch (UDFArgumentTypeException e) {
        throw Throwables.propagate(e);  // never
      }
    }
  }

  @Function.Named("rescale")
  class RescaleFunc extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      final double min = Evals.getConstantEval(args.get(1)).asDouble();
      final double max = Evals.getConstantEval(args.get(2)).asDouble();
      if (min > max) {
        throw new IllegalArgumentException("min value `" + min + "` SHOULD be less than max value `" + max + '`');
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final double value = Evals.evalDouble(args.get(0), bindings);
          if (min == max) {
            return ExprEval.of(0.5d);
          }
          if (value < min) {
            return ExprEval.of(0.0d);
          }
          if (value > max) {
            return ExprEval.of(1.0d);
          }
          return ExprEval.of((value - min) / (max - min));
        }
      };
    }
  }
}
