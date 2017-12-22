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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Expr.TypeBinding;
import io.druid.math.expr.Expr.WindowContext;
import io.druid.math.expr.Function.Factory;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ScriptableObject;
import org.python.core.Py;
import org.python.core.PyArray;
import org.python.core.PyCode;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.core.adapter.PyObjectAdapter;
import org.python.util.PythonInterpreter;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RFactor;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface BuiltinFunctions extends Function.Library
{
  static final Logger log = new Logger(BuiltinFunctions.class);

  abstract class SingleParam extends Function.NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return args.size() == 1 ? type(args.get(0).type(bindings)) : ExprType.UNKNOWN;
    }

    @Override
    public final ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      Expr expr = args.get(0);
      return eval(expr.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval param);

    protected abstract ExprType type(ExprType param);
  }

  abstract class DoubleParam extends Function.NamedFunction
  {
    @Override
    public final ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return args.size() == 2 ? eval(args.get(0).type(bindings), args.get(1).type(bindings)) : ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 2 arguments");
      }
      Expr expr1 = args.get(0);
      Expr expr2 = args.get(1);
      return eval(expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval x, ExprEval y);

    protected abstract ExprType eval(ExprType x, ExprType y);
  }

  abstract class TripleParam extends Function.NamedFunction
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function '" + name() + "' needs 3 arguments");
      }
      Expr expr0 = args.get(0);
      Expr expr1 = args.get(1);
      Expr expr2 = args.get(2);
      return eval(expr0.eval(bindings), expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval x, ExprEval y, ExprEval z);
  }

  abstract class NamedParams extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      final int namedParamStart;
      int i = 0;
      for (; i < args.size(); i++) {
        if (args.get(i) instanceof AssignExpr) {
          break;
        }
      }
      namedParamStart = i;
      final Map<String, ExprEval> namedParam = Maps.newLinkedHashMap();
      for (; i < args.size(); i++) {
        Expr expr = args.get(i);
        if (!(expr instanceof AssignExpr)) {
          throw new RuntimeException("named parameters should not be mixed with generic param");
        }
        AssignExpr assign = (AssignExpr) expr;
        namedParam.put(Evals.getIdentifier(assign.assignee), Evals.getConstantEval(assign.assigned));
        Preconditions.checkArgument(Evals.isConstant(assign.assigned), "named params should be constant");
      }
      List<Expr> remaining = args.subList(0, namedParamStart);
      final Function function = toFunction(parameterize(remaining, namedParam));

      return new Child()
      {
        @Override
        public ExprType apply(List<Expr> args, TypeBinding bindings)
        {
          return function.apply(args, bindings);
        }

        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          return function.apply(args.subList(0, namedParamStart), bindings);
        }
      };
    }

    protected Map<String, Object> parameterize(List<Expr> exprs, Map<String, ExprEval> namedParam)
    {
      return Maps.newHashMap();
    }

    protected final String getString(Map<String, ExprEval> namedParam, String key)
    {
      return namedParam.containsKey(key) ? namedParam.get(key).asString() : null;
    }

    protected final boolean getBoolean(Map<String, ExprEval> namedParam, String key)
    {
      return namedParam.containsKey(key) && namedParam.get(key).asBoolean();
    }

    protected abstract Function toFunction(final Map<String, Object> parameter);
  }

  abstract class SingleParamMath extends SingleParam
  {
    @Override
    public ExprType type(ExprType param)
    {
      if (param == ExprType.LONG) {
        return supports(Long.TYPE) ? ExprType.LONG : ExprType.DOUBLE;
      } else if (param == ExprType.DOUBLE) {
        return supports(Double.TYPE) ? ExprType.DOUBLE : ExprType.LONG;
      }
      return ExprType.UNKNOWN;
    }

    private boolean supports(Class<?> type)
    {
      try {
        return getClass().getDeclaredMethod("eval", type).getDeclaringClass() != SingleParamMath.class;
      }
      catch (Exception e) {
        return false;
      }
    }

    @Override
    protected ExprEval eval(ExprEval param)
    {
      if (param.type() == ExprType.LONG) {
        return eval(param.longValue());
      } else if (param.type() == ExprType.DOUBLE) {
        return eval(param.doubleValue());
      }
      return ExprEval.of(null, ExprType.UNKNOWN);
    }

    protected ExprEval eval(long param)
    {
      return eval((double) param);
    }

    protected ExprEval eval(double param)
    {
      return eval((long) param);
    }
  }

  abstract class DoubleParamMath extends DoubleParam
  {
    @Override
    public ExprType eval(ExprType x, ExprType y)
    {
      if (x.isNumeric() || y.isNumeric()) {
        if (x == ExprType.LONG && y == ExprType.LONG) {
          return supports(Long.TYPE) ? ExprType.LONG : ExprType.DOUBLE;
        } else {
          return supports(Double.TYPE) ? ExprType.DOUBLE : ExprType.LONG;
        }
      }
      return ExprType.UNKNOWN;
    }

    private boolean supports(Class<?> type)
    {
      try {
        return getClass().getDeclaredMethod("eval", type, type).getDeclaringClass() != SingleParamMath.class;
      }
      catch (Exception e) {
        return false;
      }
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.type().isNumeric() || y.type().isNumeric()) {
        if (x.type() == ExprType.LONG && y.type() == ExprType.LONG) {
          return eval(x.longValue(), y.longValue());
        } else {
          return eval(x.doubleValue(), y.doubleValue());
        }
      }
      return ExprEval.of(null, ExprType.UNKNOWN);
    }

    protected ExprEval eval(long x, long y)
    {
      return eval((double) x, (double) y);
    }

    protected ExprEval eval(double x, double y)
    {
      return eval((long) x, (long) y);
    }
  }

  abstract class TripleParamMath extends TripleParam
  {
    @Override
    protected ExprEval eval(ExprEval x, ExprEval y, ExprEval z)
    {
      if (x.type().isNumeric() || y.type().isNumeric() || z.type().isNumeric()) {
        if (x.type() == ExprType.LONG && y.type() == ExprType.LONG && z.type() == ExprType.LONG) {
          return eval(x.longValue(), y.longValue(), z.longValue());
        } else {
          return eval(x.doubleValue(), y.doubleValue(), z.doubleValue());
        }
      }
      return ExprEval.of(null, ExprType.STRING);
    }

    protected ExprEval eval(long x, long y, long z)
    {
      return eval((double) x, (double) y, (double) z);
    }

    protected ExprEval eval(double x, double y, double z)
    {
      return eval((long) x, (long) y, (long) z);
    }
  }

  @Function.Named("size")
  final class Size extends SingleParam
  {
    @Override
    public ExprType type(ExprType param)
    {
      return ExprType.LONG;
    }

    @Override
    protected ExprEval eval(ExprEval param)
    {
      if (param.value() instanceof Collection) {
        return ExprEval.of(((Collection) param.value()).size());
      }
      throw new IllegalArgumentException("parameter is not a collection");
    }
  }

  @Function.Named("regex")
  final class Regex extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new RuntimeException("function '" + name() + "' needs 2 or 3 arguments");
      }
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      final int index = args.size() == 3 ? Ints.checkedCast(Evals.getConstantLong(args.get(2))) : 0;

      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          Matcher m = matcher.reset(Evals.evalString(args.get(0), bindings));
          return ExprEval.of(m.matches() ? matcher.group(index) : null);
        }
      };
    }
  }

  abstract class AbstractRFunc extends Function.AbstractFactory
  {
    private static final Rengine r;
    private static final Map<String, String> functions = Maps.newHashMap();

    static {
      String rHome = System.getProperty("R_HOME");
      Rengine engine = null;
      if (rHome != null) {
        try {
          engine = new Rengine(new String[]{"--vanilla"}, false, null);
        }
        catch (Exception e) {
          log.warn(e, "Failed to initialize r");
        }
      }
      r = engine;
    }

    private final StringBuilder query = new StringBuilder();

    protected final String registerFunction(String function, String expression)
    {
      String prev = functions.putIfAbsent(function, expression);
      if (prev != null && !prev.equals(expression)) {
        functions.put(function, prev);
        throw new IllegalStateException("function " + function + " is registered already");
      }
      if (r.eval(expression) == null) {
        functions.remove(function);
        throw new IllegalArgumentException("invalid expression " + expression);
      }
      return function;
    }

    protected final REXP evaluate(String function, List<Expr> args, NumericBinding bindings)
    {
      query.setLength(0);
      query.append(function != null ? function : args.get(1).eval(bindings).asString()).append('(');

      r.getRsync().lock();
      try {
        for (int i = 0; i < args.size(); i++) {
          final String symbol = "p" + i;
          if (i > 0) {
            query.append(", ");
          }
          query.append(symbol);

          REXP rexp = toR(args.get(i).eval(bindings));
          r.rniAssign(symbol, exp(rexp), 0);
        }
        String expression = query.append(')').toString();
        return r.eval(expression);
      }
      finally {
        r.getRsync().unlock();
      }
    }

    private long exp(REXP rexp)
    {
      switch (rexp.getType()) {
        case REXP.XT_INT:
        case REXP.XT_ARRAY_INT:
          return r.rniPutIntArray(rexp.asIntArray());
        case REXP.XT_DOUBLE:
        case REXP.XT_ARRAY_DOUBLE:
          return r.rniPutDoubleArray(rexp.asDoubleArray());
        case REXP.XT_ARRAY_BOOL_INT:
          return r.rniPutBoolArrayI(rexp.asIntArray());
        case REXP.XT_STR:
        case REXP.XT_ARRAY_STR:
          return r.rniPutStringArray(rexp.asStringArray());
        case REXP.XT_VECTOR:
          RVector vector = rexp.asVector();
          long[] exps = new long[vector.size()];
          for (int j = 0; j < exps.length; j++) {
            exps[j] = exp((REXP)vector.get(j));
          }
          long exp = r.rniPutVector(exps);
          @SuppressWarnings("unchecked")
          Vector<String> names = vector.getNames();
          if (names != null) {
            long attr = r.rniPutStringArray(names.toArray(new String[names.size()]));
            r.rniSetAttr(exp, "names", attr);
          }
          return exp;
        default:
          return -1;
      }
    }

    private REXP toR(ExprEval eval)
    {
      if (eval.isNull()) {
        return null;
      }
      switch (eval.type()) {
        case DOUBLE:
          return new REXP(new double[]{eval.doubleValue()});
        case LONG:
          long value = eval.longValue();
          return value == (int) value ? new REXP(new int[]{(int) value}) : new REXP(new double[]{value});
        case STRING:
          return new REXP(new String[]{eval.asString()});
      }
      return toR(eval.value());
    }

    @SuppressWarnings("unchecked")
    private REXP toR(Object value)
    {
      if (value == null) {
        return new REXP(REXP.XT_NULL, null);
      }
      if (value instanceof String) {
        return new REXP(new String[]{(String) value});
      } else if (value instanceof Double) {
        return new REXP(new double[]{(Double) value});
      } else if (value instanceof Long) {
        long longValue = (Long)value;
        return longValue == (int) longValue ? new REXP(new int[]{(int) longValue}) : new REXP(new double[]{longValue});
      } else if (value instanceof List) {
        RVector vector = new RVector();
        for (Object element : ((List)value)) {
          vector.add(toR(element));
        }
        return new REXP(REXP.XT_VECTOR, vector);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        RVector vector = new RVector();
        String[] names = new String[map.size()];
        int i = 0;
        for (Map.Entry entry : map.entrySet()) {
          names[i++] = Objects.toString(entry.getKey(), null);
          vector.add(toR(entry.getValue()));
        }
        vector.setNames(names);
        return new REXP(REXP.XT_VECTOR, vector);
      } else if (value.getClass().isArray()) {
        Class component = value.getClass().getComponentType();
        if (component == String.class) {
          return new REXP((String[])value);
        } else if (component == double.class) {
          return new REXP((double[])value);
        } else if (component == long.class) {
          long[] longs = (long[]) value;
          int[] ints = GuavaUtils.checkedCast(longs);
          return ints != null ? new REXP(ints) : new REXP(GuavaUtils.castDouble(longs));
        } else if (component == int.class) {
          return new REXP((int[])value);
        }
      }
      return new REXP(new String[] {Objects.toString(value)});
    }

    protected final ExprEval toJava(REXP expr)
    {
      switch (expr.getType()) {
        case REXP.XT_INT:
          return ExprEval.of(expr.asInt());
        case REXP.XT_ARRAY_INT:
          int[] ints = expr.asIntArray();
          return ints.length == 1 ? ExprEval.of(ints[0]) : ExprEval.of(ints, ExprType.UNKNOWN);
        case REXP.XT_DOUBLE:
          return ExprEval.of(expr.asDouble());
        case REXP.XT_ARRAY_DOUBLE:
          double[] doubles = expr.asDoubleArray();
          return doubles.length == 1 ? ExprEval.of(doubles[0]) : ExprEval.of(doubles, ExprType.UNKNOWN);
        case REXP.XT_STR:
          return ExprEval.of(expr.asString());
        case REXP.XT_ARRAY_STR:
          String[] strings = expr.asStringArray();
          return strings.length == 1 ? ExprEval.of(strings[0]) : ExprEval.of(strings, ExprType.UNKNOWN);
        case REXP.XT_VECTOR:
          RVector vector = expr.asVector();
          Vector names = vector.getNames();
          if (names == null) {
            List<Object> result = Lists.newArrayList();
            for (Object element : vector) {
              result.add(toJava((REXP) element).value());
            }
            return ExprEval.of(result, ExprType.UNKNOWN);
          }
          Map<String, Object> result = Maps.newLinkedHashMap();
          for (int i = 0; i < names.size(); i++) {
            result.put(String.valueOf(names.get(i)), toJava((REXP) vector.get(i)).value());
          }
          return ExprEval.of(result, ExprType.UNKNOWN);
        case REXP.XT_FACTOR:
          RFactor factor = expr.asFactor();
          String[] array = new String[factor.size()];
          for (int i = 0; i < factor.size(); i++) {
            array[i] = factor.at(i);
          }
          return ExprEval.of(array, ExprType.UNKNOWN);
        case REXP.XT_LIST:
          // RList.. what the fuck is this?
        default:
          return ExprEval.bestEffortOf(expr.getContent());
      }
    }
  }

  @Function.Named("r")
  final class RFunc extends AbstractRFunc
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function '" + name() + "' should have at least two arguments");
      }
      String name = Evals.getConstantString(args.get(1));
      String expression = Evals.getConstantString(args.get(0));
      final String function = registerFunction(name, expression);
      return new ExternalChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          return toJava(evaluate(function, args.subList(2, args.size()), bindings));
        }
      };
    }
  }

  abstract class AbstractPythonFunc extends Function.AbstractFactory
  {
    static final boolean init;

    static {
      Properties prop = new Properties();
      String pythonHome = System.getProperty("python.home", System.getProperty("user.home") + "/jython2.7.0");
      if (new File(pythonHome).isDirectory()) {
        prop.setProperty("python.home", pythonHome);
        PythonInterpreter.initialize(System.getProperties(), prop, new String[]{});

        Py.getAdapter().addPostClass(
            new PyObjectAdapter()
            {
              @Override
              public PyObject adapt(Object o)
              {
                Map<?, ?> map = (Map<?, ?>) o;
                Map<PyObject, PyObject> converted = Maps.newHashMap();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                  converted.put(Py.java2py(entry.getKey()), Py.java2py(entry.getValue()));
                }
                return new PyDictionary(converted);
              }

              @Override
              public boolean canAdapt(Object o)
              {
                return Map.class.isInstance(o);
              }
            }
        );
        Py.getAdapter().addPostClass(
            new PyObjectAdapter()
            {
              @Override
              public PyObject adapt(Object o)
              {
                List<?> list = (List<?>) o;
                List<PyObject> converted = Lists.newArrayList();
                for (Object element : list) {
                  converted.add(Py.java2py(element));
                }
                return PyList.fromList(converted);
              }

              @Override
              public boolean canAdapt(Object o)
              {
                return List.class.isInstance(o);
              }
            }
        );
        init = true;
      } else {
        log.info("invalid or absent of python.home in system environment..");
        init = false;
      }
    }

    private static final String[] params = new String[] {"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"};

    final String paramName(int index)
    {
      return index < params.length ? params[index] : "p" + index;
    }

    final PythonInterpreter p = init ? new PythonInterpreter() : null;

    final ExprEval toExprEval(PyObject result)
    {
      return toExprEval(result, false);
    }

    final ExprEval toExprEval(PyObject result, boolean evaluation)
    {
      if (result == null || result instanceof PyNone) {
        return ExprEval.of(null, ExprType.UNKNOWN);
      }
      if (result instanceof PyString) {
        return ExprEval.of(result.asString(), ExprType.STRING);
      }
      if (result instanceof PyFloat) {
        return ExprEval.of(result.asDouble(), ExprType.DOUBLE);
      }
      if (result instanceof PyInteger || result instanceof PyLong) {
        return ExprEval.of(result.asLong(), ExprType.LONG);
      }
      if (result instanceof PyArray) {
        return ExprEval.of(((PyArray)result).getArray(), ExprType.UNKNOWN);
      }
      if (result instanceof PyList) {
        PyList pyList = (PyList) result;
        List<Object> list = Lists.newArrayList();
        for (int i = 0; i < pyList.size(); i++) {
          list.add(toExprEval(pyList.pyget(i)).value());
        }
        return ExprEval.of(list, ExprType.UNKNOWN);
      }
      if (result instanceof PyDictionary) {
        Map<PyObject, PyObject> internal = ((PyDictionary) result).getMap();
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(internal.size());
        for (Map.Entry<PyObject, PyObject> entry : internal.entrySet()) {
          ExprEval key = toExprEval(entry.getKey());
          if (!key.isNull()) {
            map.put(key.asString(), toExprEval(entry.getValue()).value());
          }
        }
        return ExprEval.of(map, ExprType.UNKNOWN);
      }
      if (result instanceof PyTuple) {
        PyObject[] array = ((PyTuple)result).getArray();
        if (evaluation) {
          return toExprEval(array[array.length - 1]);
        }
        List<Object> list = Lists.newArrayList();
        for (PyObject element : array) {
          list.add(toExprEval(element).value());
        }
        return ExprEval.of(list, ExprType.UNKNOWN);
      }
      return ExprEval.of(result.toString(), ExprType.UNKNOWN);
    }
  }

  @Function.Named("py")
  final class PythonFunc extends AbstractPythonFunc
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function '" + name() + "' should have at least two arguments");
      }
      p.exec(Evals.getConstantString(args.get(0)));
      final boolean constantMethod = Evals.isConstantString(args.get(1));

      StringBuilder builder = new StringBuilder();
      if (constantMethod) {
        builder.append(Evals.getConstantString(args.get(1)));
      }
      builder.append('(');
      for (int i = 0; i < args.size() - 2; i++) {
        if (i > 0) {
          builder.append(',');
        }
        builder.append(paramName(i));
      }
      builder.append(')');

      if (constantMethod) {
        final PyCode code = p.compile(builder.toString());
        return new ExternalChild()
        {
          @Override
          public ExprEval apply(List<Expr> args, NumericBinding bindings)
          {
            setParameters(args, bindings);
            return toExprEval(p.eval(code));
          }
        };
      }
      final String parameters = builder.toString();
      return new ExternalChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          setParameters(args, bindings);
          String functionName = Evals.evalString(args.get(1), bindings);
          PyCode code = p.compile(functionName + parameters);
          return toExprEval(p.eval(code));
        }
      };
    }

    private void setParameters(List<Expr> args, NumericBinding bindings)
    {
      for (int i = 0; i < args.size() - 2; i++) {
        Object value = args.get(i + 2).eval(bindings).value();
        p.set(paramName(i), Py.java2py(value));
      }
    }
  }

  @Function.Named("pyEval")
  final class PythonEvalFunc extends AbstractPythonFunc
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.isEmpty()) {
        throw new RuntimeException("function '" + name() + "' should have one argument");
      }
      final PyCode code = p.compile(Evals.getConstantString(args.get(0)));
      return new ExternalChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          for (String column : bindings.names()) {
            p.set(column, bindings.get(column));
          }
          return toExprEval(p.eval(code), true);
        }
      };
    }
  }

  @Function.Named("abs")
  final class Abs extends SingleParamMath
  {
    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(Math.abs(param));
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.abs(param));
    }
  }

  @Function.Named("acos")
  final class Acos extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.acos(param));
    }
  }

  @Function.Named("asin")
  final class Asin extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.asin(param));
    }
  }

  @Function.Named("atan")
  final class Atan extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.atan(param));
    }
  }

  @Function.Named("cbrt")
  final class Cbrt extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cbrt(param));
    }
  }

  @Function.Named("ceil")
  final class Ceil extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ceil(param));
    }
  }

  @Function.Named("cos")
  final class Cos extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param));
    }
  }

  @Function.Named("cosh")
  final class Cosh extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cosh(param));
    }
  }

  @Function.Named("exp")
  final class Exp extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.exp(param));
    }
  }

  @Function.Named("expm1")
  final class Expm1 extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.expm1(param));
    }
  }


  @Function.Named("floor")
  final class Floor extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.floor(param));
    }
  }

  @Function.Named("getExponent")
  final class GetExponent extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.getExponent(param));
    }
  }

  @Function.Named("log")
  final class Log extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log(param));
    }
  }

  @Function.Named("log10")
  final class Log10 extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log10(param));
    }
  }

  @Function.Named("log1p")
  final class Log1p extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log1p(param));
    }
  }

  @Function.Named("nextUp")
  final class NextUp extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.nextUp(param));
    }
  }

  @Function.Named("rint")
  final class Rint extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.rint(param));
    }
  }

  @Function.Named("round")
  final class Round extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.round(param));
    }
  }

  @Function.Named("signum")
  final class Signum extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.signum(param));
    }
  }

  @Function.Named("sin")
  final class Sin extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sin(param));
    }
  }

  @Function.Named("sinh")
  final class Sinh extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sinh(param));
    }
  }

  @Function.Named("sqrt")
  final class Sqrt extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sqrt(param));
    }
  }

  @Function.Named("tan")
  final class Tan extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tan(param));
    }
  }

  @Function.Named("tanh")
  final class Tanh extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tanh(param));
    }
  }

  @Function.Named("toDegrees")
  final class ToDegrees extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toDegrees(param));
    }
  }

  @Function.Named("toRadians")
  final class ToRadians extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toRadians(param));
    }
  }

  @Function.Named("ulp")
  final class Ulp extends SingleParamMath
  {
    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ulp(param));
    }
  }

  @Function.Named("atan2")
  final class Atan2 extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double y, double x)
    {
      return ExprEval.of(Math.atan2(y, x));
    }
  }

  @Function.Named("copySign")
  final class CopySign extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.copySign(x, y));
    }
  }

  @Function.Named("hypot")
  final class Hypot extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.hypot(x, y));
    }
  }

  @Function.Named("remainder")
  final class Remainder extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.IEEEremainder(x, y));
    }
  }

  @Function.Named("max")
  final class Max extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.max(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.max(x, y));
    }
  }

  @Function.Named("min")
  final class Min extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.min(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.min(x, y));
    }
  }

  @Function.Named("div")
  final class Div extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(final long x, final long y)
    {
      return ExprEval.of(x / y);
    }

    @Override
    protected ExprEval eval(final double x, final double y)
    {
      return ExprEval.of((long) (x / y));
    }
  }

  @Function.Named("nextAfter")
  final class NextAfter extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.nextAfter(x, y));
    }
  }

  @Function.Named("pow")
  final class Pow extends DoubleParamMath
  {
    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.pow(x, y));
    }
  }

  @Function.Named("scalb")
  final class Scalb extends DoubleParam
  {
    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.isNumeric() && y.isNumeric()) {
        return ExprEval.of(Math.scalb(x.doubleValue(), y.intValue()));
      }
      return ExprEval.of(null, ExprType.UNKNOWN);
    }

    @Override
    protected ExprType eval(ExprType x, ExprType y)
    {
      if (x.isNumeric() && y.isNumeric()) {
        return ExprType.DOUBLE;
      }
      return ExprType.UNKNOWN;
    }
  }

  @Function.Named("if")
  final class ConditionFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      if (args.size() < 3 || args.size() % 2 == 0) {
        return ExprType.UNKNOWN;
      }
      ExprType prev = args.get(1).type(bindings);
      for (int i = 3; i < args.size() - 1; i += 2) {
        ExprType type = args.get(i).type(bindings);
        if (prev != null && prev != type) {
          return ExprType.UNKNOWN;
        }
        prev = type;
      }
      if (!prev.equals(args.get(args.size() - 1).type(bindings))) {
        return ExprType.UNKNOWN;
      }
      return prev;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'if' needs at least 3 argument");
      }
      if (args.size() % 2 == 0) {
        throw new RuntimeException("function 'if' needs default value");
      }

      for (int i = 0; i < args.size() - 1; i += 2) {
        if (args.get(i).eval(bindings).asBoolean()) {
          return args.get(i + 1).eval(bindings);
        }
      }
      return args.get(args.size() - 1).eval(bindings);
    }
  }

  @Function.Named("cast")
  final class CastFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 2 argument");
      }
      final ExprType castTo = ExprType.bestEffortOf(Evals.getConstantString(args.get(1)));
      return new Child()
      {
        @Override
        public ExprType apply(List<Expr> args, TypeBinding bindings)
        {
          return castTo;
        }

        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          try {
            return Evals.castTo(args.get(0).eval(bindings), castTo);
          }
          catch (Exception e) {
            return ExprEval.of(null, castTo);
          }
        }
      };
    }
  }

  @Function.Named("isNull")
  final class IsNullFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return args.size() == 1 ? ExprType.LONG : ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'isnull' needs 1 argument");
      }
      ExprEval eval = args.get(0).eval(bindings);
      return ExprEval.of(eval.isNull());
    }
  }

  @Function.Named("nvl")
  class NvlFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      if (args.size() == 2) {
        ExprType x = args.get(0).type(bindings);
        ExprType y = args.get(1).type(bindings);
        if (x == y) {
          return x;
        }
      }
      return ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'nvl' needs 2 arguments");
      }
      ExprEval eval = args.get(0).eval(bindings);
      if (eval.isNull()) {
        return args.get(1).eval(bindings);
      }
      return eval;
    }
  }

  @Function.Named("coalesce")
  final class Coalesce extends NvlFunc
  {
  }

  @Function.Named("datediff")
  final class DateDiffFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return args.size() == 2 ? ExprType.LONG : ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'datediff' need at least 2 arguments");
      }
      DateTime t1 = Evals.toDateTime(args.get(0).eval(bindings), (DateTimeZone) null);
      DateTime t2 = Evals.toDateTime(args.get(1).eval(bindings), (DateTimeZone) null);
      return ExprEval.of(Days.daysBetween(t1.withTimeAtStartOfDay(), t2.withTimeAtStartOfDay()).getDays());
    }
  }

  @Function.Named("switch")
  final class SwitchFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      if (args.size() < 3) {
        return ExprType.UNKNOWN;
      }
      ExprType prev = args.get(2).type(bindings);
      for (int i = 4; i < args.size(); i += 2) {
        if (prev != args.get(i).type(bindings)) {
          return ExprType.UNKNOWN;
        }
      }
      if (args.size() % 2 != 1 && prev != args.get(args.size() - 1).type(bindings)) {
        return ExprType.UNKNOWN;
      }
      return prev;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'switch' needs at least 3 arguments");
      }
      final ExprEval leftVal = args.get(0).eval(bindings);
      for (int i = 1; i < args.size() - 1; i += 2) {
        if (Evals.eq(leftVal, args.get(i).eval(bindings))) {
          return args.get(i + 1).eval(bindings);
        }
      }
      if (args.size() % 2 != 1) {
        return args.get(args.size() - 1).eval(bindings);
      }
      return leftVal.defaultValue();
    }
  }

  @Function.Named("case")
  final class CaseFunc extends Function.NamedFunction
  {
    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      if (args.size() < 2) {
        return ExprType.UNKNOWN;
      }
      ExprType prev = null;
      for (int i = 1; i < args.size() - 1; i += 2) {
        ExprType type = args.get(i).type(bindings);
        if (prev == null || prev == type) {
          prev = type;
          continue;
        }
        return ExprType.UNKNOWN;
      }
      if (args.size() % 2 == 1 && prev != args.get(args.size() - 1).type(bindings)) {
        return ExprType.UNKNOWN;
      }
      return prev;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'case' needs at least 2 arguments");
      }
      ExprType type = null;
      for (int i = 0; i < args.size() - 1; i += 2) {
        ExprEval eval = Evals.eval(args.get(i), bindings);
        if (eval.asBoolean()) {
          return args.get(i + 1).eval(bindings);
        }
        if (type != null && type != eval.type()) {
          type = ExprType.UNKNOWN;
        } else {
          type = eval.type();
        }
      }
      if (args.size() % 2 == 1) {
        return args.get(args.size() - 1).eval(bindings);
      }
      return ExprEval.of(null, type);
    }
  }

  @Function.Named("javascript")
  final class JavaScriptFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'javascript' needs 2 argument");
      }
      final String[] parameters = splitAndTrim(Evals.getConstantString(args.get(0)));
      final String function =
          "function(" + StringUtils.join(parameters, ",") + ") {" + Evals.getConstantString(args.get(1)) + "}";

      final ScriptableObject scope;
      final org.mozilla.javascript.Function fnApply;
      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();
        fnApply = cx.compileFunction(scope, function, "script", 1, null);
      }
      finally {
        Context.exit();
      }

      return new ExternalChild()
      {
        private final Object[] convey = new Object[parameters.length];

        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          for (int i = 0; i < parameters.length; i++) {
            convey[i] = bindings.get(parameters[i]);
          }
          // one and only one context per thread
          final Context cx = Context.enter();
          try {
            return ExprEval.bestEffortOf(fnApply.call(cx, scope, scope, convey));
          }
          finally {
            Context.exit();
          }
        }
      };
    }

    private String[] splitAndTrim(String required)
    {
      String[] splits = required.split(",");
      for (int i = 0; i < splits.length; i++) {
        splits[i] = splits[i].trim();
      }
      return splits;
    }
  }

  @Function.Named("concat")
  final class ConcatFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      StringBuilder b = new StringBuilder();
      for (Expr expr : args) {
        b.append(expr.eval(bindings).asString());
      }
      return ExprEval.of(b.toString());
    }
  }

  @Function.Named("format")
  final class FormatFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.isEmpty()) {
        throw new RuntimeException("function 'format' needs at least 1 argument");
      }
      final String format = Evals.getConstantString(args.get(0));
      final Object[] formatArgs = new Object[args.size() - 1];
      return new StringChild()
      {
        final StringBuilder builder = new StringBuilder();
        final Formatter formatter = new Formatter(builder);

        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          builder.setLength(0);
          for (int i = 0; i < formatArgs.length; i++) {
            formatArgs[i] = args.get(i + 1).eval(bindings).value();
          }
          formatter.format(format, formatArgs);
          return ExprEval.of(builder.toString());
        }
      };
    }
  }

  @Function.Named("lpad")
  final class LPadFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'lpad' needs 3 arguments");
      }
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new RuntimeException("3rd argument of function 'lpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return ExprEval.of(input == null ? null : Strings.padStart(input, length, padding));
        }
      };
    }
  }

  @Function.Named("rpad")
  final class RPadFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 3) {
        throw new RuntimeException("function 'rpad' needs 3 arguments");
      }
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new RuntimeException("3rd argument of function 'rpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return ExprEval.of(input == null ? null : Strings.padEnd(input, length, padding));
        }
      };
    }
  }

  @Function.Named("upper")
  final class UpperFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'upper' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : input.toUpperCase());
    }
  }

  @Function.Named("lower")
  final class LowerFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'lower' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : input.toLowerCase());
    }
  }

  // pattern
  @Function.Named("splitRegex")
  final class SplitRegex extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'splitRegex' needs 3 arguments");
      }
      ExprEval inputEval = args.get(0).eval(bindings);
      if (inputEval.isNull()) {
        return ExprEval.of((String) null);
      }
      String input = inputEval.asString();
      String splitter = args.get(1).eval(bindings).asString();
      int index = (int) args.get(2).eval(bindings).longValue();

      String[] split = input.split(splitter);
      return ExprEval.of(index >= split.length ? null : split[index]);
    }
  }

  @Function.Named("split")
  final class Split extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'split' needs 3 arguments");
      }
      final Splitter splitter;
      String separator = Evals.getConstantString(args.get(1));
      if (separator.length() == 1) {
        splitter = Splitter.on(separator.charAt(0));
      } else {
        splitter = Splitter.on(separator);
      }
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          ExprEval inputEval = args.get(0).eval(bindings);
          if (inputEval.isNull()) {
            return ExprEval.of((String) null);
          }
          String input = inputEval.asString();
          int index = (int) args.get(2).eval(bindings).longValue();
          if (index < 0) {
            return ExprEval.of((String) null);
          }
          for (String x : splitter.split(input)) {
            if (index-- == 0) {
              return ExprEval.of(x);
            }
          }
          return ExprEval.of((String) null);
        }
      };
    }
  }

  @Function.Named("proper")
  final class ProperFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'proper' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(
          Strings.isNullOrEmpty(input) ? input :
          Character.toUpperCase(input.charAt(0)) + input.substring(1).toLowerCase()
      );
    }
  }

  @Function.Named("length")
  class LengthFunc extends Function.LongOut
  {
    @Override
    public final ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? 0 : input.length());
    }
  }

  @Function.Named("strlen")
  final class StrlenFunc extends LengthFunc {
  }

  @Function.Named("left")
  final class LeftFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'left' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int index = (int) args.get(1).eval(bindings).longValue();

      return ExprEval.of(Strings.isNullOrEmpty(input) || input.length() < index ? input : input.substring(0, index));
    }
  }

  @Function.Named("right")
  final class RightFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'right' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int index = (int) args.get(1).eval(bindings).longValue();

      return ExprEval.of(
          Strings.isNullOrEmpty(input) || input.length() < index
          ? input
          : input.substring(input.length() - index)
      );
    }
  }

  @Function.Named("mid")
  class MidFunc extends Function.AbstractFactory
  {
    @Override
    public final Function create(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function '" + name() + "' needs 3 arguments");
      }
      if (Evals.isConstant(args.get(1)) && Evals.isConstant(args.get(2))) {
        final int start = Evals.getConstantInt(args.get(1));
        final int end = Evals.getConstantInt(args.get(2));
        return new StringChild()
        {
          @Override
          public ExprEval apply(List<Expr> args, NumericBinding bindings)
          {
            String input = Evals.evalString(args.get(0), bindings);
            return eval(input, start, end);
          }
        };
      }
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return eval(input, Evals.evalInt(args.get(1), bindings), Evals.evalInt(args.get(2), bindings));
        }
      };
    }

    protected ExprEval eval(String input, int start, int end)
    {
      if (input == null || start >= input.length()) {
        return ExprEval.of(null, ExprType.STRING);
      }
      if (end < 0) {
        return ExprEval.of(input.substring(start));
      } else {
        return ExprEval.of(input.substring(start, Math.min(end, input.length())));
      }
    }
  }

  @Function.Named("substring")
  final class SubstringFunc extends MidFunc
  {
    @Override
    protected ExprEval eval(String input, int start, int length)
    {
      if (input == null || start >= input.length()) {
        return ExprEval.of(null, ExprType.STRING);
      }
      if (length < 0) {
        return ExprEval.of(input.substring(start));
      } else {
        return ExprEval.of(input.substring(start, Math.min(start + length, input.length())));
      }
    }
  }

  @Function.Named("indexOf")
  final class IndexOfFunc extends Function.LongOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'indexOf' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String find = args.get(1).eval(bindings).asString();

      return ExprEval.of(Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? -1 : input.indexOf(find));
    }
  }

  @Function.Named("replace")
  final class ReplaceFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'indexOf' needs 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String find = args.get(1).eval(bindings).asString();
      String replace = args.get(2).eval(bindings).asString();

      return ExprEval.of(
          Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? input :
          StringUtils.replace(input, find, replace)
      );
    }
  }

  @Function.Named("trim")
  final class TrimFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'trim' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.isNullOrEmpty(input) ? input : input.trim());
    }
  }

  // sql
  @Function.Named("btrim")
  final class BtrimFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new RuntimeException("function 'btrim' needs 1 or 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
      return ExprEval.of(StringUtils.stripEnd(StringUtils.stripStart(input, strip), strip));
    }
  }

  // sql
  @Function.Named("ltrim")
  final class LtrimFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new RuntimeException("function 'ltrim' needs 1 or 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
      return ExprEval.of(StringUtils.stripStart(input, strip));
    }
  }

  // sql
  @Function.Named("rtrim")
  final class RtrimFunc extends Function.StringOut
  {
    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new RuntimeException("function 'rtrim' needs 1 or 2 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
      return ExprEval.of(StringUtils.stripEnd(input, strip));
    }
  }

  @Function.Named("ipv4_in")
  final class IPv4In extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'ipv4_in' needs at least 2 arguments");
      }
      final byte[] start = InetAddresses.forString(Evals.getConstantString(args.get(1))).getAddress();
      final byte[] end;
      Preconditions.checkArgument(start.length == 4);
      if (args.size() > 2) {
        end = InetAddresses.forString(Evals.getConstantString(args.get(2))).getAddress();
        Preconditions.checkArgument(end.length == 4);
      } else {
        end = Ints.toByteArray(-1);
      }
      for (int i = 0; i < 4; i++) {
        if (UnsignedBytes.compare(start[i], end[i]) > 0) {
          throw new IllegalArgumentException("start[n] <= end[n]");
        }
      }
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          String ipString = Evals.evalString(args.get(0), bindings);
          try {
            return ExprEval.of(evaluate(ipString));
          }
          catch (Exception e) {
            return ExprEval.of(false);
          }
        }

        private boolean evaluate(String ipString)
        {
          final byte[] address = InetAddresses.forString(ipString).getAddress();
          if (address.length != 4) {
            return false;
          }
          for (int i = 0; i < 4; i++) {
            if (UnsignedBytes.compare(address[i], start[i]) < 0 || UnsignedBytes.compare(address[i], end[i]) > 0) {
              return false;
            }
          }
          return true;
        }
      };
    }
  }

  abstract class PartitionFunction extends Function.IndecisiveOut implements Factory
  {
    protected String fieldName;
    protected ExprType fieldType;
    protected Object[] parameters;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (!(bindings instanceof WindowContext)) {
        throw new IllegalStateException("function '" + name() + "' needs window context");
      }
      WindowContext context = (WindowContext) bindings;
      if (fieldName == null) {
        initialize(args, context);
      }
      return ExprEval.bestEffortOf(invoke(context), fieldType);
    }

    protected final void initialize(List<Expr> args, WindowContext context)
    {
      if (args.size() > 0) {
        fieldName = Evals.getIdentifier(args.get(0));   // todo can be expression
        fieldType = context.type(fieldName);
        parameters = Evals.getConstants(args.subList(1, args.size()));
      } else {
        fieldName = "$$$";
        parameters = new Object[0];
      }
      initialize(context, parameters);
    }

    protected void initialize(WindowContext context, Object[] parameters) { }

    protected abstract Object invoke(WindowContext context);

    protected void reset() { }

    @Override
    public Function create(List<Expr> args)
    {
      try {
        return getClass().newInstance();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  abstract class WindowSupport extends PartitionFunction
  {
    protected int[] window;

    @Override
    protected void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length >= 2) {
        window = new int[]{Integer.MIN_VALUE, 0};
        if (!"?".equals(parameters[parameters.length - 2])) {
          window[0] = ((Number) parameters[parameters.length - 2]).intValue();
        }
        if (!"?".equals(parameters[parameters.length - 1])) {
          window[1] = ((Number) parameters[parameters.length - 1]).intValue();
        }
      }
    }

    protected final int sizeOfWindow()
    {
      return window == null ? -1 : Math.abs(window[0] - window[1]) + 1;
    }

    protected final Object invoke(WindowContext context)
    {
      if (window != null) {
        reset();
        for (Object object : context.iterator(window[0], window[1], fieldName)) {
          invoke(object);
        }
      } else {
        invoke(context.get(fieldName));
      }
      return current();
    }

    protected abstract void invoke(Object current);

    protected abstract Object current();
  }

  @Function.Named("$prev")
  final class Prev extends PartitionFunction
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() - 1, fieldName);
    }
  }

  @Function.Named("$next")
  final class Next extends PartitionFunction
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() + 1, fieldName);
    }
  }

  @Function.Named("$last")
  final class PartitionLast extends PartitionFunction
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.size() - 1, fieldName);
    }
  }

  @Function.Named("$first")
  final class PartitionFirst extends PartitionFunction
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(0, fieldName);
    }
  }

  @Function.Named("$nth")
  final class PartitionNth extends PartitionFunction
  {
    private int nth;

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new RuntimeException("function 'nth' needs 1 index argument");
      }
      nth = ((Number) parameters[0]).intValue();
      if (nth < 0) {
        throw new IllegalArgumentException("nth should not be negative");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(nth, fieldName);
    }
  }

  @Function.Named("$lag")
  final class Lag extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new IllegalArgumentException("function 'lag' needs 1 index argument");
      }
      delta = ((Number) parameters[0]).intValue();
      if (delta <= 0) {
        throw new IllegalArgumentException("delta should be positive integer");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() - delta, fieldName);
    }
  }

  @Function.Named("$lead")
  final class Lead extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    protected final void initialize(WindowContext context, Object[] parameters)
    {
      if (parameters.length != 1 || !(parameters[0] instanceof Long)) {
        throw new IllegalArgumentException("function 'lead' needs 1 index argument");
      }
      delta = ((Number) parameters[0]).intValue();
      if (delta <= 0) {
        throw new IllegalArgumentException("delta should be positive integer");
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() + delta, fieldName);
    }
  }

  @Function.Named("$delta")
  final class RunningDelta extends PartitionFunction
  {
    private long longPrev;
    private double doublePrev;

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0) {
        switch (fieldType) {
          case LONG:
            longPrev = ((Number) current).longValue();
            return 0L;
          case DOUBLE:
            doublePrev = ((Number) current).doubleValue();
            return 0D;
          default:
            throw new IllegalArgumentException("unsupported type " + fieldType);
        }
      }
      switch (fieldType) {
        case LONG:
          long currentLong = ((Number) current).longValue();
          long deltaLong = currentLong - longPrev;
          longPrev = currentLong;
          return deltaLong;
        case DOUBLE:
          double currentDouble = ((Number) current).doubleValue();
          double deltaDouble = currentDouble - doublePrev;
          doublePrev = currentDouble;
          return deltaDouble;
        default:
          throw new IllegalArgumentException("unsupported type " + fieldType);
      }
    }

    @Override
    protected void reset()
    {
      longPrev = 0;
      doublePrev = 0;
    }
  }

  @Function.Named("$sum")
  class RunningSum extends WindowSupport implements Factory
  {
    private long longSum;
    private double doubleSum;

    @Override
    protected void invoke(Object current)
    {
      switch (fieldType) {
        case LONG:
          longSum += ((Number) current).longValue();
          break;
        case DOUBLE:
          doubleSum += ((Number) current).doubleValue();
          break;
        default:
          throw new IllegalArgumentException("unsupported type " + fieldType);
      }
    }

    @Override
    protected Object current()
    {
      if (fieldType == ExprType.LONG) {
        return longSum;
      } else {
        return doubleSum;
      }
    }

    @Override
    protected void reset()
    {
      longSum = 0;
      doubleSum = 0;
    }
  }

  @Function.Named("$min")
  final class RunningMin extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    @SuppressWarnings("unchecked")
    protected void invoke(Object current)
    {
      Comparable comparable = (Comparable) current;
      if (prev == null || (comparable != null && comparable.compareTo(prev) < 0)) {
        prev = comparable;
      }
    }

    @Override
    protected Object current()
    {
      return prev;
    }

    @Override
    protected void reset()
    {
      prev = null;
    }
  }

  @Function.Named("$max")
  final class RunningMax extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    @SuppressWarnings("unchecked")
    protected void invoke(Object current)
    {
      Comparable comparable = (Comparable) current;
      if (prev == null || (comparable != null && comparable.compareTo(prev) > 0)) {
        prev = comparable;
      }
    }

    @Override
    protected Object current()
    {
      return prev;
    }

    @Override
    protected void reset()
    {
      prev = null;
    }
  }

  @Function.Named("$row_num")
  final class RowNum extends PartitionFunction implements Factory
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return context.index() + 1L;
    }
  }

  @Function.Named("$rank")
  final class Rank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0 || !Objects.equals(prev, current)) {
        prev = current;
        prevRank = context.index() + 1;
      }
      return prevRank;
    }

    @Override
    protected void reset()
    {
      prevRank = 0L;
      prev = null;
    }
  }

  @Function.Named("$dense_rank")
  final class DenseRank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (context.index() == 0 || !Objects.equals(prev, current)) {
        prev = current;
        prevRank++;
      }
      return prevRank;
    }

    @Override
    protected void reset()
    {
      prevRank = 0L;
      prev = null;
    }
  }

  @Function.Named("$mean")
  final class RunningMean extends RunningSum
  {
    private int count;

    @Override
    protected void invoke(Object current)
    {
      super.invoke(current);
      count++;
    }

    @Override
    protected Object current()
    {
      return ((Number) super.current()).doubleValue() / count;
    }

    public void reset()
    {
      super.reset();
      count = 0;
    }
  }

  @Function.Named("$variance")
  class RunningVariance extends WindowSupport
  {
    long count; // number of elements
    double sum; // sum of elements
    double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

    @Override
    protected void invoke(Object current)
    {
      double v = ((Number) current).doubleValue();
      count++;
      sum += v;
      if (count > 1) {
        double t = count * v - sum;
        nvariance += (t * t) / ((double) count * (count - 1));
      }
    }

    @Override
    protected Double current()
    {
      return count == 1 ? 0d : nvariance / (count - 1);
    }

    public void reset()
    {
      count = 0;
      sum = 0;
      nvariance = 0;
    }
  }

  @Function.Named("$stddev")
  final class RunningStandardDeviation extends RunningVariance
  {
    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }
  }

  @Function.Named("$variancePop")
  class RunningVariancePop extends RunningVariance
  {
    @Override
    protected Double current()
    {
      return count == 1 ? 0d : nvariance / count;
    }
  }

  @Function.Named("$stddevPop")
  final class RunningStandardDeviationPop extends RunningVariancePop
  {
    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }
  }

  @Function.Named("$percentile")
  final class RunningPercentile extends WindowSupport implements Factory
  {
    private float percentile;

    private int size;
    private long[] longs;
    private double[] doubles;

    @Override
    protected void initialize(WindowContext context, Object[] parameters)
    {
      super.initialize(context, parameters);
      Evals.assertNumeric(fieldType);
      percentile = ((Number) parameters[0]).floatValue();

      int limit = window == null ? context.size() : sizeOfWindow();
      if (fieldType == ExprType.LONG) {
        longs = new long[limit];
      } else {
        doubles = new double[limit];
      }
    }

    @Override
    protected void invoke(Object current)
    {
      if (window == null) {
        if (fieldType == ExprType.LONG) {
          long longValue = ((Number) current).longValue();
          int index = Arrays.binarySearch(longs, 0, size, longValue);
          if (index < 0) {
            index = -index - 1;
          }
          System.arraycopy(longs, index, longs, index + 1, size - index);
          longs[index] = longValue;
        } else {
          double doubleValue = ((Number) current).doubleValue();
          int index = Arrays.binarySearch(doubles, 0, size, doubleValue);
          if (index < 0) {
            index = -index - 1;
          }
          System.arraycopy(doubles, index, doubles, index + 1, size - index);
          doubles[index] = doubleValue;
        }
      } else {
        if (fieldType == ExprType.LONG) {
          longs[size] = ((Number) current).longValue();
        } else {
          doubles[size] = ((Number) current).doubleValue();
        }
      }
      size++;
    }

    @Override
    protected Object current()
    {
      if (window != null) {
        if (fieldType == ExprType.LONG) {
          Arrays.sort(longs, 0, size);
        } else {
          Arrays.sort(doubles, 0, size);
        }
      }
      int index = (int) (size * percentile);
      if (fieldType == ExprType.LONG) {
        return longs[index];
      } else {
        return doubles[index];
      }
    }

    @Override
    public void reset()
    {
      size = 0;
    }
  }

  @Function.Named("$histogram")
  final class Histogram extends PartitionFunction implements Factory
  {
    private int binCount = -1;

    private double from = Double.MAX_VALUE;
    private double step = Double.MAX_VALUE;

    private long[] longs;
    private double[] doubles;

    @Override
    protected void initialize(WindowContext context, Object[] parameters)
    {
      super.initialize(context, parameters);
      if (parameters.length == 0) {
        throw new IllegalArgumentException(name() + " should have at least one argument (binCount)");
      }
      binCount = ((Number)parameters[0]).intValue();

      if (parameters.length > 1) {
        from = ((Number)parameters[1]).doubleValue();
      }
      if (parameters.length > 2) {
        step = ((Number)parameters[2]).doubleValue();
      }

      if (fieldType == ExprType.LONG) {
        longs = new long[context.size()];
      } else if (fieldType == ExprType.DOUBLE) {
        doubles = new double[context.size()];
      } else {
        throw new IllegalArgumentException("unsupported type " + fieldType);
      }
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      Object current = context.get(fieldName);
      if (fieldType == ExprType.LONG) {
        longs[context.index()] = ((Number) current).longValue();
      } else {
        doubles[context.index()] = ((Number) current).doubleValue();
      }
      if (context.index() < context.size() - 1) {
        return null;
      }
      if (fieldType == ExprType.LONG) {
        Arrays.sort(longs);
      } else {
        Arrays.sort(doubles);
      }
      if (fieldType == ExprType.LONG) {
        Arrays.sort(longs);

        long min = longs[0];
        long max = longs[longs.length - 1];

        double start = from == Double.MAX_VALUE ? min : from;
        double delta = step == Double.MAX_VALUE ? (max - start) / binCount : step;

        long[] breaks = new long[binCount + 1];
        int[] counts = new int[binCount];
        for (int i = 0; i < breaks.length; i++) {
          breaks[i] = (long)(start + (delta * i));
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
          counts[index == counts.length  ? index - 1 : index]++;
        }
        return ImmutableMap.of("min", min, "max", max, "breaks", Longs.asList(breaks), "counts", Ints.asList(counts));
      } else {
        Arrays.sort(doubles);

        double min = doubles[0];
        double max = doubles[doubles.length - 1];

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
        return ImmutableMap.of("min", min, "max", max, "breaks", Doubles.asList(breaks), "counts", Ints.asList(counts));
      }
    }
  }

  @Function.Named("$size")
  final class PartitionSize extends PartitionFunction implements Factory
  {
    @Override
    protected Object invoke(WindowContext context)
    {
      return (long) context.size();
    }
  }

  @Function.Named("$assign")
  final class PartitionEval extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          if (args.isEmpty()) {
            throw new IllegalArgumentException(name() + " should have at least output field name");
          }
          StringBuilder builder = new StringBuilder();
          builder.append(args.get(0).eval(bindings).stringValue());
          for (int i = 1; i < args.size(); i++) {
            builder.append(':').append(args.get(i).eval(bindings).longValue());
          }
          return ExprEval.of(builder.toString());
        }
      };
    }
  }

  @Function.Named("$assignFirst")
  final class AssignFirst extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      return new StringChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, NumericBinding bindings)
        {
          if (args.size() != 1) {
            throw new IllegalArgumentException(name() + " should have one argument (output field name)");
          }
          return ExprEval.of(args.get(0).eval(bindings).stringValue() + ":0");
        }
      };
    }
  }
}
