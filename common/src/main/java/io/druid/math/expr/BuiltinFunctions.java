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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.Pair;
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
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface BuiltinFunctions extends Function.Library
{
  static final Logger log = new Logger(BuiltinFunctions.class);

  abstract class SingleParam implements Function
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

  abstract class DoubleParam implements Function
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

  abstract class TripleParam implements Function
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

  abstract class NamedParams implements Function
  {
    private int namedParamStart = -1;
    private final Map<String, Expr> namedParam = Maps.newLinkedHashMap();

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (namedParamStart < 0) {
        List<Expr> param = Lists.newArrayList();
        int i = 0;
        for (; i < args.size(); i++) {
          Expr expr = args.get(i);
          if (expr instanceof AssignExpr) {
            break;
          }
          param.add(args.get(i));
        }
        namedParamStart = i;

        for (; i < args.size(); i++) {
          Expr expr = args.get(i);
          if (!(expr instanceof AssignExpr)) {
            throw new RuntimeException("named parameters should not be mixed with generic param");
          }
          AssignExpr assign = (AssignExpr) expr;
          namedParam.put(Evals.getIdentifier(assign.assignee), assign.assigned);
          Preconditions.checkArgument(Evals.isConstant(assign.assigned), "named params should be constant");
        }
      }
      return eval(args.subList(0, namedParamStart), namedParam, bindings);
    }

    protected abstract ExprEval eval(List<Expr> exprs, Map<String, Expr> params, NumericBinding bindings);
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

  class Size extends SingleParam
  {
    @Override
    public String name()
    {
      return "size";
    }

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

  class Like extends ExprType.StringFunction implements Factory
  {
    private Pair<RegexUtils.PatternType, Object> matcher;

    @Override
    public String name()
    {
      return "like";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (matcher == null) {
        if (args.size() != 2) {
          throw new RuntimeException("function '" + name() + "' needs 2 arguments");
        }
        Expr expr2 = args.get(1);
        matcher = RegexUtils.parse(Evals.getConstantString(expr2));
      }
      ExprEval eval = args.get(0).eval(bindings);
      return ExprEval.of(RegexUtils.evaluate(eval.asString(), matcher.lhs, matcher.rhs));
    }

    @Override
    public Function get()
    {
      return new Like();
    }
  }

  class Regex implements Function, Factory
  {
    private Matcher matcher;
    private int index = -1;

    @Override
    public String name()
    {
      return "regex";
    }

    @Override
    public final ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return args.size() == 2 ? ExprType.LONG : args.size() == 3 ?ExprType.STRING : ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (matcher == null) {
        if (args.size() != 2 && args.size() != 3) {
          throw new RuntimeException("function '" + name() + "' needs 2 or 3 arguments");
        }
        Expr expr2 = args.get(1);
        matcher = Pattern.compile(Evals.getConstantString(expr2)).matcher("");
        if (args.size() == 3) {
          Expr expr3 = args.get(2);
          index = Ints.checkedCast(Evals.getConstantLong(expr3));
        }
      }
      ExprEval eval = args.get(0).eval(bindings);
      Matcher m = matcher.reset(eval.asString());
      if (index < 0) {
        return ExprEval.of(m.matches());
      }
      return ExprEval.of(m.matches() ? matcher.group(index) : null);
    }

    @Override
    public Function get()
    {
      return new Regex();
    }
  }

  abstract class AbstractRFunc extends ExprType.IndecisiveFunction implements Factory
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

  final class RFunc extends AbstractRFunc
  {
    private String function;

    @Override
    public String name()
    {
      return "r";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (function == null) {
        if (args.size() < 2) {
          throw new RuntimeException("function '" + name() + "' should have at least two arguments");
        }
        String name = Evals.getConstantString(args.get(1));
        String expression = Evals.getConstantString(args.get(0));
        function = registerFunction(name, expression);
      }
      return toJava(evaluate(function, args.subList(2, args.size()), bindings));
    }

    @Override
    public Function get()
    {
      return new RFunc();
    }
  }

  abstract class AbstractPythonFunc extends ExprType.IndecisiveFunction implements Factory
  {
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
      } else {
        log.info("invalid or absent of python.home in system environment..");
      }
    }

    private static final String[] params = new String[] {"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"};

    final String paramName(int index)
    {
      return index < params.length ? params[index] : "p" + index;
    }

    final PythonInterpreter p = new PythonInterpreter();

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

  final class PythonFunc extends AbstractPythonFunc
  {
    private PyCode code;
    private String parameters;

    @Override
    public String name()
    {
      return "py";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (code == null && parameters == null) {
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
          code = p.compile(builder.toString());
        } else {
          parameters = builder.toString();
        }
      }
      for (int i = 0; i < args.size() - 2; i++) {
        Object value = args.get(i + 2).eval(bindings).value();
        p.set(paramName(i), Py.java2py(value));
      }
      if (code != null) {
        return toExprEval(p.eval(code));
      } else {
        String functionName = Evals.evalString(args.get(1), bindings);
        PyCode code = p.compile(functionName + parameters);
        return toExprEval(p.eval(code));
      }
    }

    @Override
    public Function get()
    {
      return new PythonFunc();
    }
  }

  final class PythonEvalFunc extends AbstractPythonFunc
  {
    private PyCode code;

    @Override
    public String name()
    {
      return "pyEval";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (code == null) {
        if (args.isEmpty()) {
          throw new RuntimeException("function '" + name() + "' should have one argument");
        }
        code = p.compile(Evals.getConstantString(args.get(0)));
      }
      for (String column : bindings.names()) {
        p.set(column, bindings.get(column));
      }
      return toExprEval(p.eval(code), true);
    }

    @Override
    public Function get()
    {
      return new PythonEvalFunc();
    }
  }

  class Abs extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "abs";
    }

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

  class Acos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "acos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.acos(param));
    }
  }

  class Asin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "asin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.asin(param));
    }
  }

  class Atan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "atan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.atan(param));
    }
  }

  class Cbrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cbrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cbrt(param));
    }
  }

  class Ceil extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ceil";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ceil(param));
    }
  }

  class Cos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param));
    }
  }

  class Cosh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cosh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cosh(param));
    }
  }

  class Exp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "exp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.exp(param));
    }
  }

  class Expm1 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "expm1";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.expm1(param));
    }
  }

  class Floor extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "floor";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.floor(param));
    }
  }

  class GetExponent extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "getExponent";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.getExponent(param));
    }
  }

  class Log extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log(param));
    }
  }

  class Log10 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log10";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log10(param));
    }
  }

  class Log1p extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log1p";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log1p(param));
    }
  }

  class NextUp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "nextUp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.nextUp(param));
    }
  }

  class Rint extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "rint";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.rint(param));
    }
  }

  class Round extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "round";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.round(param));
    }
  }

  class Signum extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "signum";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.signum(param));
    }
  }

  class Sin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sin(param));
    }
  }

  class Sinh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sinh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sinh(param));
    }
  }

  class Sqrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sqrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sqrt(param));
    }
  }

  class Tan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tan(param));
    }
  }

  class Tanh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tanh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tanh(param));
    }
  }

  class ToDegrees extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toDegrees";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toDegrees(param));
    }
  }

  class ToRadians extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toRadians";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toRadians(param));
    }
  }

  class Ulp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ulp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ulp(param));
    }
  }

  class Atan2 extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "atan2";
    }

    @Override
    protected ExprEval eval(double y, double x)
    {
      return ExprEval.of(Math.atan2(y, x));
    }
  }

  class CopySign extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "copySign";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.copySign(x, y));
    }
  }

  class Hypot extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "hypot";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.hypot(x, y));
    }
  }

  class Remainder extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "remainder";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.IEEEremainder(x, y));
    }
  }

  class Max extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "max";
    }

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

  class Min extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "min";
    }

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

  class NextAfter extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "nextAfter";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.nextAfter(x, y));
    }
  }

  class Pow extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "pow";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.pow(x, y));
    }
  }

  class Scalb extends DoubleParam
  {
    @Override
    public String name()
    {
      return "scalb";
    }

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

  class ConditionFunc implements Function
  {
    @Override
    public String name()
    {
      return "if";
    }

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

  class CastFunc implements Function, Factory
  {
    private ExprType castTo;

    @Override
    public String name()
    {
      return "cast";
    }

    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      if (args.size() != 2) {
        return ExprType.UNKNOWN;
      }
      return ExprType.bestEffortOf(Evals.getConstantString(args.get(1)));
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 2 argument");
      }
      if (castTo == null) {
        castTo = ExprType.bestEffortOf(Evals.getConstantString(args.get(1)));
      }
      return Evals.castTo(args.get(0).eval(bindings), castTo);
    }

    @Override
    public Function get()
    {
      return new CastFunc();
    }
  }


  class IsNullFunc implements Function
  {
    @Override
    public String name()
    {
      return "isnull";
    }

    @Override
    public ExprType apply(List<Expr> args, TypeBinding bindings)
    {
      return args.size() == 1 ? ExprType.LONG : ExprType.UNKNOWN;
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'is_null' needs 1 argument");
      }
      ExprEval eval = args.get(0).eval(bindings);
      return ExprEval.of(eval.isNull());
    }
  }

  class NvlFunc implements Function
  {
    @Override
    public String name()
    {
      return "nvl";
    }

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

  class Coalesce extends NvlFunc
  {
    @Override
    public String name()
    {
      return "coalesce";
    }
  }

  class DateDiffFunc implements Function
  {
    @Override
    public String name()
    {
      return "datediff";
    }

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
      DateTime t1 = Evals.toDateTime(args.get(0).eval(bindings), (DateTimeZone)null);
      DateTime t2 = Evals.toDateTime(args.get(1).eval(bindings), (DateTimeZone)null);
      return ExprEval.of(Days.daysBetween(t1.withTimeAtStartOfDay(), t2.withTimeAtStartOfDay()).getDays());
    }
  }

  class CaseWhenFunc implements Function
  {
    @Override
    public String name()
    {
      return "case";
    }

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
        throw new RuntimeException("function 'case' needs at least 3 arguments");
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

  class JavaScriptFunc extends ExprType.IndecisiveFunction
  {
    ScriptableObject scope;
    org.mozilla.javascript.Function fnApply;
    com.google.common.base.Function<NumericBinding, Object[]> bindingExtractor;

    @Override
    public String name()
    {
      return "javascript";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (fnApply == null) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'javascript' needs 2 argument");
        }
        makeFunction(Evals.getConstantString(args.get(0)), Evals.getConstantString(args.get(1)));
      }

      final Object[] params = bindingExtractor.apply(bindings);
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return ExprEval.bestEffortOf(fnApply.call(cx, scope, scope, params));
      }
      finally {
        Context.exit();
      }
    }

    private void makeFunction(String required, String script)
    {
      final String[] bindings = splitAndTrim(required);
      final String function = "function(" + StringUtils.join(bindings, ",") + ") {" + script + "}";

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        this.scope = cx.initStandardObjects();
        this.fnApply = cx.compileFunction(scope, function, "script", 1, null);
      }
      finally {
        Context.exit();
      }

      final Object[] convey = new Object[bindings.length];
      bindingExtractor = new com.google.common.base.Function<NumericBinding, Object[]>()
      {
        @Override
        public Object[] apply(NumericBinding input)
        {
          for (int i = 0; i < bindings.length; i++) {
            convey[i] = input.get(bindings[i]);
          }
          return convey;
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

  class ConcatFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "concat";
    }

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

  class FormatFunc extends ExprType.StringFunction implements Factory
  {
    final StringBuilder builder = new StringBuilder();
    final Formatter formatter = new Formatter(builder);

    String format;
    Object[] formatArgs;

    @Override
    public String name()
    {
      return "format";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (format == null) {
        if (args.isEmpty()) {
          throw new RuntimeException("function 'format' needs at least 1 argument");
        }
        format = Evals.getConstantString(args.get(0));
        formatArgs = new Object[args.size() - 1];
      }
      builder.setLength(0);
      for (int i = 0; i < formatArgs.length; i++) {
        formatArgs[i] = args.get(i + 1).eval(bindings).value();
      }
      formatter.format(format, formatArgs);
      return ExprEval.of(builder.toString());
    }

    @Override
    public Function get()
    {
      return new FormatFunc();
    }
  }

  class LPadFunc extends ExprType.StringFunction implements Factory
  {
    @Override
    public String name()
    {
      return "lpad";
    }

    private transient int length = -1;
    private transient char padding;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (length < 0) {
        if (args.size() < 3) {
          throw new RuntimeException("function 'lpad' needs 3 arguments");
        }
        length = (int) Evals.getConstantLong(args.get(1));
        String string = Evals.getConstantString(args.get(2));
        if (string.length() != 1) {
          throw new RuntimeException("3rd argument of function 'lpad' should be constant char");
        }
        padding = string.charAt(0);
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : Strings.padStart(input, length, padding));
    }

    @Override
    public Function get()
    {
      return new LPadFunc();
    }
  }

  class RPadFunc extends ExprType.StringFunction implements Factory
  {
    @Override
    public String name()
    {
      return "rpad";
    }

    private transient int length = -1;
    private transient char padding;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (length < 0) {
        if (args.size() < 3) {
          throw new RuntimeException("function 'rpad' needs 3 arguments");
        }
        length = (int) Evals.getConstantLong(args.get(1));
        String string = Evals.getConstantString(args.get(2));
        if (string.length() != 1) {
          throw new RuntimeException("3rd argument of function 'rpad' should be constant char");
        }
        padding = string.charAt(0);
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(input == null ? null : Strings.padEnd(input, length, padding));
    }

    @Override
    public Function get()
    {
      return new RPadFunc();
    }
  }

  class UpperFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "upper";
    }

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

  class LowerFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "lower";
    }

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
  class SplitRegex extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "splitRegex";
    }

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

  // constant literal
  class Split extends ExprType.StringFunction implements Factory
  {
    private Splitter splitter;

    @Override
    public String name()
    {
      return "split";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (splitter == null) {
        if (args.size() != 3) {
          throw new RuntimeException("function 'split' needs 3 arguments");
        }
        String separator = Evals.getConstantString(args.get(1));
        if (separator.length() == 1) {
          splitter = Splitter.on(separator.charAt(0));
        } else {
          splitter = Splitter.on(separator);
        }
      }
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

    @Override
    public Function get()
    {
      return new Split();
    }
  }

  class ProperFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "proper";
    }

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

  class LengthFunc extends ExprType.LongFunction
  {
    @Override
    public String name()
    {
      return "length";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function 'length' needs 1 argument");
      }
      String input = args.get(0).eval(bindings).asString();
      return ExprEval.of(Strings.isNullOrEmpty(input) ? 0 : input.length());
    }
  }

  class LeftFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "left";
    }

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

  class RightFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "right";
    }

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

  class MidFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "mid";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'mid' needs 3 arguments");
      }
      String input = args.get(0).eval(bindings).asString();
      int start = (int) args.get(1).eval(bindings).longValue();
      int end = (int) args.get(2).eval(bindings).longValue();

      return ExprEval.of(Strings.isNullOrEmpty(input) ? input : input.substring(start, end));
    }
  }

  class IndexOfFunc extends ExprType.LongFunction
  {
    @Override
    public String name()
    {
      return "indexOf";
    }

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

  class ReplaceFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "replace";
    }

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

  class TrimFunc extends ExprType.StringFunction
  {
    @Override
    public String name()
    {
      return "trim";
    }

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

  class InFunc extends ExprType.LongFunction implements Factory
  {
    @Override
    public String name()
    {
      return "in";
    }

    private transient Set<Object> set;

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (set == null) {
        if (args.size() < 2) {
          throw new RuntimeException("function 'in' needs at least 2 arguments");
        }
        set = Sets.newHashSet();
        for (int i = 1; i < args.size(); i++) {
          set.add(Evals.getConstant(args.get(i)));
        }
      }
      return ExprEval.of(set.contains(args.get(0).eval(bindings).value()));
    }

    @Override
    public Function get()
    {
      return new InFunc();
    }
  }

  class Now extends ExprType.LongFunction {

    @Override
    public String name()
    {
      return "now";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      return ExprEval.of(System.currentTimeMillis());
    }
  }

  class IPv4In extends ExprType.LongFunction implements Factory
  {
    byte[] start;
    byte[] end = Ints.toByteArray(-1);

    @Override
    public String name()
    {
      return "ipv4_in";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'ipv4_in' needs at least 2 arguments");
      }
      if (start == null) {
        start = InetAddresses.forString(Evals.getConstantString(args.get(1))).getAddress();
        Preconditions.checkArgument(start.length == 4);
        if (args.size() > 2) {
          end = InetAddresses.forString(Evals.getConstantString(args.get(2))).getAddress();
          Preconditions.checkArgument(end.length == 4);
        }
        for (int i = 0; i < 4; i++) {
          if (UnsignedBytes.compare(start[i], end[i]) > 0) {
            throw new IllegalArgumentException();
          }
        }
      }
      String ipString = args.get(0).eval(bindings).asString();
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

    @Override
    public Function get()
    {
      return new IPv4In();
    }
  }

  abstract class PartitionFunction extends ExprType.IndecisiveFunction implements Factory
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

  class Prev extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$prev";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() - 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new Prev();
    }
  }

  class Next extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$next";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.index() + 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new Next();
    }
  }

  class PartitionLast extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$last";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(context.size() - 1, fieldName);
    }

    @Override
    public Function get()
    {
      return new PartitionLast();
    }
  }

  class PartitionFirst extends PartitionFunction
  {
    @Override
    public String name()
    {
      return "$first";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.get(0, fieldName);
    }

    @Override
    public Function get()
    {
      return new PartitionFirst();
    }
  }

  class PartitionNth extends PartitionFunction
  {
    private int nth;

    @Override
    public String name()
    {
      return "$nth";
    }

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

    @Override
    public Function get()
    {
      return new PartitionNth();
    }
  }

  class Lag extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    public String name()
    {
      return "$lag";
    }

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

    @Override
    public Function get()
    {
      return new Lag();
    }
  }

  class Lead extends PartitionFunction implements Factory
  {
    private int delta;

    @Override
    public String name()
    {
      return "$lead";
    }

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

    @Override
    public Function get()
    {
      return new Lead();
    }
  }

  class RunningDelta extends PartitionFunction
  {
    private long longPrev;
    private double doublePrev;

    @Override
    public String name()
    {
      return "$delta";
    }

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

    @Override
    public Function get()
    {
      return new RunningDelta();
    }
  }

  class RunningSum extends WindowSupport implements Factory
  {
    private long longSum;
    private double doubleSum;

    @Override
    public String name()
    {
      return "$sum";
    }

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

    @Override
    public Function get()
    {
      return new RunningSum();
    }
  }

  class RunningMin extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    public String name()
    {
      return "$min";
    }

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

    @Override
    public Function get()
    {
      return new RunningMin();
    }
  }

  class RunningMax extends WindowSupport implements Factory
  {
    private Comparable prev;

    @Override
    public String name()
    {
      return "$max";
    }

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

    @Override
    public Function get()
    {
      return new RunningMax();
    }
  }

  class RowNum extends PartitionFunction implements Factory
  {
    @Override
    public String name()
    {
      return "$row_num";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return context.index() + 1L;
    }

    @Override
    public Function get()
    {
      return new RowNum();
    }
  }

  class Rank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    public String name()
    {
      return "$rank";
    }

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

    @Override
    public Function get()
    {
      return new Rank();
    }
  }

  class DenseRank extends PartitionFunction implements Factory
  {
    private long prevRank;
    private Object prev;

    @Override
    public String name()
    {
      return "$dense_rank";
    }

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

    @Override
    public Function get()
    {
      return new DenseRank();
    }
  }

  class RunningMean extends RunningSum
  {
    private int count;

    @Override
    public String name()
    {
      return "$mean";
    }

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

    @Override
    public Function get()
    {
      return new RunningMean();
    }
  }

  class RunningVariance extends WindowSupport
  {
    long count; // number of elements
    double sum; // sum of elements
    double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

    @Override
    public String name()
    {
      return "$variance";
    }

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

    @Override
    public Function get()
    {
      return new RunningVariance();
    }
  }

  class RunningStandardDeviation extends RunningVariance
  {
    @Override
    public String name()
    {
      return "$stddev";
    }

    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }

    @Override
    public Function get()
    {
      return new RunningStandardDeviation();
    }
  }

  class RunningVariancePop extends RunningVariance
  {
    @Override
    public String name()
    {
      return "$variancePop";
    }

    @Override
    protected Double current()
    {
      return count == 1 ? 0d : nvariance / count;
    }

    @Override
    public Function get()
    {
      return new RunningVariancePop();
    }
  }

  class RunningStandardDeviationPop extends RunningVariancePop
  {
    @Override
    public String name()
    {
      return "$stddevPop";
    }

    @Override
    protected Double current()
    {
      return Math.sqrt(super.current());
    }

    @Override
    public Function get()
    {
      return new RunningStandardDeviationPop();
    }
  }

  class RunningPercentile extends WindowSupport implements Factory
  {
    private float percentile;

    private int size;
    private long[] longs;
    private double[] doubles;

    @Override
    public String name()
    {
      return "$percentile";
    }

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

    @Override
    public Function get()
    {
      return new RunningPercentile();
    }
  }

  class Histogram extends PartitionFunction implements Factory
  {
    private int binCount = -1;

    private double from = Double.MAX_VALUE;
    private double step = Double.MAX_VALUE;

    private long[] longs;
    private double[] doubles;

    @Override
    public String name()
    {
      return "$histogram";
    }

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

    @Override
    public Function get()
    {
      return new Histogram();
    }
  }

  class PartitionSize extends PartitionFunction implements Factory
  {
    @Override
    public String name()
    {
      return "$size";
    }

    @Override
    protected Object invoke(WindowContext context)
    {
      return (long) context.size();
    }

    @Override
    public Function get()
    {
      return new PartitionSize();
    }
  }

  class PartitionEval extends ExprType.StringFunction implements Function
  {
    @Override
    public String name()
    {
      return "$assign";
    }

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
  }

  class AssignFirst extends ExprType.StringFunction implements Function
  {
    @Override
    public String name()
    {
      return "$assignFirst";
    }

    @Override
    public ExprEval apply(List<Expr> args, NumericBinding bindings)
    {
      if (args.size() != 1) {
        throw new IllegalArgumentException(name() + " should have one argument (output field name)");
      }
      return ExprEval.of(args.get(0).eval(bindings).stringValue() + ":0");
    }
  }
}
