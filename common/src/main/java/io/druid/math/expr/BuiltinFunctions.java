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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Expr.WindowContext;
import io.druid.math.expr.Function.Factory;
import io.druid.math.expr.Function.NamedFactory;
import org.apache.commons.lang.StringUtils;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface BuiltinFunctions extends Function.Library
{
  static final Logger log = new Logger(BuiltinFunctions.class);

  abstract class NamedParams extends NamedFactory
  {
    @Override
    public final Function create(List<Expr> args, TypeResolver resolver)
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
          throw new IAE("named parameters should not be mixed with generic param");
        }
        AssignExpr assign = (AssignExpr) expr;
        namedParam.put(Evals.getIdentifier(assign.assignee), Evals.getConstantEval(assign.assigned));
        Preconditions.checkArgument(Evals.isConstant(assign.assigned), "named params should be constant");
      }
      final Function function = toFunction(args, namedParamStart, namedParam);

      return asChild(namedParamStart, function);
    }

    protected final Function asChild(final int namedParamStart, final Function function)
    {
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return function.returns();
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return function.evaluate(args.subList(0, namedParamStart), bindings);
        }
      };
    }

    protected abstract Function toFunction(List<Expr> args, int start, Map<String, ExprEval> parameter);

    protected final String getString(Map<String, ExprEval> namedParam, String key, Object defaultValue)
    {
      return namedParam.containsKey(key) ? namedParam.get(key).asString() : Objects.toString(defaultValue, null);
    }

    protected final boolean getBoolean(Map<String, ExprEval> namedParam, String key)
    {
      return namedParam.containsKey(key) && namedParam.get(key).asBoolean();
    }

    protected final int getInt(Map<String, ExprEval> namedParam, String key, int defaultValue)
    {
      return namedParam.containsKey(key) ? namedParam.get(key).asInt() : defaultValue;
    }

    protected final double getDouble(Map<String, ExprEval> namedParam, String key, double defaultValue)
    {
      return namedParam.containsKey(key) ? namedParam.get(key).asDouble() : defaultValue;
    }
  }

  abstract class ParameterizingNamedParams extends NamedParams
  {
    @Override
    protected Function toFunction(List<Expr> args, int start, Map<String, ExprEval> parameter)
    {
      return asChild(start, toFunction(parameterize(args.subList(0, start), parameter)));
    }

    protected Map<String, Object> parameterize(List<Expr> exprs, Map<String, ExprEval> namedParam)
    {
      return Maps.newHashMap();
    }

    protected abstract Function toFunction(final Map<String, Object> parameter);
  }

  @Function.Named("size")
  final class Size extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final Object value = Evals.eval(args.get(0), bindings);
          if (value == null) {
            return ExprEval.of(0);
          }
          if (value instanceof Collection) {
            return ExprEval.of(((Collection) value).size());
          }
          if (value.getClass().isArray()) {
            return ExprEval.of(java.lang.reflect.Array.getLength(value));
          }
          throw new IAE("parameter is not a collection");
        }
      };
    }
  }

  @Function.Named("array.string")
  class StringArray extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.STRING_ARRAY;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() == 1 && args.get(0).returns().isArray()) {
        return new Child()
        {
          @Override
          public ValueDesc returns()
          {
            return ValueDesc.LONG_ARRAY;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            List<String> strings = Lists.newArrayList();
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
            }
            for (Object value : (List) eval.value()) {
              strings.add(Objects.toString(value, null));
            }
            return ExprEval.of(strings, ValueDesc.DOUBLE_ARRAY);
          }
        };
      }
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.STRING_ARRAY;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          List<String> strings = Lists.newArrayList();
          for (Expr arg : args) {
            strings.add(Evals.evalString(arg, bindings));
          }
          return ExprEval.of(strings, ValueDesc.STRING_ARRAY);
        }
      };
    }
  }

  @Function.Named("array.long")
  final class LongArray extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG_ARRAY;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() == 1 && args.get(0).returns().isArray()) {
        return new Child()
        {
          @Override
          public ValueDesc returns()
          {
            return ValueDesc.LONG_ARRAY;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            List<Long> longs = Lists.newArrayList();
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
            }
            for (Object value : (List) eval.value()) {
              longs.add(Rows.parseLong(value, null));
            }
            return ExprEval.of(longs, ValueDesc.DOUBLE_ARRAY);
          }
        };
      }
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.LONG_ARRAY;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          List<Long> longs = Lists.newArrayList();
          for (Expr arg : args) {
            longs.add(Evals.evalLong(arg, bindings));
          }
          return ExprEval.of(longs, ValueDesc.LONG_ARRAY);
        }
      };
    }
  }

  @Function.Named("array.double")
  class DoubleArray extends NamedFactory.DoubleArrayType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() == 1 && args.get(0).returns().isArray()) {
        return new DoubleArrayChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            List<Double> doubles = Lists.newArrayList();
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
            }
            for (Object value : (List)eval.value()) {
              doubles.add(Rows.parseDouble(value, null));
            }
            return ExprEval.of(doubles, ValueDesc.DOUBLE_ARRAY);
          }
        };
      }
      return new DoubleArrayChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          List<Double> doubles = Lists.newArrayList();
          for (Expr arg : args) {
            doubles.add(Evals.evalDouble(arg, bindings));
          }
          return ExprEval.of(doubles, ValueDesc.DOUBLE_ARRAY);
        }
      };
    }
  }

  @Function.Named("array")
  final class Array extends DoubleArray
  {
  }

  @Function.Named("regex")
  final class Regex extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IAE("function 'regex' needs 2 or 3 arguments");
      }
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      final int index = args.size() == 3 ? Ints.checkedCast(Evals.getConstantLong(args.get(2))) : 0;

      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          Matcher m = matcher.reset(Evals.evalString(args.get(0), bindings));
          return ExprEval.of(m.find() ? matcher.group(index) : null);
        }
      };
    }
  }

  abstract class AbstractRFunc extends NamedFactory
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
        throw new IAE("function %s is registered already", function);
      }
      if (r.eval(expression) == null) {
        functions.remove(function);
        throw new IAE("invalid expression %s", expression);
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
            exps[j] = exp((REXP) vector.get(j));
          }
          long exp = r.rniPutVector(exps);
          @SuppressWarnings("unchecked")
          Vector<String> names = vector.getNames();
          if (names != null) {
            long attr = r.rniPutStringArray(names.toArray(new String[0]));
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
      ValueDesc type = eval.type();
      switch (type.type()) {
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
        long longValue = (Long) value;
        return longValue == (int) longValue ? new REXP(new int[]{(int) longValue}) : new REXP(new double[]{longValue});
      } else if (value instanceof List) {
        RVector vector = new RVector();
        for (Object element : ((List) value)) {
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
          return new REXP((String[]) value);
        } else if (component == double.class) {
          return new REXP((double[]) value);
        } else if (component == long.class) {
          long[] longs = (long[]) value;
          int[] ints = GuavaUtils.checkedCast(longs);
          return ints != null ? new REXP(ints) : new REXP(GuavaUtils.castDouble(longs));
        } else if (component == int.class) {
          return new REXP((int[]) value);
        }
      }
      return new REXP(new String[]{Objects.toString(value)});
    }

    protected final ExprEval toJava(REXP expr)
    {
      switch (expr.getType()) {
        case REXP.XT_INT:
          return ExprEval.of(expr.asInt());
        case REXP.XT_ARRAY_INT:
          int[] ints = expr.asIntArray();
          return ints.length == 1 ? ExprEval.of(ints[0]) : ExprEval.of(ints, ValueDesc.UNKNOWN);
        case REXP.XT_DOUBLE:
          return ExprEval.of(expr.asDouble());
        case REXP.XT_ARRAY_DOUBLE:
          double[] doubles = expr.asDoubleArray();
          return doubles.length == 1 ? ExprEval.of(doubles[0]) : ExprEval.of(doubles, ValueDesc.UNKNOWN);
        case REXP.XT_STR:
          return ExprEval.of(expr.asString());
        case REXP.XT_ARRAY_STR:
          String[] strings = expr.asStringArray();
          return strings.length == 1 ? ExprEval.of(strings[0]) : ExprEval.of(strings, ValueDesc.UNKNOWN);
        case REXP.XT_VECTOR:
          RVector vector = expr.asVector();
          Vector names = vector.getNames();
          if (names == null) {
            List<Object> result = Lists.newArrayList();
            for (Object element : vector) {
              result.add(toJava((REXP) element).value());
            }
            return ExprEval.of(result, ValueDesc.UNKNOWN);
          }
          Map<String, Object> result = Maps.newLinkedHashMap();
          for (int i = 0; i < names.size(); i++) {
            result.put(String.valueOf(names.get(i)), toJava((REXP) vector.get(i)).value());
          }
          return ExprEval.of(result, ValueDesc.UNKNOWN);
        case REXP.XT_FACTOR:
          RFactor factor = expr.asFactor();
          String[] array = new String[factor.size()];
          for (int i = 0; i < factor.size(); i++) {
            array[i] = factor.at(i);
          }
          return ExprEval.of(array, ValueDesc.UNKNOWN);
        case REXP.XT_LIST:
          // RList.. what the hell is this?
        default:
          return ExprEval.bestEffortOf(expr.getContent());
      }
    }
  }

  @Function.Named("r")
  final class RFunc extends AbstractRFunc
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("function 'r' should have at least two arguments");
      }
      String name = Evals.getConstantString(args.get(1));
      String expression = Evals.getConstantString(args.get(0));
      final String function = registerFunction(name, expression);
      return new ExternalChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return toJava(RFunc.this.evaluate(function, args.subList(2, args.size()), bindings));
        }
      };
    }
  }

  abstract class AbstractPythonFunc extends NamedFactory
  {
    static final boolean init;

    static {
      Properties prop = new Properties();
      String pythonHome = System.getProperty("python.home", System.getProperty("user.home") + "/jython2.7.0");
      if (new File(pythonHome).isDirectory()) {
        prop.setProperty("python.home", pythonHome);
        boolean success = false;
        try {
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
          new PythonInterpreter();
          success = true;
        }
        catch (Throwable e) {
          log.info("failed initialize python interpreter.. disabling python functions");
          // ignore
        }
        init = success;
      } else {
        log.info("invalid or absent of python.home in system environment.. disabling python functions");
        init = false;
      }
    }

    private static final String[] params = new String[]{"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"};

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
        return ExprEval.UNKNOWN;
      }
      if (result instanceof PyString) {
        return ExprEval.of(result.asString(), ValueDesc.STRING);
      }
      if (result instanceof PyFloat) {
        return ExprEval.of(result.asDouble(), ValueDesc.DOUBLE);
      }
      if (result instanceof PyInteger || result instanceof PyLong) {
        return ExprEval.of(result.asLong(), ValueDesc.LONG);
      }
      if (result instanceof PyArray) {
        return ExprEval.of(Arrays.asList(((PyArray) result).getArray()), ValueDesc.LIST);
      }
      if (result instanceof PyList) {
        PyList pyList = (PyList) result;
        List<Object> list = Lists.newArrayList();
        for (int i = 0; i < pyList.size(); i++) {
          list.add(toExprEval(pyList.pyget(i)).value());
        }
        return ExprEval.of(list, ValueDesc.LIST);
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
        return ExprEval.of(map, ValueDesc.MAP);
      }
      if (result instanceof PyTuple) {
        PyObject[] array = ((PyTuple) result).getArray();
        if (evaluation) {
          return toExprEval(array[array.length - 1]);
        }
        List<Object> list = Lists.newArrayList();
        for (PyObject element : array) {
          list.add(toExprEval(element).value());
        }
        return ExprEval.of(list, ValueDesc.STRUCT);
      }
      return ExprEval.of(result.toString());
    }
  }

  @Function.Named("py")
  final class PythonFunc extends AbstractPythonFunc
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (p == null) {
        throw new IAE("python initialization failed..");
      }
      if (args.size() < 2) {
        throw new IAE("function 'py' should have at least two arguments");
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
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (p == null) {
        throw new IAE("python initialization failed..");
      }
      if (args.isEmpty()) {
        throw new IAE("function 'pyEval' should have one argument");
      }
      final PyCode code = p.compile(Evals.getConstantString(args.get(0)));
      return new ExternalChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
    protected float eval(float x)
    {
      return Math.abs(x);
    }

    @Override
    protected double eval(double x)
    {
      return Math.abs(x);
    }

    @Override
    protected long eval(long x)
    {
      return Math.abs(x);
    }
  }

  @Function.Named("acos")
  final class Acos extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.acos(param);
    }
  }

  @Function.Named("asin")
  final class Asin extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.asin(param);
    }
  }

  @Function.Named("atan")
  final class Atan extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.atan(param);
    }
  }

  @Function.Named("cbrt")
  final class Cbrt extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.cbrt(param);
    }
  }

  @Function.Named("ceil")
  final class Ceil extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.ceil(param);
    }
  }

  @Function.Named("cos")
  final class Cos extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.cos(param);
    }
  }

  @Function.Named("cosh")
  final class Cosh extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.cosh(param);
    }
  }

  @Function.Named("exp")
  final class Exp extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.exp(param);
    }
  }

  @Function.Named("expm1")
  final class Expm1 extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.expm1(param);
    }
  }

  @Function.Named("floor")
  final class Floor extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.floor(param);
    }
  }

  @Function.Named("getExponent")
  final class GetExponent extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return ExprEval.of(Math.getExponent(Evals.eval(args.get(0), bindings).asDouble()));
        }
      };
    }
  }

  @Function.Named("log")
  final class Log extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.log(param);
    }
  }

  @Function.Named("log10")
  final class Log10 extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.log10(param);
    }
  }

  @Function.Named("log1p")
  final class Log1p extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.log1p(param);
    }
  }

  @Function.Named("nextUp")
  final class NextUp extends SingleParamRealMath
  {
    @Override
    protected float eval(float param)
    {
      return Math.nextUp(param);
    }

    @Override
    protected double eval(double param)
    {
      return Math.nextUp(param);
    }
  }

  @Function.Named("rint")
  final class Rint extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.rint(param);
    }
  }

  @Function.Named("round")
  final class Round extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("function 'round' needs 1 or 2 arguments");
      }
      final ValueDesc type = args.get(0).returns();
      if (args.size() == 1) {
        return new Child()
        {
          @Override
          public ValueDesc returns()
          {
            return type;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final ExprEval param = Evals.eval(args.get(0), bindings);
            if (param.isNull()) {
              return param;
            }
            ValueDesc type = param.type();
            if (type.isLong()) {
              return param;
            } else if (type.isFloat()) {
              final float value = param.floatValue();
              return ExprEval.of(Float.isNaN(value) || Float.isInfinite(value) ? value : Math.round(value));
            } else if (type.isDouble()) {
              final double value = param.doubleValue();
              return ExprEval.of(Double.isNaN(value) || Double.isInfinite(value) ? value : Math.round(value));
            } else if (type.isDecimal()) {
              BigDecimal decimal = (BigDecimal) param.value();
              return ExprEval.of(decimal.setScale(0, RoundingMode.HALF_UP), type);
            }
            return param;
          }
        };
      }
      final int scale = Evals.getConstantInt(args.get(1));
      if (scale < 0) {
        throw new IAE("2nd argument of 'round' should be positive integer");
      }
      final double x = Math.pow(10, scale);
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return type;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final ExprEval param = Evals.eval(args.get(0), bindings);
          if (param.isNull()) {
            return param;
          }
          ValueDesc type = param.type();
          if (type.isLong()) {
            return param;
          } else if (type.isFloat()) {
            final double value = param.floatValue();
            return ExprEval.of(
                Double.isNaN(value) || Double.isInfinite(value) ? (float) value : (float) Math.round(value * x) / x
            );
          } else if (type.isDouble()) {
            final double value = param.doubleValue();
            return ExprEval.of(Double.isNaN(value) || Double.isInfinite(value) ? value : Math.round(value * x) / x);
          } else if (type.isDecimal()) {
            BigDecimal decimal = (BigDecimal) param.value();
            return ExprEval.of(decimal.setScale(scale, RoundingMode.HALF_UP), type);
          }
          return param;
        }
      };
    }
  }

  @Function.Named("signum")
  final class Signum extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function 'signum' needs 1 argument");
      }
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            return ExprEval.of(Math.signum(Evals.eval(args.get(0), bindings).asFloat()));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            return ExprEval.of(Math.signum(Evals.eval(args.get(0), bindings).asDouble()));
          }
        };
      }
    }
  }

  @Function.Named("sin")
  final class Sin extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.sin(param);
    }
  }

  @Function.Named("sinh")
  final class Sinh extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.sinh(param);
    }
  }

  @Function.Named("sqrt")
  final class Sqrt extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.sqrt(param);
    }
  }

  @Function.Named("tan")
  final class Tan extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.tan(param);
    }
  }

  @Function.Named("tanh")
  final class Tanh extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.tanh(param);
    }
  }

  @Function.Named("toDegrees")
  final class ToDegrees extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.toDegrees(param);
    }
  }

  @Function.Named("toRadians")
  final class ToRadians extends SingleParamDoubleMath
  {
    @Override
    protected double eval(double param)
    {
      return Math.toRadians(param);
    }
  }

  @Function.Named("ulp")
  final class Ulp extends SingleParamRealMath
  {
    @Override
    protected float eval(float param)
    {
      return Math.ulp(param);
    }

    @Override
    protected double eval(double param)
    {
      return Math.ulp(param);
    }
  }

  @Function.Named("atan2")
  final class Atan2 extends DoubleParamDoubleMath
  {
    @Override
    protected double eval(double y, double x)
    {
      return Math.atan2(y, x);
    }
  }

  @Function.Named("copySign")
  final class CopySign extends DoubleParamRealMath
  {
    @Override
    protected float eval(float x, float y)
    {
      return Math.copySign(x, y);
    }

    @Override
    protected double eval(double x, double y)
    {
      return Math.copySign(x, y);
    }
  }

  @Function.Named("hypot")
  final class Hypot extends DoubleParamDoubleMath
  {
    @Override
    protected double eval(double x, double y)
    {
      return Math.hypot(x, y);
    }
  }

  @Function.Named("remainder")
  final class Remainder extends DoubleParamDoubleMath
  {
    @Override
    protected double eval(double x, double y)
    {
      return Math.IEEEremainder(x, y);
    }
  }

  abstract class SingleParamDoubleMath extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function '%s' needs 1 argument", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final Double x = Evals.evalDouble(args.get(0), bindings);
          return ExprEval.of(x == null ? null : eval(x));
        }
      };
    }

    protected abstract double eval(double x);
  }

  abstract class SingleParamRealMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function '%s' needs 1 argument", name());
      }
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            return ExprEval.of(x == null ? null : eval(x));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            return ExprEval.of(x == null ? null : eval(x));
          }
        };
      }
    }

    protected abstract float eval(float x);

    protected abstract double eval(double x);
  }

  abstract class SingleParamMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("Function '%s' needs 1 argument", name());
      }
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            return ExprEval.of(x == null ? null : eval(x));
          }
        };
      } else if (type.isLong()) {
        return new LongChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Long x = Evals.evalLong(args.get(0), bindings);
            return ExprEval.of(x == null ? null : eval(x));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            return ExprEval.of(x == null ? null : eval(x));
          }
        };
      }
    }

    protected abstract float eval(float x);

    protected abstract double eval(double x);

    protected abstract long eval(long x);
  }

  abstract class DoubleParamDoubleMath extends NamedFactory.DoubleType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function '%s' needs 2 arguments", name());
      }
      return new DoubleChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final Double x = Evals.evalDouble(args.get(0), bindings);
          final Double y = Evals.evalDouble(args.get(1), bindings);
          return ExprEval.of(x == null || y == null ? null : eval(x, y));
        }
      };
    }

    protected abstract double eval(double x, double y);
  }

  abstract class DoubleParamRealMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function '%s' needs 2 arguments", name());
      }
      final ValueDesc type1 = args.get(0).returns();
      final ValueDesc type2 = args.get(1).returns();
      if (type1.isFloat() && type2.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            final Float y = Evals.evalFloat(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : eval(x, y));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            final Double y = Evals.evalDouble(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : eval(x, y));
          }
        };
      }
    }

    protected abstract float eval(float x, float y);

    protected abstract double eval(double x, double y);
  }

  abstract class DoubleParamMath extends NamedFactory implements Factory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function '%s' needs 2 arguments", name());
      }
      final ValueDesc type1 = args.get(0).returns();
      final ValueDesc type2 = args.get(1).returns();
      if (type1.isLong() && type2.isLong()) {
        return new LongChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Long x = Evals.evalLong(args.get(0), bindings);
            final Long y = Evals.evalLong(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : eval(x, y));
          }
        };
      } else if (type1.isFloat() && type2.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            final Float y = Evals.evalFloat(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : eval(x, y));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            final Double y = Evals.evalDouble(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : eval(x, y));
          }
        };
      }
    }

    protected abstract long eval(long x, long y);

    protected abstract float eval(float x, float y);

    protected abstract double eval(double x, double y);
  }

  @Function.Named("max")
  final class Max extends DoubleParamMath
  {
    @Override
    protected long eval(long x, long y)
    {
      return Math.max(x, y);
    }

    @Override
    protected float eval(float x, float y)
    {
      return Math.max(x, y);
    }

    @Override
    protected double eval(double x, double y)
    {
      return Math.max(x, y);
    }
  }

  @Function.Named("min")
  final class Min extends DoubleParamMath
  {
    @Override
    protected long eval(long x, long y)
    {
      return Math.min(x, y);
    }

    @Override
    protected float eval(float x, float y)
    {
      return Math.min(x, y);
    }

    @Override
    protected double eval(double x, double y)
    {
      return Math.min(x, y);
    }
  }

  @Function.Named("div")
  final class Div extends DoubleParamMath
  {
    @Override
    protected long eval(long x, long y)
    {
      return x / y;
    }

    @Override
    protected float eval(float x, float y)
    {
      return x / y;
    }

    @Override
    protected double eval(double x, double y)
    {
      return x / y;
    }
  }

  @Function.Named("nextAfter")
  final class NextAfter extends DoubleParamRealMath
  {
    @Override
    protected float eval(float x, float y)
    {
      return Math.nextAfter(x, y);
    }

    @Override
    protected double eval(double x, double y)
    {
      return Math.nextAfter(x, y);
    }
  }

  @Function.Named("pow")
  final class Pow extends DoubleParamDoubleMath
  {
    @Override
    protected double eval(double x, double y)
    {
      return Math.pow(x, y);
    }
  }

  @Function.Named("scalb")
  final class Scalb extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("Function '%s' needs 2 arguments", name());
      }
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            Float x = Evals.evalFloat(args.get(0), bindings);
            Integer y = Evals.evalInt(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : Math.scalb(x, y));
          }
        };
      } else {
        return new DoubleChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            Double x = Evals.evalDouble(args.get(0), bindings);
            Integer y = Evals.evalInt(args.get(1), bindings);
            return ExprEval.of(x == null || y == null ? null : Math.scalb(x, y));
          }
        };
      }
    }
  }

  @Function.Named("if")
  final class IfFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 3) {
        throw new IAE("function 'if' needs at least 3 argument");
      }
      if (args.size() % 2 == 0) {
        throw new IAE("function 'if' needs default value");
      }
      final ValueDesc commonType = returns(args);

      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return commonType;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          for (int i = 0; i < args.size() - 1; i += 2) {
            if (args.get(i).eval(bindings).asBoolean()) {
              return args.get(i + 1).eval(bindings);
            }
          }
          return args.get(args.size() - 1).eval(bindings);
        }
      };
    }

    private ValueDesc returns(List<Expr> args)
    {
      ValueDesc prev = null;
      for (int i = 1; i < args.size() - 1; i += 2) {
        prev = ValueDesc.toCommonType(prev, args.get(i));
        if (prev != null && prev.isUnknown()) {
          return ValueDesc.UNKNOWN;
        }
      }
      final ValueDesc type = ValueDesc.toCommonType(prev, (Expr) GuavaUtils.lastOf(args));
      return Optional.ofNullable(type).orElse(ValueDesc.STRING);
    }
  }

  @Function.Named("cast")
  final class CastFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'cast' needs 2 argument");
      }
      final ValueDesc castTo = ValueDesc.fromTypeString(Evals.getConstantString(args.get(1)));
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return castTo;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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

  @Function.Named("nvl")
  final class NvlFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'nvl' needs 2 arguments");
      }
      final Expr type1 = args.get(0);
      final Expr type2 = args.get(1);
      final ValueDesc common = Optional.ofNullable(ValueDesc.toCommonType(type1, type2)).orElse(ValueDesc.STRING);
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return common;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          if (eval.isNull()) {
            eval = args.get(1).eval(bindings);
          }
          return eval;
        }
      };
    }
  }

  @Function.Named("coalesce")
  final class Coalesce extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.isEmpty()) {
        throw new IAE("function 'coalesce' needs at least 1 argument");
      }
      ValueDesc prev = null;
      for (int i = 0; i < args.size(); i++) {
        prev = ValueDesc.toCommonType(prev, args.get(i));
      }
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return type;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          for (int i = 1; i < args.size() && eval.isNull(); i++) {
            eval = args.get(1).eval(bindings);
          }
          return eval;
        }
      };
    }
  }

  @Function.Named("switch")
  final class SwitchFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 3) {
        throw new IAE("function 'switch' needs at least 3 arguments");
      }
      ValueDesc prev = null;
      for (int i = 2; i < args.size(); i += 2) {
        prev = ValueDesc.toCommonType(prev, args.get(i));
        if (prev != null && prev.equals(ValueDesc.UNKNOWN)) {
          break;
        }
      }
      if (args.size() % 2 != 1) {
        prev = ValueDesc.toCommonType(prev, args.get(args.size() - 1));
      }
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);;
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return type;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
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
      };
    }
  }

  @Function.Named("case")
  final class CaseFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("function 'case' needs at least 2 arguments");
      }
      ValueDesc prev = null;
      for (int i = 1; i < args.size() - 1; i += 2) {
        prev = ValueDesc.toCommonType(prev, args.get(i));
        if (prev != null && prev.equals(ValueDesc.UNKNOWN)) {
          break;
        }
      }
      if (args.size() % 2 == 1) {
        prev = ValueDesc.toCommonType(prev, args.get(args.size() - 1));
      }
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);;
      return new Child()
      {
        @Override
        public ValueDesc returns()
        {
          return type;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final int size = args.size();
          for (int i = 0; i < size - 1; i += 2) {
            ExprEval eval = Evals.eval(args.get(i), bindings);
            if (eval.asBoolean()) {
              return args.get(i + 1).eval(bindings);
            }
          }
          if (size % 2 == 1) {
            return args.get(size - 1).eval(bindings);
          }
          ValueDesc type = null;
          for (int i = 1; i < size - 1; i += 2) {
            type = ValueDesc.toCommonType(type, args.get(i).eval(bindings).type());
          }
          return ExprEval.of(null, Optional.ofNullable(type).orElse(ValueDesc.STRING));
        }
      };
    }
  }

  @Function.Named("javascript")
  final class JavaScriptFunc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'javascript' needs 2 argument");
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
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
  final class ConcatFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          StringBuilder b = new StringBuilder();
          for (Expr expr : args) {
            b.append(Strings.nullToEmpty(expr.eval(bindings).asString()));
          }
          return ExprEval.of(b.toString());
        }
      };
    }
  }

  @Function.Named("format")
  final class FormatFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.isEmpty()) {
        throw new IAE("function 'format' needs at least 1 argument");
      }
      final String format = Evals.getConstantString(args.get(0));
      final Object[] formatArgs = new Object[args.size() - 1];
      return new StringChild()
      {
        final StringBuilder builder = new StringBuilder();
        final Formatter formatter = new Formatter(builder);

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
  final class LPadFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 3) {
        throw new IAE("function 'lpad' needs 3 arguments");
      }
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new IAE("3rd argument of function 'lpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return ExprEval.of(input == null ? null : Strings.padStart(input, length, padding));
        }
      };
    }
  }

  @Function.Named("rpad")
  final class RPadFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 3) {
        throw new IAE("function 'rpad' needs 3 arguments");
      }
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new IAE("3rd argument of function 'rpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return ExprEval.of(input == null ? null : Strings.padEnd(input, length, padding));
        }
      };
    }
  }

  @Function.Named("upper")
  final class UpperFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function 'upper' needs 1 argument");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return ExprEval.of(input == null ? null : input.toUpperCase());
        }
      };
    }
  }

  @Function.Named("lower")
  final class LowerFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function 'lower' needs 1 argument");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return ExprEval.of(input == null ? null : input.toLowerCase());
        }
      };
    }
  }

  // pattern
  @Function.Named("splitRegex")
  final class SplitRegex extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 3) {
        throw new IAE("function 'splitRegex' needs 3 arguments");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
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
      };
    }
  }

  @Function.Named("split")
  final class Split extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2 && args.size() != 3) {
        throw new IAE("function 'split' needs 2 or 3 arguments");
      }
      final Splitter splitter;
      final String separator = Evals.getConstantString(args.get(1));
      if (separator.length() == 1) {
        splitter = Splitter.on(separator.charAt(0));
      } else {
        splitter = Splitter.on(separator);
      }
      if (args.size() == 2) {
        return new Child()
        {
          @Override
          public ValueDesc returns()
          {
            return ValueDesc.STRING_ARRAY;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final ExprEval inputEval = args.get(0).eval(bindings);
            if (inputEval.isNull()) {
              return ExprEval.of(null, ValueDesc.STRING_ARRAY);
            }
            final String input = inputEval.asString();
            return ExprEval.of(Lists.newArrayList(splitter.split(input)), ValueDesc.STRING_ARRAY);
          }
        };
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
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
  final class ProperFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function 'proper' needs 1 argument");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return ExprEval.of(
              Strings.isNullOrEmpty(input) ? input :
              Character.toUpperCase(input.charAt(0)) + input.substring(1).toLowerCase()
          );
        }
      };
    }
  }

  @Function.Named("length")
  class LengthFunc extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function '%s' needs 1 argument", name());
      }
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return ExprEval.of(input == null ? 0 : input.length());
        }
      };
    }
  }

  @Function.Named("strlen")
  final class StrlenFunc extends LengthFunc
  {
  }

  @Function.Named("left")
  final class LeftFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'left' needs 2 arguments");
      }
      if (Evals.isConstant(args.get(1))) {
        final int index = Evals.getConstantInt(args.get(1));
        return new StringChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final String input = Evals.evalString(args.get(0), bindings);
            if (input == null) {
              return ExprEval.of(input);
            }
            final int length = input.length();
            if (index == 0 || length == 0) {
              return ExprEval.of("");
            }
            if (index < 0) {
              final int endIndex = length + index;
              return ExprEval.of(endIndex < 0 ? "" : input.substring(0, length + index));
            }
            return ExprEval.of(index > length ? input : input.substring(0, index));
          }
        };
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final String input = Evals.evalString(args.get(0), bindings);
          if (input == null) {
            return ExprEval.of(input);
          }
          final int index = Evals.evalInt(args.get(1), bindings);
          final int length = input.length();
          if (index == 0 || length == 0) {
            return ExprEval.of("");
          }
          if (index < 0) {
            final int endIndex = length + index;
            return ExprEval.of(endIndex < 0 ? "" : input.substring(0, length + index));
          }
          return ExprEval.of(index > length ? input : input.substring(0, index));
        }
      };
    }
  }

  @Function.Named("right")
  final class RightFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'right' needs 2 arguments");
      }
      if (Evals.isConstant(args.get(1))) {
        final int index = Evals.getConstantInt(args.get(1));
        return new StringChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final String input = args.get(0).eval(bindings).asString();
            if (input == null) {
              return ExprEval.of(input);
            }
            final int length = input.length();
            if (index == 0 || length == 0) {
              return ExprEval.of("");
            }
            if (index < 0) {
              return ExprEval.of(length + index < 0 ? "" : input.substring(-index));
            }
            final int startIndex = length - index;
            return ExprEval.of(startIndex < 0 ? input : input.substring(startIndex));
          }
        };
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final String input = args.get(0).eval(bindings).asString();
          if (input == null) {
            return ExprEval.of(input);
          }
          final int length = input.length();
          final int index = Evals.evalInt(args.get(0), bindings);
          if (index == 0 || length == 0) {
            return ExprEval.of("");
          }
          if (index < 0) {
            return ExprEval.of(length + index < 0 ? "" : input.substring(-index));
          }
          final int startIndex = length - index;
          return ExprEval.of(startIndex < 0 ? input : input.substring(startIndex));
        }
      };
    }
  }

  @Function.Named("mid")
  class MidFunc extends NamedFactory.StringType
  {
    @Override
    public final Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 3) {
        throw new IAE("function '" + name() + "' needs 3 arguments");
      }
      if (Evals.isConstant(args.get(1)) && Evals.isConstant(args.get(2))) {
        final int start = Evals.getConstantInt(args.get(1));
        final int end = Evals.getConstantInt(args.get(2));
        return new StringChild()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            String input = Evals.evalString(args.get(0), bindings);
            return eval(input, start, end);
          }
        };
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return eval(input, Evals.evalInt(args.get(1), bindings), Evals.evalInt(args.get(2), bindings));
        }
      };
    }

    protected ExprEval eval(String input, int start, int end)
    {
      if (input == null || start >= input.length()) {
        return ExprEval.of(null, ValueDesc.STRING);
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
        return ExprEval.of(null, ValueDesc.STRING);
      }
      if (length < 0) {
        return ExprEval.of(input.substring(start));
      } else {
        return ExprEval.of(input.substring(start, Math.min(start + length, input.length())));
      }
    }
  }

  @Function.Named("indexOf")
  final class IndexOfFunc extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'indexOf' needs 2 arguments");
      }
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String find = args.get(1).eval(bindings).asString();

          return ExprEval.of(Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? -1 : input.indexOf(find));
        }
      };
    }
  }

  @Function.Named("countOf")
  final class CountOfFunc extends NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'countOf' needs 2 arguments");
      }
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String find = args.get(1).eval(bindings).asString();
          Preconditions.checkArgument(!Strings.isNullOrEmpty(find), "find string cannot be null or empty");
          if (Strings.isNullOrEmpty(input)) {
            return ExprEval.of(0);
          }
          int counter = 0;
          int findLen = find.length();
          for (int i = 0; i < input.length(); i++) {
            int index = input.indexOf(find, i);
            if (index < 0) {
              break;
            }
            i = index + findLen;
            counter++;
          }
          return ExprEval.of(counter);
        }
      };
    }
  }

  @Function.Named("replace")
  final class ReplaceFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 3) {
        throw new IAE("function 'replace' needs 3 arguments");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String find = args.get(1).eval(bindings).asString();
          String replace = args.get(2).eval(bindings).asString();
          return ExprEval.of(
              Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? input :
              StringUtils.replace(input, find, replace)
          );
        }
      };
    }
  }

  @Function.Named("trim")
  final class TrimFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function 'trim' needs 1 argument");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return ExprEval.of(Strings.isNullOrEmpty(input) ? input : input.trim());
        }
      };
    }
  }

  // sql
  @Function.Named("btrim")
  final class BtrimFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("function 'btrim' needs 1 or 2 arguments");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return ExprEval.of(StringUtils.stripEnd(StringUtils.stripStart(input, strip), strip));
        }
      };
    }
  }

  // sql
  @Function.Named("ltrim")
  final class LtrimFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("function 'ltrim' needs 1 or 2 arguments");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return ExprEval.of(StringUtils.stripStart(input, strip));
        }
      };
    }
  }

  // sql
  @Function.Named("rtrim")
  final class RtrimFunc extends NamedFactory.StringType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("function 'rtrim' needs 1 or 2 arguments");
      }
      return new StringChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return ExprEval.of(StringUtils.stripEnd(input, strip));
        }
      };
    }
  }

  @Function.Named("struct")
  final class Struct extends NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.STRUCT;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.STRUCT;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final Object[] array = new Object[args.size()];
          for (int i = 0; i < array.length; i++) {
            array[i] = args.get(i).eval(bindings).value();
          }
          return ExprEval.of(array, ValueDesc.STRUCT);
        }
      };
    }
  }

  @Function.Named("struct_desc")
  final class StructDesc extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("function 'struct_desc' needs at least 2 arguments");
      }
      final String desc = Evals.getConstantString(GuavaUtils.lastOf(args));
      final String[] split = desc.split(",");
      final String[] fieldNames = new String[split.length];
      final ValueDesc[] fieldTypes = new ValueDesc[split.length];
      int i = 0;
      for (String field : split) {
        int index = field.indexOf(':');
        fieldNames[i] = field.substring(0, index);
        fieldTypes[i] = ValueDesc.of(field.substring(index + 1));
        i++;
      }
      final ValueDesc struct = ValueDesc.ofStruct(fieldNames, fieldTypes);
      if (args.size() == 2 && args.get(0).returns().isArray()) {
        // todo it's kind of type cast.. later
        return new Function()
        {
          @Override
          public ValueDesc returns()
          {
            return struct;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            final ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return new ExprEval(null, struct);
            }
            final List value = (List) eval.value();
            final Object[] array = new Object[fieldTypes.length];
            final int max = Math.max(array.length, value.size());
            for (int i = 0; i < max; i++) {
              array[i] = fieldTypes[i].cast(value.get(i));
            }
            return ExprEval.of(array, struct);
          }
        };
      }

      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return struct;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          final Object[] array = new Object[fieldTypes.length];
          for (int i = 0; i < fieldTypes.length; i++) {
            array[i] = fieldTypes[i].cast(args.get(i).eval(bindings).value());
          }
          return ExprEval.of(array, struct);
        }
      };
    }
  }

  abstract class WindowFunctionFactory extends NamedFactory
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

    protected abstract class WindowFunction implements Function
    {
      protected final WindowContext context;

      protected final String inputField;
      protected final ValueDesc inputType;
      protected final Object[] parameters;

      protected WindowFunction(List<Expr> args, WindowContext context)
      {
        this.context = context;
        if (args.size() > 0) {
          inputField = Evals.getIdentifier(args.get(0));   // todo can be expression
          inputType = context.resolve(inputField, ValueDesc.UNKNOWN);
          parameters = Evals.getConstants(args.subList(1, args.size()));
        } else {
          inputField = "$$$";
          inputType = ValueDesc.UNKNOWN;
          parameters = new Object[0];
        }
      }

      @Override
      public ValueDesc returns()
      {
        return inputType;
      }

      protected void init() { }
    }
  }

  abstract class StatelessWindowFunctionFactory extends WindowFunctionFactory
  {
    private final ValueDesc outputType;

    public StatelessWindowFunctionFactory(ValueDesc outputType)
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
      protected StatelessWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return ExprEval.of(invoke(context, inputField), returns());
      }

      @Override
      public ValueDesc returns()
      {
        return outputType != null ? outputType : inputType;
      }
    }

    protected abstract Object invoke(WindowContext context, String fieldName);
  }

  abstract class SimpleWindowFunctionFactory extends StatelessWindowFunctionFactory
  {
    public SimpleWindowFunctionFactory()
    {
      super(null);
    }
  }

  abstract class WindowSupport extends WindowFunctionFactory
  {
    protected abstract class WindowSupportFunction extends WindowFunction
    {
      protected final int[] window;

      protected WindowSupportFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        if (parameters.length >= 2) {
          window = new int[]{Integer.MIN_VALUE, 0};
          if (!"?".equals(parameters[parameters.length - 2])) {
            window[0] = ((Number) parameters[parameters.length - 2]).intValue();
          }
          if (!"?".equals(parameters[parameters.length - 1])) {
            window[1] = ((Number) parameters[parameters.length - 1]).intValue();
          }
        } else {
          window = null;
        }
      }

      @Override
      public final ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        if (window != null) {
          init();
          for (Object object : context.iterator(window[0], window[1], inputField)) {
            if (object != null) {
              invoke(object, context);
            }
          }
        } else {
          Object current = context.get(inputField);
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
  final class Prev extends SimpleWindowFunctionFactory
  {
    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return context.get(context.index() - 1, fieldName);
    }
  }

  @Function.Named("$next")
  final class Next extends SimpleWindowFunctionFactory
  {
    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return context.get(context.index() + 1, fieldName);
    }
  }

  @Function.Named("$last")
  final class Last extends SimpleWindowFunctionFactory
  {
    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return context.get(context.size() - 1, fieldName);
    }
  }

  @Function.Named("$first")
  final class First extends SimpleWindowFunctionFactory
  {
    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return context.get(0, fieldName);
    }
  }

  @Function.Named("$nth")
  final class Nth extends WindowFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      if (args.size() != 2) {
        throw new IAE("function '$nth' needs 2 argument");
      }
      return new NthWindowFunction(args, context);
    }

    protected final class NthWindowFunction extends WindowFunction
    {
      private final int nth;

      protected NthWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        nth = ((Number) parameters[0]).intValue() - 1;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return ExprEval.of(context.get(nth, inputField), inputType);
      }
    }
  }

  @Function.Named("$lag")
  final class Lag extends WindowFunctionFactory implements Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      if (args.size() != 2) {
        throw new IAE("function '$lag' needs 2 arguments");
      }
      return new LagWindowFunction(args, context);
    }

    protected final class LagWindowFunction extends WindowFunction
    {
      private final int delta;

      protected LagWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        delta = ((Number) parameters[0]).intValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return ExprEval.of(context.get(context.index() - delta, inputField), inputType);
      }
    }
  }

  @Function.Named("$lead")
  final class Lead extends WindowFunctionFactory implements Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      if (args.size() != 2) {
        throw new IAE("function '$lead' needs 2 arguments");
      }
      return new LeadWindowFunction(args, context);
    }

    protected final class LeadWindowFunction extends WindowFunction
    {
      private final int delta;

      protected LeadWindowFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        delta = ((Number) parameters[0]).intValue();
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        return ExprEval.of(context.get(context.index() + delta, inputField), inputType);
      }
    }
  }

  @Function.Named("$delta")
  final class RunningDelta extends WindowFunctionFactory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      if (args.size() != 1) {
        throw new IAE("function '$delta' needs 1 argument");
      }
      return new DeltaWindowFunction(args, context);
    }

    protected final class DeltaWindowFunction extends WindowFunction
    {
      private long longPrev;
      private float floatPrev;
      private double doublePrev;

      protected DeltaWindowFunction(List<Expr> args, WindowContext context)
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
        Object current = context.get(inputField);
        if (current == null) {
          return ExprEval.of(null, inputType);
        }
        if (context.index() == 0) {
          switch (inputType.type()) {
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
              throw new ISE("unsupported type %s", inputType);
          }
        }
        switch (inputType.type()) {
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
            throw new ISE("unsupported type %s", inputType);
        }
      }
    }
  }

  @Function.Named("$sum")
  class RunningSum extends WindowSupport implements Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningSumFunction(args, context);
    }

    class RunningSumFunction extends WindowSupportFunction
    {
      private long longSum;
      private double doubleSum;

      protected RunningSumFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return inputType.type() == ValueType.LONG ? ValueDesc.LONG : ValueDesc.DOUBLE;
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
        if (current == null) {
          return;
        }
        switch (inputType.type()) {
          case LONG:
            longSum += ((Number) current).longValue();
            break;
          case FLOAT:
          case DOUBLE:
            doubleSum += ((Number) current).doubleValue();
            break;
          default:
            throw new ISE("unsupported type %s", inputType);
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        if (inputType.isLong()) {
          return ExprEval.of(longSum);
        } else {
          return ExprEval.of(doubleSum);
        }
      }
    }
  }

  @Function.Named("$min")
  final class RunningMin extends WindowSupport implements Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMinFunction(args, context);
    }

    private class RunningMinFunction extends WindowSupportFunction
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
        if (comparable != null && (prev == null || comparable.compareTo(prev) < 0)) {
          prev = comparable;
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(prev, inputType);
      }

      @Override
      protected void init()
      {
        prev = null;
      }
    }
  }

  @Function.Named("$max")
  final class RunningMax extends WindowSupport implements Factory
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMaxFunction(args, context);
    }

    private class RunningMaxFunction extends WindowSupportFunction
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
        if (comparable != null && (prev == null || comparable.compareTo(prev) > 0)) {
          prev = comparable;
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(prev, inputType);
      }

      @Override
      protected void init()
      {
        prev = null;
      }
    }
  }

  @Function.Named("$row_num")
  final class RowNum extends StatelessWindowFunctionFactory implements Function.FixedTyped
  {
    public RowNum()
    {
      super(ValueDesc.LONG);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return context.index() + 1L;
    }
  }

  @Function.Named("$rank")
  final class Rank extends WindowFunctionFactory implements Function.FixedTyped
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RankFunction(args, context);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    private class RankFunction extends WindowFunction
    {
      private long prevRank;
      private Object prev;

      protected RankFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.LONG;
      }

      @Override
      public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
      {
        final Object current = context.get(inputField);
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
  final class DenseRank extends WindowFunctionFactory implements Factory, Function.FixedTyped
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new DenseRankFunction(args, context);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    private class DenseRankFunction extends WindowFunction
    {
      private long prevRank;
      private Object prev;

      protected DenseRankFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.LONG;
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
        final Object current = context.get(inputField);
        if (context.index() == 0 || !Objects.equals(prev, current)) {
          prev = current;
          prevRank++;
        }
        return ExprEval.of(prevRank);
      }
    }
  }

  @Function.Named("$mean")
  class RunningMean extends RunningSum implements Function.FixedTyped
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new RunningMeanFunction(args, context);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.DOUBLE;
    }

    class RunningMeanFunction extends RunningSumFunction
    {
      private int count;

      protected RunningMeanFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }

      public void init()
      {
        super.init();
        count = 0;
      }

      @Override
      protected void invoke(Object current, WindowContext context)
      {
        super.invoke(current, context);
        if (current != null) {
          count++;
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(count == 0 ? null : super.current(context).asDouble() / count, ValueDesc.DOUBLE);
      }
    }
  }

  @Function.Named("$avg")
  class RunningAvg extends RunningMean
  {
  }

  @Function.Named("$variance")
  class RunningVariance extends WindowSupport implements Function.FixedTyped
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new VarianceFunction(args, context);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.DOUBLE;
    }

    class VarianceFunction extends WindowSupportFunction
    {
      long count; // number of elements
      double sum; // sum of elements
      double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

      protected VarianceFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      public ValueDesc returns()
      {
        return ValueDesc.DOUBLE;
      }

      @Override
      public void init()
      {
        count = 0;
        sum = 0;
        nvariance = 0;
      }

      @Override
      public void invoke(Object current, WindowContext context)
      {
        if (current != null) {
          double v = ((Number) current).doubleValue();
          count++;
          sum += v;
          if (count > 1) {
            double t = count * v - sum;
            nvariance += (t * t) / ((double) count * (count - 1));
          }
        }
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(count == 0 ? null : count == 1 ? 0d : nvariance / (count - 1), ValueDesc.DOUBLE);
      }
    }
  }

  @Function.Named("$stddev")
  final class RunningStandardDeviation extends RunningVariance
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StddevFunction(args, context);
    }

    class StddevFunction extends VarianceFunction
    {
      protected StddevFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        final ExprEval current = super.current(context);
        return current.isNull() ? current : ExprEval.of(Math.sqrt(current.doubleValue()));
      }
    }
  }

  @Function.Named("$variancePop")
  class RunningVariancePop extends RunningVariance
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new VariancePopFunction(args, context);
    }

    class VariancePopFunction extends VarianceFunction
    {
      protected VariancePopFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        return ExprEval.of(count == 0 ? null : count == 1 ? 0d : nvariance / count, ValueDesc.DOUBLE);
      }
    }
  }

  @Function.Named("$stddevPop")
  final class RunningStandardDeviationPop extends RunningVariancePop
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new StddevPopFunction(args, context);
    }

    class StddevPopFunction extends VariancePopFunction
    {
      protected StddevPopFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        final ExprEval current = super.current(context);
        return current.isNull() ? current : ExprEval.of(Math.sqrt(current.doubleValue()));
      }
    }
  }

  @Function.Named("$percentile")
  final class RunningPercentile extends WindowSupport
  {
    @Override
    protected WindowFunction newInstance(List<Expr> args, WindowContext context)
    {
      return new PercentileFunction(args, context);
    }

    private class PercentileFunction extends WindowSupportFunction
    {
      private final ValueType type;
      private final float percentile;

      private int index;
      private long[] longs;
      private float[] floats;
      private double[] doubles;

      protected PercentileFunction(List<Expr> args, WindowContext context)
      {
        super(args, context);
        if (parameters.length == 0 || !(parameters[0] instanceof Number)) {
          throw new IAE("function 'percentile' needs 1 ratio argument");
        }
        Preconditions.checkArgument(inputType.isPrimitiveNumeric());
        type = inputType.type();
        percentile = ((Number) parameters[0]).floatValue();
        if (percentile < 0 || percentile > 1) {
          throw new IAE("percentile should be in [0 ~ 1]");
        }
      }

      @Override
      public void init()
      {
        int limit = window == null ? context.size() : sizeOfWindow();
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
      protected void invoke(Object current, WindowContext context)
      {
        if (current == null) {
          return;
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
      }

      @Override
      protected ExprEval current(WindowContext context)
      {
        final int x = (int) (index * percentile);
        if (type == ValueType.LONG) {
          Arrays.sort(longs, 0, index);
          return ExprEval.of(longs[x]);
        } else if (type == ValueType.FLOAT) {
          Arrays.sort(floats, 0, index);
          return ExprEval.of(floats[x]);
        } else {
          Arrays.sort(doubles, 0, index);
          return ExprEval.of(doubles[x]);
        }
      }
    }
  }

  @Function.Named("$histogram")
  final class Histogram extends WindowFunctionFactory implements Function.FixedTyped
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

        if (parameters.length == 0) {
          throw new IAE(name() + " should have at least one argument (binCount)");
        }
        Preconditions.checkArgument(inputType.isPrimitiveNumeric());
        type = inputType.type();

        binCount = ((Number) parameters[0]).intValue();

        from = parameters.length > 1 ? ((Number) parameters[1]).doubleValue() : Double.MAX_VALUE;
        step = parameters.length > 2 ? ((Number) parameters[2]).doubleValue() : Double.MAX_VALUE;
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
        final Object current = context.get(inputField);
        if (current == null) {
          return ExprEval.of(null, ValueDesc.MAP);
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
        return ExprEval.of(context.hasMore() ? null : toHistogram(), ValueDesc.MAP);
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
  final class PartitionSize extends StatelessWindowFunctionFactory implements Function.FixedTyped
  {
    public PartitionSize()
    {
      super(ValueDesc.LONG);
    }

    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG;
    }

    @Override
    protected Object invoke(WindowContext context, String fieldName)
    {
      return (long) context.size();
    }
  }

  @Function.Named("$assign")
  final class PartitionEval extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      return new UnknownChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          if (args.isEmpty()) {
            throw new IAE(name() + " should have at least output field name");
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
      return new UnknownChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          if (args.size() != 1) {
            throw new IAE(name() + " should have one argument (output field name)");
          }
          return ExprEval.of(new Object[]{Evals.evalString(args.get(0), bindings), 0, 1}, ValueDesc.STRUCT);
        }
      };
    }
  }
}
