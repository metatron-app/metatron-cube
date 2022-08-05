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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Expr.NumericBinding;
import io.druid.math.expr.Function.Factory;
import io.druid.math.expr.Function.NamedFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
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
  static final Logger LOG = new Logger(BuiltinFunctions.class);

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

      return wrap(namedParamStart, function);
    }

    protected final Function wrap(final int namedParamStart, final Function function)
    {
      return new Function()
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
      return wrap(start, toFunction(parameterize(args.size() == start ? args : args.subList(0, start), parameter)));
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
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, NumericBinding bindings)
        {
          final Object value = Evals.evalValue(args.get(0), bindings);
          if (value == null) {
            return 0;
          }
          if (value instanceof Collection) {
            return ((Collection) value).size();
          }
          if (value.getClass().isArray()) {
            return java.lang.reflect.Array.getLength(value);
          }
          return 1;
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
      if (args.size() == 1 && args.get(0).returns().isArrayOrStruct()) {
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
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.STRING_ARRAY);
            }
            List<String> strings = Lists.newArrayList();
            for (Object value : (List) eval.value()) {
              strings.add(Objects.toString(value, null));
            }
            return ExprEval.of(strings, ValueDesc.STRING_ARRAY);
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
      if (args.size() == 1 && args.get(0).returns().isArrayOrStruct()) {
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
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.LONG_ARRAY);
            }
            List<Long> longs = Lists.newArrayList();
            for (Object value : (List) eval.value()) {
              longs.add(Rows.parseLong(value, null));
            }
            return ExprEval.of(longs, ValueDesc.LONG_ARRAY);
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
    public DoubleArrayFunc create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() == 1 && args.get(0).returns().isArrayOrStruct()) {
        return new DoubleArrayFunc()
        {
          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            ExprEval eval = Evals.eval(args.get(0), bindings);
            if (eval.isNull()) {
              return ExprEval.of(null, ValueDesc.DOUBLE_ARRAY);
            }
            List<Double> doubles = Lists.newArrayList();
            for (Object value : (List) eval.value()) {
              doubles.add(Rows.parseDouble(value, null));
            }
            return ExprEval.of(doubles, ValueDesc.DOUBLE_ARRAY);
          }
        };
      }
      return new DoubleArrayFunc()
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
  final class Array extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() == 1 && args.get(0).returns().isArrayOrStruct()) {
        ValueDesc type = args.get(0).returns();
        return new Function()
        {
          public ValueDesc returns()
          {
            return type;
          }

          @Override
          public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
          {
            return args.get(0).eval(bindings);
          }
        };
      }
      ValueDesc element = ValueDesc.toCommonType(Iterables.transform(args, arg -> arg.returns()), ValueDesc.UNKNOWN);
      ValueDesc type = element.isUnknown() ? ValueDesc.ARRAY : ValueDesc.ofArray(element);
      return new Function()
      {
        public ValueDesc returns()
        {
          return type;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          List values = Lists.newArrayList();
          for (Expr arg : args) {
            values.add(type.cast(Evals.evalValue(arg, bindings)));
          }
          return ExprEval.of(values, type);
        }
      };
    }
  }

  @Function.Named("regex")
  class Regex extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      twoOrThree(args);
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      final int index = args.size() == 3 ? Evals.getConstantInt(args.get(2)) : 0;

      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String target = Evals.evalString(args.get(0), bindings);
          return target == null || !isMatched(matcher, target) ? null : matcher.group(index);
        }
      };
    }

    protected boolean isMatched(Matcher matcher, String target)
    {
      return matcher.reset(target).find();
    }
  }

  @Function.Named("regex.match")
  final class RegexMatch extends Regex
  {
    protected boolean isMatched(Matcher matcher, String target)
    {
      return matcher.reset(target).matches();
    }
  }

  abstract class AbstractRFunc extends NamedFactory implements Function.External
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
          LOG.warn(e, "Failed to initialize r");
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
      atLeastTwo(args);
      String name = Evals.getConstantString(args.get(1));
      String expression = Evals.getConstantString(args.get(0));
      final String function = registerFunction(name, expression);
      return new ExternalFunc()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, NumericBinding bindings)
        {
          return toJava(RFunc.this.evaluate(function, args.subList(2, args.size()), bindings));
        }
      };
    }
  }

  abstract class AbstractPythonFunc extends NamedFactory implements Function.External
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
                  return o instanceof Map;
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
                  return o instanceof List;
                }
              }
          );
          new PythonInterpreter();
          success = true;
        }
        catch (Throwable e) {
          LOG.info("failed initialize python interpreter.. disabling python functions");
          // ignore
        }
        init = success;
      } else {
        LOG.info("invalid or absent of python.home in system environment.. disabling python functions");
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
        return ExprEval.of(Arrays.asList(((PyArray) result).getArray()), ValueDesc.ARRAY);
      }
      if (result instanceof PyList) {
        PyList pyList = (PyList) result;
        List<Object> list = Lists.newArrayList();
        for (int i = 0; i < pyList.size(); i++) {
          list.add(toExprEval(pyList.pyget(i)).value());
        }
        return ExprEval.of(list, ValueDesc.ARRAY);
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
      atLeastTwo(args);
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
        return new ExternalFunc()
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
      return new ExternalFunc()
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
      exactOne(args);
      final PyCode code = p.compile(Evals.getConstantString(args.get(0)));
      return new ExternalFunc()
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
    protected float _eval(float x)
    {
      return Math.abs(x);
    }

    @Override
    protected double _eval(double x)
    {
      return Math.abs(x);
    }

    @Override
    protected long _eval(long x)
    {
      return Math.abs(x);
    }
  }

  @Function.Named("acos")
  final class Acos extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.acos(param);
    }
  }

  @Function.Named("asin")
  final class Asin extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.asin(param);
    }
  }

  @Function.Named("atan")
  final class Atan extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.atan(param);
    }
  }

  @Function.Named("cbrt")
  final class Cbrt extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.cbrt(param);
    }
  }

  @Function.Named("ceil")
  final class Ceil extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.ceil(param);
    }
  }

  @Function.Named("cos")
  final class Cos extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.cos(param);
    }
  }

  @Function.Named("cosh")
  final class Cosh extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.cosh(param);
    }
  }

  @Function.Named("exp")
  final class Exp extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.exp(param);
    }
  }

  @Function.Named("expm1")
  final class Expm1 extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.expm1(param);
    }
  }

  @Function.Named("floor")
  final class Floor extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
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
      return new Function()
      {
        @Override
        public ValueDesc returns()
        {
          return ValueDesc.LONG;
        }

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
    protected double _eval(double param)
    {
      return Math.log(param);
    }
  }

  @Function.Named("log10")
  final class Log10 extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.log10(param);
    }
  }

  @Function.Named("log1p")
  final class Log1p extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.log1p(param);
    }
  }

  @Function.Named("nextUp")
  final class NextUp extends SingleParamRealMath
  {
    @Override
    protected float _eval(float param)
    {
      return Math.nextUp(param);
    }

    @Override
    protected double _eval(double param)
    {
      return Math.nextUp(param);
    }
  }

  @Function.Named("rint")
  final class Rint extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
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
      oneOrTwo(args);
      final ValueDesc type = args.get(0).returns();
      if (args.size() == 1) {
        return new Function()
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
      return new Function()
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
      exactOne(args);
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            return Math.signum(Evals.eval(args.get(0), bindings).asFloat());
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            return Math.signum(Evals.eval(args.get(0), bindings).asDouble());
          }
        };
      }
    }
  }

  @Function.Named("sin")
  final class Sin extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.sin(param);
    }
  }

  @Function.Named("sinh")
  final class Sinh extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.sinh(param);
    }
  }

  @Function.Named("sqrt")
  final class Sqrt extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.sqrt(param);
    }
  }

  @Function.Named("tan")
  final class Tan extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.tan(param);
    }
  }

  @Function.Named("tanh")
  final class Tanh extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.tanh(param);
    }
  }

  @Function.Named("toDegrees")
  final class ToDegrees extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.toDegrees(param);
    }
  }

  @Function.Named("toRadians")
  final class ToRadians extends SingleParamDoubleMath
  {
    @Override
    protected double _eval(double param)
    {
      return Math.toRadians(param);
    }
  }

  @Function.Named("ulp")
  final class Ulp extends SingleParamRealMath
  {
    @Override
    protected float _eval(float param)
    {
      return Math.ulp(param);
    }

    @Override
    protected double _eval(double param)
    {
      return Math.ulp(param);
    }
  }

  @Function.Named("atan2")
  final class Atan2 extends DoubleParamDoubleMath
  {
    @Override
    protected double _eval(double y, double x)
    {
      return Math.atan2(y, x);
    }
  }

  @Function.Named("copySign")
  final class CopySign extends DoubleParamRealMath
  {
    @Override
    protected float _eval(float x, float y)
    {
      return Math.copySign(x, y);
    }

    @Override
    protected double _eval(double x, double y)
    {
      return Math.copySign(x, y);
    }
  }

  @Function.Named("hypot")
  final class Hypot extends DoubleParamDoubleMath
  {
    @Override
    protected double _eval(double x, double y)
    {
      return Math.hypot(x, y);
    }
  }

  @Function.Named("remainder")
  final class Remainder extends DoubleParamDoubleMath
  {
    @Override
    protected double _eval(double x, double y)
    {
      return Math.IEEEremainder(x, y);
    }
  }

  abstract class SingleParamDoubleMath extends NamedFactory.DoubleType
  {
    @Override
    public DoubleFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new DoubleFunc()
      {
        @Override
        public Double eval(List<Expr> args, NumericBinding bindings)
        {
          final Double x = Evals.evalDouble(args.get(0), bindings);
          return x == null ? null : _eval(x);
        }
      };
    }

    protected abstract double _eval(double x);
  }

  abstract class SingleParamRealMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            return x == null ? null : _eval(x);
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            return x == null ? null : _eval(x);
          }
        };
      }
    }

    protected abstract float _eval(float x);

    protected abstract double _eval(double x);
  }

  abstract class SingleParamMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            return x == null ? null : _eval(x);
          }
        };
      } else if (type.isLong()) {
        return new LongFunc()
        {
          @Override
          public Long eval(List<Expr> args, NumericBinding bindings)
          {
            final Long x = Evals.evalLong(args.get(0), bindings);
            return x == null ? null : _eval(x);
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            return x == null ? null : _eval(x);
          }
        };
      }
    }

    protected abstract float _eval(float x);

    protected abstract double _eval(double x);

    protected abstract long _eval(long x);
  }

  abstract class DoubleParamDoubleMath extends NamedFactory.DoubleType
  {
    @Override
    public DoubleFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      return new DoubleFunc()
      {
        @Override
        public Double eval(List<Expr> args, NumericBinding bindings)
        {
          final Double x = Evals.evalDouble(args.get(0), bindings);
          final Double y = Evals.evalDouble(args.get(1), bindings);
          return x == null || y == null ? null : _eval(x, y);
        }
      };
    }

    protected abstract double _eval(double x, double y);
  }

  abstract class DoubleParamRealMath extends NamedFactory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      final ValueDesc type1 = args.get(0).returns();
      final ValueDesc type2 = args.get(1).returns();
      if (type1.isFloat() && type2.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            final Float y = Evals.evalFloat(args.get(1), bindings);
            return x == null || y == null ? null : _eval(x, y);
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            final Double y = Evals.evalDouble(args.get(1), bindings);
            return x == null || y == null ? null : _eval(x, y);
          }
        };
      }
    }

    protected abstract float _eval(float x, float y);

    protected abstract double _eval(double x, double y);
  }

  abstract class DoubleParamMath extends NamedFactory implements Factory
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      final ValueDesc type1 = args.get(0).returns();
      final ValueDesc type2 = args.get(1).returns();
      if (type1.isLong() && type2.isLong()) {
        return new LongFunc()
        {
          @Override
          public Long eval(List<Expr> args, NumericBinding bindings)
          {
            final Long x = Evals.evalLong(args.get(0), bindings);
            final Long y = Evals.evalLong(args.get(1), bindings);
            return x == null || y == null ? null : _eval(x, y);
          }
        };
      } else if (type1.isFloat() && type2.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            final Float x = Evals.evalFloat(args.get(0), bindings);
            final Float y = Evals.evalFloat(args.get(1), bindings);
            return x == null || y == null ? null : _eval(x, y);
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            final Double x = Evals.evalDouble(args.get(0), bindings);
            final Double y = Evals.evalDouble(args.get(1), bindings);
            return x == null || y == null ? null : _eval(x, y);
          }
        };
      }
    }

    protected abstract long _eval(long x, long y);

    protected abstract float _eval(float x, float y);

    protected abstract double _eval(double x, double y);
  }

  @Function.Named("max")
  final class Max extends DoubleParamMath
  {
    @Override
    protected long _eval(long x, long y)
    {
      return Math.max(x, y);
    }

    @Override
    protected float _eval(float x, float y)
    {
      return Math.max(x, y);
    }

    @Override
    protected double _eval(double x, double y)
    {
      return Math.max(x, y);
    }
  }

  @Function.Named("min")
  final class Min extends DoubleParamMath
  {
    @Override
    protected long _eval(long x, long y)
    {
      return Math.min(x, y);
    }

    @Override
    protected float _eval(float x, float y)
    {
      return Math.min(x, y);
    }

    @Override
    protected double _eval(double x, double y)
    {
      return Math.min(x, y);
    }
  }

  @Function.Named("div")
  final class Div extends DoubleParamMath
  {
    @Override
    protected long _eval(long x, long y)
    {
      return x / y;
    }

    @Override
    protected float _eval(float x, float y)
    {
      return x / y;
    }

    @Override
    protected double _eval(double x, double y)
    {
      return x / y;
    }
  }

  @Function.Named("nextAfter")
  final class NextAfter extends DoubleParamRealMath
  {
    @Override
    protected float _eval(float x, float y)
    {
      return Math.nextAfter(x, y);
    }

    @Override
    protected double _eval(double x, double y)
    {
      return Math.nextAfter(x, y);
    }
  }

  @Function.Named("pow")
  final class Pow extends DoubleParamDoubleMath
  {
    @Override
    protected double _eval(double x, double y)
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
      exactTwo(args);
      final ValueDesc type = args.get(0).returns();
      if (type.isFloat()) {
        return new FloatFunc()
        {
          @Override
          public Float eval(List<Expr> args, NumericBinding bindings)
          {
            Float x = Evals.evalFloat(args.get(0), bindings);
            Integer y = Evals.evalInt(args.get(1), bindings);
            return x == null || y == null ? null : Math.scalb(x, y);
          }
        };
      } else {
        return new DoubleFunc()
        {
          @Override
          public Double eval(List<Expr> args, NumericBinding bindings)
          {
            Double x = Evals.evalDouble(args.get(0), bindings);
            Integer y = Evals.evalInt(args.get(1), bindings);
            return x == null || y == null ? null : Math.scalb(x, y);
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
      atLeastThree(args);
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
              return Evals.castTo(args.get(i + 1).eval(bindings), commonType);
            }
          }
          return Evals.castTo(args.get(args.size() - 1).eval(bindings), commonType);
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
      exactTwo(args);
      final ValueDesc castTo = ValueDesc.fromTypeString(Evals.getConstantString(args.get(1)));
      return new Function()
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
            return ExprEval.nullOf(castTo);
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
      exactTwo(args);
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
      atLeastOne(args);
      ValueDesc prev = null;
      for (int i = 0; i < args.size(); i++) {
        prev = ValueDesc.toCommonType(prev, args.get(i));
      }
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);
      return new Function()
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
          return Evals.castTo(eval, type);
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
      atLeastThree(args);
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
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);
      return new Function()
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
              return Evals.castTo(args.get(i + 1).eval(bindings), type);
            }
          }
          if (args.size() % 2 != 1) {
            return Evals.castTo(args.get(args.size() - 1).eval(bindings), type);
          }
          return Evals.castTo(leftVal.defaultValue(), type);
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
      atLeastTwo(args);
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
      final ValueDesc type = Optional.ofNullable(prev).orElse(ValueDesc.STRING);
      return new Function()
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
              return Evals.castTo(args.get(i + 1).eval(bindings), type);
            }
          }
          if (size % 2 == 1) {
            return Evals.castTo(args.get(size - 1).eval(bindings), type);
          }
          return ExprEval.nullOf(type);
        }
      };
    }
  }

  @Function.Named("javascript")
  final class JavaScriptFunc extends NamedFactory implements Function.External
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
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

      return new ExternalFunc()
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
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          StringBuilder b = new StringBuilder();
          for (Expr expr : args) {
            b.append(Strings.nullToEmpty(expr.eval(bindings).asString()));
          }
          return b.toString();
        }
      };
    }
  }

  @Function.Named("format")
  final class FormatFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      atLeastOne(args);
      final String format = Evals.getConstantString(args.get(0));
      final Object[] formatArgs = new Object[args.size() - 1];
      return new StringFunc()
      {
        final StringBuilder builder = new StringBuilder();
        final Formatter formatter = new Formatter(builder);

        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          builder.setLength(0);
          for (int i = 0; i < formatArgs.length; i++) {
            formatArgs[i] = args.get(i + 1).eval(bindings).value();
          }
          formatter.format(format, formatArgs);
          return builder.toString();
        }
      };
    }
  }

  @Function.Named("repeat")
  final class RepeatFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      return new StringFunc()
      {
        private final StringBuilder builder = new StringBuilder();

        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String string = Evals.getConstantString(args.get(0));
          if (io.druid.common.utils.StringUtils.isNullOrEmpty(string)) {
            return string;
          }
          final int repeat = Evals.getConstantInt(args.get(1));
          if (repeat <= 1) {
            return repeat < 1 ? null : string;
          }
          builder.setLength(0);
          for (int i = 0; i < repeat; i++) {
            builder.append(string);
          }
          return builder.toString();
        }
      };
    }
  }

  @Function.Named("lpad")
  final class LPadFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      atLeastThree(args);
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new IAE("3rd argument of function 'lpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = Evals.evalString(args.get(0), bindings);
          return input == null ? null : Strings.padStart(input, length, padding);
        }
      };
    }
  }

  @Function.Named("rpad")
  final class RPadFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      atLeastThree(args);
      final int length = Evals.getConstantInt(args.get(1));
      String string = Evals.getConstantString(args.get(2));
      if (string.length() != 1) {
        throw new IAE("3rd argument of function 'rpad' should be constant char");
      }
      final char padding = string.charAt(0);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = Evals.evalString(args.get(0), bindings);
          return input == null ? null : Strings.padEnd(input, length, padding);
        }
      };
    }
  }

  @Function.Named("upper")
  final class UpperFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = args.get(0).eval(bindings).asString();
          return input == null ? null : input.toUpperCase();
        }
      };
    }
  }

  @Function.Named("lower")
  final class LowerFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = args.get(0).eval(bindings).asString();
          return input == null ? null : input.toLowerCase();
        }
      };
    }
  }

  // pattern
  @Function.Named("splitRegex")
  final class SplitRegex extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          ExprEval inputEval = args.get(0).eval(bindings);
          if (inputEval.isNull()) {
            return null;
          }
          String input = inputEval.asString();
          String splitter = args.get(1).eval(bindings).asString();
          int index = (int) args.get(2).eval(bindings).longValue();

          String[] split = input.split(splitter);
          return index >= split.length ? null : split[index];
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
      twoOrThree(args);
      final Splitter splitter;
      final String separator = Evals.getConstantString(args.get(1));
      if (separator.length() == 1) {
        splitter = Splitter.on(separator.charAt(0));
      } else {
        splitter = Splitter.on(separator);
      }
      if (args.size() == 2) {
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
            final ExprEval inputEval = args.get(0).eval(bindings);
            if (inputEval.isNull()) {
              return ExprEval.of(null, ValueDesc.STRING_ARRAY);
            }
            final String input = inputEval.asString();
            return ExprEval.of(Lists.newArrayList(splitter.split(input)), ValueDesc.STRING_ARRAY);
          }
        };
      }
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          ExprEval inputEval = args.get(0).eval(bindings);
          if (inputEval.isNull()) {
            return null;
          }
          String input = inputEval.asString();
          int index = (int) args.get(2).eval(bindings).longValue();
          if (index < 0) {
            return null;
          }
          for (String x : splitter.split(input)) {
            if (index-- == 0) {
              return x;
            }
          }
          return null;
        }
      };
    }
  }

  @Function.Named("proper")
  final class ProperFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = args.get(0).eval(bindings).asString();
          return Strings.isNullOrEmpty(input) ? input :
              Character.toUpperCase(input.charAt(0)) + input.substring(1).toLowerCase();
        }
      };
    }
  }

  @Function.Named("length")
  class LengthFunc extends NamedFactory.IntType
  {
    @Override
    public IntFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return input == null ? 0 : input.length();
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
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      if (Evals.isConstant(args.get(1))) {
        final int index = Evals.getConstantInt(args.get(1));
        return new StringFunc()
        {
          @Override
          public String eval(List<Expr> args, NumericBinding bindings)
          {
            final String input = Evals.evalString(args.get(0), bindings);
            if (input == null) {
              return input;
            }
            final int length = input.length();
            if (index == 0 || length == 0) {
              return "";
            }
            if (index < 0) {
              final int endIndex = length + index;
              return endIndex < 0 ? "" : input.substring(0, length + index);
            }
            return index > length ? input : input.substring(0, index);
          }
        };
      }
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = Evals.evalString(args.get(0), bindings);
          if (input == null) {
            return input;
          }
          final int index = Evals.evalInt(args.get(1), bindings);
          final int length = input.length();
          if (index == 0 || length == 0) {
            return "";
          }
          if (index < 0) {
            final int endIndex = length + index;
            return endIndex < 0 ? "" : input.substring(0, length + index);
          }
          return index > length ? input : input.substring(0, index);
        }
      };
    }
  }

  @Function.Named("right")
  final class RightFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      if (Evals.isConstant(args.get(1))) {
        final int index = Evals.getConstantInt(args.get(1));
        return new StringFunc()
        {
          @Override
          public String eval(List<Expr> args, NumericBinding bindings)
          {
            final String input = args.get(0).eval(bindings).asString();
            if (input == null) {
              return input;
            }
            final int length = input.length();
            if (index == 0 || length == 0) {
              return "";
            }
            if (index < 0) {
              return length + index < 0 ? "" : input.substring(-index);
            }
            final int startIndex = length - index;
            return startIndex < 0 ? input : input.substring(startIndex);
          }
        };
      }
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = args.get(0).eval(bindings).asString();
          if (input == null) {
            return input;
          }
          final int length = input.length();
          final int index = Evals.evalInt(args.get(0), bindings);
          if (index == 0 || length == 0) {
            return "";
          }
          if (index < 0) {
            return length + index < 0 ? "" : input.substring(-index);
          }
          final int startIndex = length - index;
          return startIndex < 0 ? input : input.substring(startIndex);
        }
      };
    }
  }

  @Function.Named("mid")
  class MidFunc extends NamedFactory.StringType
  {
    @Override
    public final StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      if (Evals.isConstant(args.get(1)) && Evals.isConstant(args.get(2))) {
        final int start = Evals.getConstantInt(args.get(1));
        final int end = Evals.getConstantInt(args.get(2));
        return new StringFunc()
        {
          @Override
          public String eval(List<Expr> args, NumericBinding bindings)
          {
            String input = Evals.evalString(args.get(0), bindings);
            return _eval(input, start, end);
          }
        };
      }
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = Evals.evalString(args.get(0), bindings);
          return _eval(input, Evals.evalInt(args.get(1), bindings), Evals.evalInt(args.get(2), bindings));
        }
      };
    }

    protected String _eval(String input, int start, int end)
    {
      if (input == null || start >= input.length()) {
        return null;
      }
      if (end < 0) {
        return input.substring(start);
      } else {
        return input.substring(start, Math.min(end, input.length()));
      }
    }
  }

  @Function.Named("substring")
  final class SubstringFunc extends MidFunc
  {
    @Override
    protected String _eval(String input, int start, int length)
    {
      if (input == null || start >= input.length()) {
        return null;
      }
      if (length < 0) {
        return input.substring(start);
      } else {
        return input.substring(start, Math.min(start + length, input.length()));
      }
    }
  }

  @Function.Named("indexOf")
  final class IndexOfFunc extends NamedFactory.IntType
  {
    @Override
    public IntFunc create(List<Expr> args, TypeResolver resolver)
    {
      twoOrThree(args);
      final int startIx = args.size() == 3 ? Evals.getConstantInt(args.get(2)) : 0;
      if (args.get(0).returns().isArrayOrStruct()) {
        return new IntFunc()
        {
          @Override
          public Integer eval(List<Expr> args, NumericBinding bindings)
          {
            final List input = (List) Evals.evalValue(args.get(0), bindings);
            if (input == null) {
              return -1;
            }
            final Object find = Evals.evalValue(args.get(0), bindings);
            if (startIx > 0) {
              final int index = input.subList(startIx, input.size()).indexOf(find);
              return index < 0 ? -1 : index + startIx;
            }
            return input.indexOf(find);
          }
        };
      }
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, NumericBinding bindings)
        {
          final String input = Evals.evalString(args.get(0), bindings);
          if (input == null) {
            return -1;
          }
          final String find = Evals.evalString(args.get(1), bindings);
          if (find == null) {
            return -1;
          }
          return startIx > 0 ? input.indexOf(find, startIx) : input.indexOf(find);
        }
      };
    }
  }

  @Function.Named("countOf")
  final class CountOfFunc extends NamedFactory.IntType
  {
    @Override
    public IntFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args);
      return new IntFunc()
      {
        @Override
        public Integer eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String find = args.get(1).eval(bindings).asString();
          Preconditions.checkArgument(!Strings.isNullOrEmpty(find), "find string cannot be null or empty");
          if (Strings.isNullOrEmpty(input)) {
            return 0;
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
          return counter;
        }
      };
    }
  }

  @Function.Named("replace")
  final class ReplaceFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactThree(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String find = args.get(1).eval(bindings).asString();
          String replace = args.get(2).eval(bindings).asString();
          return Strings.isNullOrEmpty(input) || Strings.isNullOrEmpty(find) ? input :
                 StringUtils.replace(input, find, replace);
        }
      };
    }
  }

  @Function.Named("initcap")
  final class InitCapFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return Strings.isNullOrEmpty(input) ? input : WordUtils.capitalize(input);
        }
      };
    }
  }

  @Function.Named("trim")
  final class TrimFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          return Strings.isNullOrEmpty(input) ? input : input.trim();
        }
      };
    }
  }

  // sql
  @Function.Named("btrim")
  final class BtrimFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return StringUtils.stripEnd(StringUtils.stripStart(input, strip), strip);
        }
      };
    }
  }

  // sql
  @Function.Named("ltrim")
  final class LtrimFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return StringUtils.stripStart(input, strip);
        }
      };
    }
  }

  // sql
  @Function.Named("rtrim")
  final class RtrimFunc extends NamedFactory.StringType
  {
    @Override
    public StringFunc create(List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args);
      return new StringFunc()
      {
        @Override
        public String eval(List<Expr> args, NumericBinding bindings)
        {
          String input = args.get(0).eval(bindings).asString();
          String strip = args.size() > 1 ? Evals.getConstantString(args.get(1)) : null;
          return StringUtils.stripEnd(input, strip);
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
      atLeastTwo(args);
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
              return ExprEval.nullOf(struct);
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
}
