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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.DSuppliers;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.antlr.ExprLexer;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.joda.time.DateTime;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);

  private static final Map<String, Object> registered = Maps.newConcurrentMap();
  private static final Map<String, Function.Factory> functions = Maps.newHashMap();

  static {
    register(BuiltinFunctions.class);
    register(PredicateFunctions.class);
    register(DateTimeFunctions.class);
    register(ExcelFunctions.class);
  }

  public static void register(Class parent)
  {
    if (registered.putIfAbsent(parent.getName(), new Object()) != null) {
      return;
    }
    log.info("registering function in library %s", parent.getName());

    boolean userDefinedLibrary = parent != BuiltinFunctions.class &&
                                 parent != PredicateFunctions.class &&
                                 parent != DateTimeFunctions.class &&
                                 parent != ExcelFunctions.class;
    for (Class clazz : parent.getClasses()) {
      if (Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }
      Function.Factory factory = null;
      try {
        if (Function.Factory.class.isAssignableFrom(clazz)) {
          factory = (Function.Factory) clazz.newInstance();
        } else if (Function.class.isAssignableFrom(clazz)) {
          Function function = (Function) clazz.newInstance();
          factory = new Function.Stateless(function);
        }
      }
      catch (Exception e) {
        log.info(e, "failed to instantiate " + clazz.getName() + ".. ignoring");
        continue;
      }
      if (factory == null || factory.name() == null) {
        continue;
      }
      String name = factory.name().toLowerCase();
      Function.Factory prev = functions.get(name);
      if (prev != null) {
        throw new IllegalArgumentException("function '" + name + "' cannot not be overridden");
      }
      functions.put(name, factory);

      if (userDefinedLibrary) {
        log.info("user defined function '" + name + "' is registered with class " + clazz.getName());
      }
    }
  }

  public static Expr parse(String in)
  {
    return parse(in, functions, true);
  }

  public static Expr parse(String in, boolean flatten)
  {
    return parse(in, functions, flatten);
  }

  public static Expr parse(String in, Map<String, Function.Factory> func, boolean flatten)
  {
    ParseTree parseTree = parseTree(in);
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, func, flatten);
    walker.walk(listener, parseTree);
    Expr expr = listener.getAST();
    if (flatten) {
      expr = flatten(expr);
    }
    return expr;
  }

  public static Expr parseWith(String in, Function.Factory... moreFunctions)
  {
    Map<String, Function.Factory> custom = Maps.newHashMap(functions);
    for (Function.Factory factory : moreFunctions) {
      Preconditions.checkArgument(
          custom.put(factory.name().toLowerCase(), factory) == null,
          "cannot override existing function %s", factory.name()
      );
    }
    return parse(in, custom, true);
  }

  public static ParseTree parseTree(String in)
  {
    ExprLexer lexer = new ExprLexer(new ANTLRInputStream(in));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setBuildParseTree(true);
    return parser.expr();
  }

  public static List<String> findRequiredBindings(String in)
  {
    return findRequiredBindings(parse(in));
  }

  public static List<String> findRequiredBindings(Expr parsed)
  {
    return Lists.newArrayList(findBindingsRecursive(parsed, Sets.<String>newLinkedHashSet()));
  }

  private static Set<String> findBindingsRecursive(Expr expr, Set<String> found)
  {
    if (expr instanceof IdentifierExpr) {
      found.add(expr.toString());
    } else if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      findBindingsRecursive(binary.left, found);
      findBindingsRecursive(binary.right, found);
    } else if (expr instanceof UnaryMinusExpr) {
      findBindingsRecursive(((UnaryMinusExpr) expr).expr, found);
    } else if (expr instanceof UnaryNotExpr) {
      findBindingsRecursive(((UnaryNotExpr) expr).expr, found);
    } else if (expr instanceof FunctionExpr) {
      for (Expr child : ((FunctionExpr) expr).args) {
        findBindingsRecursive(child, found);
      }
    }
    return found;
  }

  static List<Expr> flatten(List<Expr> expr)
  {
    for (int i = 0; i < expr.size(); i++) {
      expr.set(i, flatten(expr.get(i)));
    }
    return expr;
  }

  static Expr flatten(Expr expr)
  {
    if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      Expr left = flatten(binary.left);
      Expr right = flatten(binary.right);
      if (Evals.isAllConstants(left, right)) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (left != binary.left || right != binary.right) {
        return Evals.binaryOp(binary, left, right);
      }
    } else if (expr instanceof Unary) {
      Unary unary = (Unary) expr;
      Expr eval = flatten(unary.getChild());
      if (eval instanceof Constant) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (eval != unary.getChild()) {
        if (expr instanceof UnaryMinusExpr) {
          expr = new UnaryMinusExpr(eval);
        } else if (expr instanceof UnaryNotExpr) {
          expr = new UnaryNotExpr(eval);
        } else {
          expr = unary; // unknown type..
        }
      }
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      if (functionExpr.function instanceof Function.External) {
        return expr;
      }
      List<Expr> args = functionExpr.args;
      boolean flattened = false;
      List<Expr> flattening = Lists.newArrayListWithCapacity(args.size());
      for (Expr arg : args) {
        Expr flatten = flatten(arg);
        flattened |= flatten != arg;
        flattening.add(flatten);
      }
      if (Evals.isAllConstants(flattening)) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (flattened) {
        expr = new FunctionExpr(functionExpr.function, functionExpr.name, flattening);
      }
    }
    return expr;
  }

  public static interface Visitor<T>
  {
    T visit(Expr expr, List<T> children);
  }

  public static <T> T traverse(Expr expr, Visitor<T> visitor)
  {
    List<T> params = Lists.newArrayList();
    if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      params.add(traverse(binary.left, visitor));
      params.add(traverse(binary.right, visitor));
    } else if (expr instanceof UnaryMinusExpr) {
      params.add(traverse(((UnaryMinusExpr) expr).expr, visitor));
    } else if (expr instanceof UnaryNotExpr) {
      params.add(traverse(((UnaryNotExpr) expr).expr, visitor));
    } else if (expr instanceof FunctionExpr) {
      for (Expr child : ((FunctionExpr) expr).args) {
        params.add(traverse(child, visitor));
      }
    }
    return visitor.visit(expr, params);
  }

  public static Expr.NumericBinding withMap(final Map<String, ?> bindings)
  {
    return new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return bindings.keySet();
      }

      @Override
      public Object get(String name)
      {
        Object value = bindings.get(name);
        if (value == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        return value;
      }
    };
  }

  private static final String TIME_COLUMN_NAME = "__time";

  public static Expr.NumericBinding withTimeAndMap(final DateTime timestamp, final Map<String, ?> bindings)
  {
    Preconditions.checkArgument(!bindings.containsKey(TIME_COLUMN_NAME));
    return new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return bindings.keySet();
      }

      @Override
      public Object get(String name)
      {
        if (name.equals(TIME_COLUMN_NAME)) {
          return timestamp;
        }
        Object value = bindings.get(name);
        if (value == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        return value;
      }
    };
  }

  public static Expr.NumericBinding withSuppliers(final Map<String, Supplier> bindings)
  {
    return new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return bindings.keySet();
      }

      @Override
      public Object get(String name)
      {
        Supplier supplier = bindings.get(name);
        if (supplier == null && !bindings.containsKey(name)) {
          throw new RuntimeException("No binding found for " + name);
        }
        return supplier == null ? null : supplier.get();
      }
    };
  }

  public static Expr.TypeBinding withTypeMap(final Map<String, ExprType> bindings)
  {
    return new Expr.TypeBinding()
    {
      @Override
      public ExprType type(String name)
      {
        ExprType type = bindings.get(name);
        return type == null ? ExprType.UNKNOWN : type;
      }
    };
  }

  public static Expr.TypeBinding withTypeSuppliers(final Map<String, DSuppliers.TypedSupplier> bindings)
  {
    return new Expr.TypeBinding()
    {
      @Override
      public ExprType type(String name)
      {
        DSuppliers.Typed supplier = bindings.get(name);
        return supplier == null ? ExprType.UNKNOWN : ExprType.bestEffortOf(supplier.type().typeName());
      }
    };
  }

  public static Expr.TypeBinding withTypes(final TypeResolver bindings)
  {
    return new Expr.TypeBinding()
    {
      @Override
      public ExprType type(String name)
      {
        ValueDesc valueType = bindings.resolveColumn(name);
        return valueType == null ? ExprType.UNKNOWN : ExprType.typeOf(valueType);
      }
    };
  }

  public static com.google.common.base.Function<String, String> asStringFunction(final Expr expr)
  {
    final DSuppliers.HandOver<String> supplier = new DSuppliers.HandOver<>();
    return new com.google.common.base.Function<String, String>()
    {
      private final Expr.NumericBinding bindings = new Expr.NumericBinding()
      {
        @Override
        public Collection<String> names()
        {
          return Parser.findRequiredBindings(expr);
        }

        @Override
        public String get(String name)
        {
          return supplier.get();
        }
      };

      @Override
      public String apply(String s)
      {
        supplier.set(Strings.nullToEmpty(s));
        return expr.eval(bindings).asString();
      }
    };
  }

  public static void reset(Expr expr)
  {
    if (expr instanceof BinaryOpExprBase) {
      BinaryOpExprBase binary = (BinaryOpExprBase) expr;
      reset(binary.left);
      reset(binary.right);
    } else if (expr instanceof UnaryMinusExpr) {
      reset(((UnaryMinusExpr) expr).expr);
    } else if (expr instanceof UnaryNotExpr) {
      reset(((UnaryNotExpr) expr).expr);
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      if (functionExpr.function instanceof BuiltinFunctions.PartitionFunction) {
        ((BuiltinFunctions.PartitionFunction) functionExpr.function).reset();
      } else {
        for (Expr child : functionExpr.args) {
          reset(child);
        }
      }
    }
  }

  // under construction
  public static final Expression.Factory<Expr> EXPR_FACTORY = new Expression.Factory<Expr>()
  {
    @Override
    public Expr or(List<Expr> children)
    {
      Preconditions.checkArgument(!children.isEmpty());
      if (children.size() == 1) {
        return children.get(0);
      }
      Expr prev = new BinOrExpr("||", children.get(0), children.get(1));
      for (int i = 2; i < children.size(); i++) {
        prev = new BinOrExpr("||", prev, children.get(i));
      }
      return prev;
    }

    @Override
    public Expr and(List<Expr> children)
    {
      Preconditions.checkArgument(!children.isEmpty());
      if (children.size() == 1) {
        return children.get(0);
      }
      Expr prev = new BinAndExpr("&&", children.get(0), children.get(1));
      for (int i = 2; i < children.size(); i++) {
        prev = new BinAndExpr("&&", prev, children.get(i));
      }
      return prev;
    }

    @Override
    public Expr not(Expr expression)
    {
      return new UnaryNotExpr(expression);
    }
  };
}
