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
import io.druid.data.input.Row;
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
    } else if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
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
    if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      Expr left = flatten(binary.left);
      Expr right = flatten(binary.right);
      if (Evals.isAllConstants(left, right)) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (left != binary.left || right != binary.right) {
        return Evals.binaryOp(binary, left, right);
      }
    } else if (expr instanceof UnaryOp) {
      UnaryOp unary = (UnaryOp) expr;
      Expr eval = flatten(unary.getChild());
      if (eval instanceof Constant) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (eval != unary.getChild()) {
        expr = Evals.unaryOp(unary, expr);
      }
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      if (functionExpr.function instanceof Function.External) {
        return expr;
      }
      List<Expr> flatten = flatten(functionExpr.args);
      if (Evals.isAllConstants(flatten)) {
        expr = Evals.toConstant(expr.eval(null));
      } else if (!Evals.isIdentical(functionExpr.args, flatten)) {
        expr = new FunctionExpr(functionExpr.function, functionExpr.name, flatten);
      }
    }
    return expr;
  }

  public static interface Visitor<T>
  {
    T visit(Constant expr);
    T visit(IdentifierExpr expr);
    T visit(AssignExpr expr, T assignee, T assigned);
    T visit(UnaryOp expr, T child);
    T visit(BinaryOp expr, T left, T right);
    T visit(FunctionExpr expr, List<T> children);
    T visit(Expr other);
  }

  public static <T> T traverse(Expr expr, Visitor<T> visitor)
  {
    if (expr instanceof Constant) {
      return visitor.visit((Constant) expr);
    } else if (expr instanceof IdentifierExpr) {
      return visitor.visit((IdentifierExpr) expr);
    } else if (expr instanceof AssignExpr) {
      T assignee = traverse(((AssignExpr) expr).assignee, visitor);
      T assigned = traverse(((AssignExpr) expr).assigned, visitor);
      return visitor.visit((AssignExpr) expr, assignee, assigned);
    } else if (expr instanceof UnaryOp) {
      T child = traverse(((UnaryOp) expr).getChild(), visitor);
      return visitor.visit((UnaryOp) expr, child);
    } else if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      T left = traverse(binary.left, visitor);
      T right = traverse(binary.right, visitor);
      return visitor.visit((BinaryOp) expr, left, right);
    } else if (expr instanceof FunctionExpr) {
      List<T> params = Lists.newArrayList();
      for (Expr child : ((FunctionExpr) expr).args) {
        params.add(traverse(child, visitor));
      }
      return visitor.visit((FunctionExpr) expr, params);
    }
    return visitor.visit(expr);
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

  public static Expr.NumericBinding withRowSupplier(final Supplier<Row> rowSupplier)
  {
    return new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return rowSupplier.get().getColumns();
      }

      @Override
      public Object get(String name)
      {
        if (name.equals(TIME_COLUMN_NAME)) {
          return rowSupplier.get().getTimestampFromEpoch();
        }
        return rowSupplier.get().getRaw(name);
      }
    };
  }

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

  public static TypeResolver withTypeMap(final Map<String, ValueDesc> bindings)
  {
    return new TypeResolver.Abstract()
    {
      @Override
      public ValueDesc resolve(String name)
      {
        ValueDesc type = bindings.get(name);
        return type == null ? ValueDesc.UNKNOWN : type;
      }
    };
  }

  public static TypeResolver withTypeSuppliers(final Map<String, DSuppliers.TypedSupplier> bindings)
  {
    return new TypeResolver.Abstract()
    {
      @Override
      public ValueDesc resolve(String name)
      {
        DSuppliers.Typed supplier = bindings.get(name);
        return supplier == null ? ValueDesc.UNKNOWN : supplier.type();
      }
    };
  }

  public static TypeResolver withTypes(final TypeResolver bindings)
  {
    return new TypeResolver.Abstract()
    {
      @Override
      public ValueDesc resolve(String name)
      {
        return bindings.resolve(name, ValueDesc.UNKNOWN);
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
    if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
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
