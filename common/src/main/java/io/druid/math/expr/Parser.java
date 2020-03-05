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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.BuiltinFunctions.WindowFunctionFactory;
import io.druid.math.expr.antlr.ExprLexer;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.joda.time.DateTime;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
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
    final boolean builtInLibrary = parent == BuiltinFunctions.class ||
                                   parent == PredicateFunctions.class ||
                                   parent == DateTimeFunctions.class ||
                                   parent == ExcelFunctions.class;

    log.info("registering functions in %s library [%s]", builtInLibrary ? "built-in" : "user", parent.getName());

    for (Function.Factory factory : getFunctions(parent)) {
      String name = Preconditions.checkNotNull(factory.name(), "name for [%s] is null", factory).toLowerCase();
      Function.Factory prev = functions.get(name);
      if (prev != null) {
        throw new IAE("function '%s' cannot not be overridden", name);
      }
      functions.put(name, factory);

      if (!builtInLibrary) {
        log.info("> '%s' is registered with class %s", name, factory.getClass().getSimpleName());
      }
    }
  }

  public static boolean isBuiltIn(Function.Factory factory)
  {
    final Class parent = factory.getClass().getEnclosingClass();
    return parent == BuiltinFunctions.class ||
           parent == PredicateFunctions.class ||
           parent == DateTimeFunctions.class ||
           parent == ExcelFunctions.class;
  }

  private static Iterable<Function.Factory> getFunctions(Class parent)
  {
    try {
      if (Function.Provider.class.isAssignableFrom(parent)) {
        return ((Function.Provider) parent.newInstance()).getFunctions();
      }
      final List<Function.Factory> functions = Lists.newArrayList();
      for (Class clazz : parent.getClasses()) {
        if (Modifier.isAbstract(clazz.getModifiers())) {
          continue;
        }
        try {
          if (Function.Factory.class.isAssignableFrom(clazz)) {
            functions.add((Function.Factory) clazz.newInstance());
          }
        }
        catch (Throwable t) {
          if (BuiltinFunctions.AbstractRFunc.class.isAssignableFrom(clazz) ||
              BuiltinFunctions.AbstractPythonFunc.class.isAssignableFrom(clazz)) {
            continue;   // skip warning
          }
          log.warn(t, "failed to instantiate %s by %s .. ignoring", clazz.getName(), t);
        }
      }
      return functions;
    }
    catch (Throwable t) {
      if (t instanceof NoClassDefFoundError) {
        ClassLoader current = parent.getClassLoader();
        for (;current instanceof URLClassLoader; current = current.getParent()) {
          log.info("--- " + Arrays.<URL>asList(((URLClassLoader) current).getURLs()));
        }
      }
      log.warn(t, "failed to load functions from %s by %s .. ignoring", parent.getName(), t);
      return ImmutableList.of();
    }
  }

  public static List<Function.Factory> getAllFunctions()
  {
    return Lists.newArrayList(functions.values());
  }

  // this can be used whenever return type is not important
  public static Expr parse(String in)
  {
    return parse(in, functions, null, TypeResolver.UNKNOWN, true);
  }

  public static Expr parse(String in, TypeResolver resolver)
  {
    return parse(in, functions, null, resolver, true);
  }

  public static Expr parse(String in, Map<String, String> mapping, TypeResolver resolver)
  {
    return parse(in, functions, mapping, resolver, true);
  }

  public static Expr parse(String in, boolean flatten)
  {
    return parse(in, functions, null, TypeResolver.UNKNOWN, flatten);
  }

  private static Expr parse(
      String in,
      Map<String, Function.Factory> func,
      final Map<String, String> mapping,
      TypeResolver resolver,
      boolean flatten
  )
  {
    ParseTree parseTree = parseTree(in);
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, func, resolver)
    {
      @Override
      protected String normalize(String identifier)
      {
        if (!GuavaUtils.isNullOrEmpty(mapping)) {
          String normalized = mapping.get(identifier);
          if (normalized != null) {
            return normalized;
          }
        }
        return identifier;
      }
    };
    walker.walk(listener, parseTree);
    Expr expr = listener.getAST();
    if (flatten) {
      expr = flatten(expr);
    }
    return expr;
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
      found.add(((IdentifierExpr) expr).identifier());
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
    } else if (expr instanceof AssignExpr) {
      AssignExpr assign = (AssignExpr) expr;
      findRequiredBindings(assign.assignee);
      findRequiredBindings(assign.assigned);
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
      } else if (left.equals(right)) {
        expr = Evals.hasEq(binary) ? Evals.TRUE : Evals.FALSE;
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
        expr = functionExpr.with(flatten);
      }
    } else if (expr instanceof AssignExpr) {
      AssignExpr assign = (AssignExpr) expr;
      Expr assignee = flatten(assign.assignee);
      Expr assigned = flatten(assign.assigned);
      if (assignee != assign.assignee || assigned != assign.assigned) {
        expr = new AssignExpr(assignee, assigned);
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

  public static class ExprVisitor implements Visitor<Expr>
  {
    @Override
    public Expr visit(Constant expr)
    {
      return expr;
    }

    @Override
    public Expr visit(IdentifierExpr expr)
    {
      return expr;
    }

    @Override
    public Expr visit(AssignExpr expr, Expr assignee, Expr assigned)
    {
      return expr.assignee == assignee && expr.assigned == assigned ? expr : Evals.assignExpr(assignee, assigned);
    }

    @Override
    public Expr visit(UnaryOp expr, Expr child)
    {
      return expr.getChild() == child ? expr : expr.with(child);
    }

    @Override
    public Expr visit(BinaryOp expr, Expr left, Expr right)
    {
      return expr.left == left && expr.right == right ? expr : expr.with(left, right);
    }

    @Override
    public Expr visit(FunctionExpr expr, List<Expr> children)
    {
      return Evals.isIdentical(expr.args, children) ? expr : expr.with(children);
    }

    @Override
    public Expr visit(Expr other)
    {
      return other;
    }
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

  public static Expr rewrite(Expr expr, final TypeResolver resolver, final Map<String, String> mapping)
  {
    return traverse(expr, new ExprVisitor()
    {
      @Override
      public Expr visit(IdentifierExpr expr)
      {
        final String mapped = mapping.get(expr.identifier());
        return mapped == null ? expr : Evals.identifierExpr(mapped, resolver.resolve(mapped));
      }
    });
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
        return bindings.get(name);
      }
    };
  }

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
        if (Row.TIME_COLUMN_NAME.equals(name)) {
          return rowSupplier.get().getTimestampFromEpoch();
        }
        return rowSupplier.get().getRaw(name);
      }
    };
  }

  public static Expr.NumericBinding withTimeAndMap(final DateTime timestamp, final Map<String, ?> bindings)
  {
    Preconditions.checkArgument(!bindings.containsKey(Row.TIME_COLUMN_NAME));
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
        if (Row.TIME_COLUMN_NAME.equals(name)) {
          return timestamp;
        }
        return bindings.get(name);
      }
    };
  }

  public static Expr.TypedBinding withTypedSuppliers(final Map<String, DSuppliers.TypedSupplier> bindings)
  {
    return new Expr.TypedBinding()
    {
      @Override
      public ValueDesc resolve(String column)
      {
        DSuppliers.TypedSupplier supplier = bindings.get(column);
        return supplier == null ? null : supplier.type();
      }

      @Override
      public Collection<String> names()
      {
        return bindings.keySet();
      }

      @Override
      public Object get(String name)
      {
        DSuppliers.TypedSupplier supplier = bindings.get(name);
        return supplier == null ? null : supplier.get();
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
        return supplier == null ? null : supplier.get();
      }
    };
  }

  public static TypeResolver withTypeMap(final Map<String, ValueDesc> bindings)
  {
    return new TypeResolver()
    {
      @Override
      public ValueDesc resolve(String name)
      {
        ValueDesc type = bindings.get(name);
        return type == null ? ValueDesc.UNKNOWN : type;
      }
    };
  }

  public static TypeResolver withTypes(final TypeResolver bindings)
  {
    return new TypeResolver()
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

  public static void init(Expr expr)
  {
    if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      init(binary.left);
      init(binary.right);
    } else if (expr instanceof UnaryMinusExpr) {
      init(((UnaryMinusExpr) expr).expr);
    } else if (expr instanceof UnaryNotExpr) {
      init(((UnaryNotExpr) expr).expr);
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      if (functionExpr.function instanceof WindowFunctionFactory.WindowFunction) {
        ((WindowFunctionFactory.WindowFunction) functionExpr.function).init();
      } else {
        for (Expr child : functionExpr.args) {
          init(child);
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
