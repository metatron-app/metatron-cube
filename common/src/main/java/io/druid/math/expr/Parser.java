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
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.DSuppliers;
import io.druid.common.guava.DSuppliers.TypedSupplier;
import io.druid.common.guava.DSuppliers.WithRawAccess;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);

  private static final Map<String, Object> registered = Maps.newConcurrentMap();
  private static final Map<String, Function.Factory> functions = Maps.newHashMap();

  static {
    register(BuiltinFunctions.class);
    register(WindowFunctions.class);
    register(PredicateFunctions.class);
    register(BitSetFunctions.class);
    register(DateTimeFunctions.class);
    register(ExcelFunctions.class);
    register(JsonFunctions.class);
  }

  public static void register(Class parent)
  {
    if (registered.putIfAbsent(parent.getName(), new Object()) != null) {
      return;
    }

    boolean builtInLibrary = isBuiltIn(parent);

    log.info("registering functions in %s library [%s]", builtInLibrary ? "built-in" : "user", parent.getName());

    for (Function.Factory factory : getFunctions(parent)) {
      register(factory, builtInLibrary);
    }
  }

  public static void register(Function.Factory factory)
  {
    register(factory, false);
  }

  private static void register(Function.Factory factory, boolean builtInLibrary)
  {
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

  public static boolean isBuiltIn(Function.Factory factory)
  {
    return isBuiltIn(factory.getClass().getEnclosingClass());
  }

  public static boolean isBuiltIn(Class parent)
  {
    return parent == BuiltinFunctions.class ||
           parent == WindowFunctions.class ||
           parent == PredicateFunctions.class ||
           parent == BitSetFunctions.class ||
           parent == DateTimeFunctions.class ||
           parent == ExcelFunctions.class ||
           parent == JsonFunctions.class;
  }

  private static Iterable<Function.Factory> getFunctions(Class<?> parent)
  {
    try {
      if (Function.Provider.class.isAssignableFrom(parent)) {
        return ((Function.Provider) parent.getDeclaredConstructor().newInstance()).getFunctions();
      }
      final List<Function.Factory> functions = Lists.newArrayList();
      for (Class<?> clazz : parent.getClasses()) {
        if (Modifier.isAbstract(clazz.getModifiers())) {
          continue;
        }
        try {
          if (Function.Factory.class.isAssignableFrom(clazz)) {
            functions.add((Function.Factory) clazz.getDeclaredConstructor().newInstance());
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
        for (; current instanceof URLClassLoader; current = current.getParent()) {
          log.info("--- %s", Arrays.<URL>asList(((URLClassLoader) current).getURLs()));
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
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, func, resolver, flatten)
    {
      @Override
      protected String normalize(String identifier)
      {
        String normalized = super.normalize(identifier);
        if (!GuavaUtils.isNullOrEmpty(mapping)) {
          return mapping.getOrDefault(normalized, normalized);
        }
        return normalized;
      }
    };
    walker.walk(listener, parseTree);
    return listener.getAST();
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
    } else if (expr instanceof UnaryOp) {
      findBindingsRecursive(((UnaryOp) expr).expr(), found);
    } else if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      findBindingsRecursive(binary.left(), found);
      findBindingsRecursive(binary.right(), found);
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

  private static final Object NOT_EXISTS = new Object();

  public static Expr optimizeFunction(final Expr expr)
  {
    return traverse(expr, new ExprVisitor()
    {
      @Override
      public Expr visit(FunctionExpr expr, List<Expr> children)
      {
        if ("case".equals(expr.op())) {
          Expr param = null;
          final Map<Object, Object> mapping = Maps.newHashMap();
          final int size = children.size();
          for (int i = 0; i < size - 1; i += 2) {
            if (!Evals.isConstant(children.get(i + 1))) {
              return expr;
            }
            Pair<Expr, Object> split = Evals.splitSimpleEq(children.get(i));
            if (split == null || (param != null && !param.equals(split.lhs))) {
              return expr;
            }
            param = split.lhs;
            mapping.put(split.rhs, Evals.getConstant(children.get(i + 1)));
          }
          final FunctionEval func;
          final List<Expr> params;
          if (size % 2 == 1) {
            final Expr defaultExpr = children.get(size - 1);
            if (Evals.isConstant(defaultExpr)) {
              Object constant = Evals.getConstant(defaultExpr);
              func = (args, binding) -> mapping.getOrDefault(Evals.evalValue(args.get(0), binding), constant);
            } else {
              func = (args, binding) -> {
                Object mapped = mapping.getOrDefault(Evals.evalValue(args.get(0), binding), NOT_EXISTS);
                return mapped == NOT_EXISTS ? Evals.evalValue(args.get(1), binding) : mapped;
              };
            }
            params = Arrays.asList(param, defaultExpr);
          } else {
            func = (args, binding) -> mapping.get(Evals.evalValue(args.get(0), binding));
            params = Arrays.asList(param);
          }
          return new FunctionExpr("__map", params, Suppliers.memoize(() -> Evals.asFunction(func, expr.returns())));
        } else if ("switch".equals(expr.op())) {
          final Expr param = children.get(0);
          final Map<Object, Object> mapping = Maps.newHashMap();
          final int size = children.size();
          for (int i = 1; i < size - 1; i += 2) {
            if (!Evals.isConstant(children.get(i)) || !Evals.isConstant(children.get(i + 1))) {
              return expr;
            }
            mapping.put(Evals.getConstant(children.get(i)), Evals.getConstant(children.get(i + 1)));
          }
          final FunctionEval func;
          final List<Expr> params;
          if (size % 2 == 0) {
            final Expr defaultExpr = children.get(size - 1);
            if (Evals.isConstant(defaultExpr)) {
              Object constant = Evals.getConstant(defaultExpr);
              func = (args, binding) -> mapping.getOrDefault(Evals.evalValue(args.get(0), binding), constant);
            } else {
              func = (args, binding) -> {
                Object mapped = mapping.getOrDefault(Evals.evalValue(args.get(0), binding), NOT_EXISTS);
                return mapped == NOT_EXISTS ? Evals.evalValue(args.get(1), binding) : mapped;
              };
            }
            params = Arrays.asList(param, defaultExpr);
          } else {
            func = (args, binding) -> mapping.get(Evals.evalValue(args.get(0), binding));
            params = Arrays.asList(param);
          }
          return new FunctionExpr("__map", params, Suppliers.memoize(() -> Evals.asFunction(func, expr.returns())));
        }
        return expr;
      }
    });
  }

  public static Expr optimize(
      final Expr expr,
      final Map<String, TypedSupplier> values,
      final Map<String, WithRawAccess> rawAccessible
  )
  {
    Expr optimized = traverse(optimizeFunction(expr), new ExprVisitor()
    {
      @Override
      public Expr visit(BinaryOp op, Expr left, Expr right)
      {
        if (op instanceof BooleanOp) {
          if (Evals.isIdentifier(left) && Evals.isIdentifier(right)) {
            String id1 = Evals.getIdentifier(left);
            String id2 = Evals.getIdentifier(right);
            WithRawAccess leftAccess = rawAccessible.get(id1);
            WithRawAccess rightAccess = rawAccessible.get(id2);
            if (leftAccess != null && rightAccess != null) {
              BooleanOp rewritten = rewriteCompare(op.op(), leftAccess, rightAccess);
              if (rewritten != null) {
                return rewritten;
              }
            }
            TypedSupplier supplier1 = values.getOrDefault(id1, TypedSupplier.UNKNOWN);
            TypedSupplier supplier2 = values.getOrDefault(id2, TypedSupplier.UNKNOWN);
            if (supplier1.type().isPrimitive() && Objects.equals(supplier1.type(), supplier2.type())) {
              BooleanOp rewritten = rewriteCompare(op.op(), supplier1, supplier2, supplier1.type().comparator());
              if (rewritten != null) {
                return rewritten;
              }
            }
          } else if (Evals.isIdentifier(left) && Evals.isConstant(right)) {
            String id1 = Evals.getIdentifier(left);
            TypedSupplier supplier1 = values.getOrDefault(id1, TypedSupplier.UNKNOWN);
            ValueDesc type1 = supplier1.type();
            if (type1.isPrimitive()) {
              Supplier supplier2 = Suppliers.ofInstance(Evals.castTo(Evals.getConstantEval(right), type1).value());
              BooleanOp rewritten = rewriteCompare(op.op(), supplier1, supplier2, type1.comparator());
              if (rewritten != null) {
                return rewritten;
              }
            }
          } else if (Evals.isConstant(left) && Evals.isIdentifier(right)) {
            String id2 = Evals.getIdentifier(right);
            TypedSupplier supplier2 = values.getOrDefault(id2, TypedSupplier.UNKNOWN);
            ValueDesc type2 = supplier2.type();
            if (type2.isPrimitive()) {
              Supplier supplier1 = Suppliers.ofInstance(Evals.castTo(Evals.getConstantEval(left), type2).value());
              BooleanOp rewritten = rewriteCompare(op.op(), supplier1, supplier2, type2.comparator());
              if (rewritten != null) {
                return rewritten;
              }
            }
          }
        } else if (op instanceof BinaryArithmeticOp) {
          ValueDesc commonType = ValueDesc.toCommonType(toType(left, values), toType(right, values));
          if (commonType.isPrimitiveNumeric()) {
            TypedSupplier supplier1 = Optimizers.toTypedSupplier(left, values, commonType);
            TypedSupplier supplier2 = Optimizers.toTypedSupplier(right, values, commonType);
            if (supplier1 != null && supplier2 != null) {
              Expr rewritten = Optimizers.optimize(commonType.type(), supplier1, supplier2, Arithmetics.of(op.op()));
              if (rewritten != null) {
                return rewritten;
              }
            }
          }
        }
        return super.visit(op, left, right);
      }
    });
    return traverse(optimized, new ExprVisitor()
    {
      @Override
      public Expr visit(IdentifierExpr expr)
      {
        return expr.rewrite(values);
      }
    });
  }

  private static ValueDesc toType(Expr expr, Map<String, TypedSupplier> suppliers)
  {
    if (Evals.isIdentifier(expr)) {
      return suppliers.getOrDefault(Evals.getIdentifier(expr), TypedSupplier.UNKNOWN).type();
    } else if (Evals.isConstant(expr) || expr instanceof Expr.Optimized) {
      return expr.returns();
    }
    return ValueDesc.UNKNOWN;
  }

  private static BooleanOp rewriteCompare(String op, WithRawAccess left, WithRawAccess right)
  {
    switch (op) {
      case "<":
        return x -> ExprEval.of(left.getAsRef().compareTo(right.getAsRef()) < 0);
      case "<=":
        return x -> ExprEval.of(left.getAsRef().compareTo(right.getAsRef()) <= 0);
      case ">":
        return x -> ExprEval.of(left.getAsRef().compareTo(right.getAsRef()) > 0);
      case ">=":
        return x -> ExprEval.of(left.getAsRef().compareTo(right.getAsRef()) >= 0);
      case "==":
        return x -> ExprEval.of(left.getAsRef().equals(right.getAsRef()));
      case "!=":
        return x -> ExprEval.of(!left.getAsRef().equals(right.getAsRef()));
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static BooleanOp rewriteCompare(String op, Supplier left, Supplier right, Comparator cp)
  {
    switch (op) {
      case "<":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) < 0);
      case "<=":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) <= 0);
      case ">":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) > 0);
      case ">=":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) >= 0);
      case "==":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) == 0);
      case "!=":
        return x -> ExprEval.of(cp.compare(left.get(), right.get()) != 0);
    }
    return null;
  }

  public static SimpleBinaryOp isBinaryRangeOpWith(Expr expr, Predicate<String> predicate)
  {
    if (expr instanceof BinaryOp && expr instanceof BooleanOp) {
      BinaryOp binaryOp = (BinaryOp) expr;
      if (!Expressions.isCompare(binaryOp.op())) {
        return null;
      }
      Expr left = binaryOp.left();
      Expr right = binaryOp.right();
      if (Evals.isIdentifier(left) && Evals.isIdentifier(right)) {
        String id1 = Evals.getIdentifier(left);
        String id2 = Evals.getIdentifier(right);
        if (predicate.apply(id1) && predicate.apply(id2)) {
          return new SimpleBinaryOp(binaryOp.op(), id1, id2);
        }
      }
    }
    return null;
  }

  public static class SimpleBinaryOp
  {
    public final String op;
    public final String left;
    public final String right;

    private SimpleBinaryOp(String op, String left, String right)
    {
      this.op = op;
      this.left = left;
      this.right = right;
    }
  }

  public static interface Visitor
  {
    default Expr visit(Constant expr) {return expr;}

    default Expr visit(IdentifierExpr expr) {return expr;}

    default Expr visit(AssignExpr expr, Expr assignee, Expr assigned) {return expr;}

    default Expr visit(UnaryOp expr, Expr child) {return expr;}

    default Expr visit(BinaryOp expr, Expr left, Expr right) {return expr;}

    Expr visit(FunctionExpr expr, List<Expr> children);

    default Expr visit(Expr other) {return other;}
  }

  public static class ExprVisitor implements Visitor
  {
    @Override
    public Expr visit(AssignExpr expr, Expr assignee, Expr assigned)
    {
      return expr.assignee == assignee && expr.assigned == assigned ? expr : Evals.assignExpr(assignee, assigned);
    }

    @Override
    public Expr visit(UnaryOp expr, Expr child)
    {
      return expr.expr() == child ? expr : expr.with(child);
    }

    @Override
    public Expr visit(BinaryOp expr, Expr left, Expr right)
    {
      return expr.left() == left && expr.right() == right ? expr : expr.with(left, right);
    }

    @Override
    public Expr visit(FunctionExpr expr, List<Expr> children)
    {
      return Evals.isIdentical(expr.args, children) ? expr : expr.with(children);
    }
  }

  public static void closeQuietly(Expr expr)
  {
    traverse(expr, (func, params) ->  {
      CloseQuietly.close(func.getFunction());
      return func;
    });
  }

  public static Expr traverse(Expr expr, Visitor visitor)
  {
    if (expr instanceof Constant) {
      return visitor.visit((Constant) expr);
    } else if (expr instanceof IdentifierExpr) {
      return visitor.visit((IdentifierExpr) expr);
    } else if (expr instanceof AssignExpr) {
      AssignExpr assign = (AssignExpr) expr;
      Expr assignee = traverse(assign.assignee, visitor);
      Expr assigned = traverse(assign.assigned, visitor);
      return visitor.visit(assign, assignee, assigned);
    } else if (expr instanceof UnaryOp) {
      UnaryOp unary = (UnaryOp) expr;
      Expr child = traverse(unary.expr(), visitor);
      return visitor.visit(unary, child);
    } else if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      Expr left = traverse(binary.left(), visitor);
      Expr right = traverse(binary.right(), visitor);
      return visitor.visit(binary, left, right);
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr function = (FunctionExpr) expr;
      List<Expr> params = Lists.newArrayList();
      for (Expr child : function.args) {
        params.add(traverse(child, visitor));
      }
      return visitor.visit(function, params);
    }
    return visitor.visit(expr);
  }

  // for trivial case
  public static Expr.NumericBinding returnAlways(Object value)
  {
    return new Expr.NumericBinding()
    {
      @Override
      public Collection<String> names()
      {
        return ImmutableList.of();
      }

      @Override
      public Object get(String name)
      {
        return value;
      }
    };
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

  public static Expr.Bindable bindable(String name)
  {
    return new Expr.Bindable()
    {
      private final DSuppliers.HandOver<Object> handOver = new DSuppliers.HandOver<>();

      @Override
      public Expr.Bindable bind(String name, Object value)
      {
        handOver.set(value);
        return this;
      }

      @Override
      public Collection<String> names()
      {
        return Arrays.asList(name);
      }

      @Override
      public Object get(String name)
      {
        return handOver.get();
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
    if (expr instanceof UnaryOp) {
      init(((UnaryOp) expr).expr());
    } else if (expr instanceof BinaryOp) {
      BinaryOp binary = (BinaryOp) expr;
      init(binary.left());
      init(binary.right());
    } else if (expr instanceof FunctionExpr) {
      FunctionExpr functionExpr = (FunctionExpr) expr;
      if (functionExpr.getFunction() instanceof WindowFunctions.Factory.WindowFunction) {
        ((WindowFunctions.Factory.WindowFunction) functionExpr.getFunction()).init();
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
