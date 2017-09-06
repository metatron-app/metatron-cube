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
import com.google.common.base.Suppliers;
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

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Parser
{
  private static final Logger log = new Logger(Parser.class);
  private static final Map<String, Supplier<Function>> functions = Maps.newHashMap();

  static {
    register(BuiltinFunctions.class);
    register(DateTimeFunctions.class);
    register(ExcelFunctions.class);
  }

  public static void register(Class parent)
  {
    for (Class clazz : parent.getClasses()) {
      if (Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }
      String name = null;
      Supplier<Function> supplier = null;
      try {
        if (Function.Factory.class.isAssignableFrom(clazz)) {
          Function.Factory factory = (Function.Factory) clazz.newInstance();
          name = factory.name().toLowerCase();
          supplier = factory;
        } else if (Function.class.isAssignableFrom(clazz)) {
          Function function = (Function) clazz.newInstance();
          name = function.name().toLowerCase();
          supplier = Suppliers.ofInstance(function);
        }
      }
      catch (Exception e) {
        log.info(e, "failed to instantiate " + clazz.getName() + ".. ignoring");
      }
      if (name == null || supplier == null) {
        continue;
      }
      Supplier prev = functions.get(name);
      if (prev != null) {
        if (prev.get().getClass() != supplier.get().getClass()) {
          throw new IllegalArgumentException("function '" + name + "' cannot not be overridden");
        }
        continue;
      }
      functions.put(name, supplier);
      if (parent != BuiltinFunctions.class && parent != DateTimeFunctions.class && parent != ExcelFunctions.class) {
        log.info("user defined function '" + name + "' is registered with class " + clazz.getName());
      }
    }
  }

  public static Expr parse(String in)
  {
    return parse(in, functions);
  }

  public static Expr parse(String in, Map<String, Supplier<Function>> func)
  {
    ParseTree parseTree = parseTree(in);
    ParseTreeWalker walker = new ParseTreeWalker();
    ExprListenerImpl listener = new ExprListenerImpl(parseTree, func);
    walker.walk(listener, parseTree);
    return listener.getAST();
  }

  public static Expr parseWith(String in, Function.Factory... moreFunctions)
  {
    Map<String, Supplier<Function>> custom = Maps.newHashMap(functions);
    for (Function.Factory factory : moreFunctions) {
      Preconditions.checkArgument(
          custom.put(factory.name().toLowerCase(), factory) == null,
          "cannot override existing function %s", factory.name()
      );
    }
    return parse(in, custom);
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
        if (type == null) {
          throw new RuntimeException("No binding found for " + name);
        }
        return type;
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
        if (supplier == null) {
          throw new RuntimeException("No binding found for " + name);
        }
        return ExprType.bestEffortOf(supplier.type().typeName());
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
        if (valueType == null) {
          throw new RuntimeException("No binding found for " + name);
        }
        return ExprType.typeOf(valueType);
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
