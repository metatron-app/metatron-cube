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

import com.google.common.base.Suppliers;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.antlr.ExprBaseListener;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class ExprListenerImpl extends ExprBaseListener
{
  private final Map<ParseTree, Object> nodes;
  private final ParseTree rootNodeKey;
  private final Map<String, Function.Factory> functions;
  private final TypeResolver resolver;
  private final boolean flatten;

  ExprListenerImpl(
      ParseTree rootNodeKey,
      Map<String, Function.Factory> functions,
      TypeResolver resolver,
      boolean flatten
  )
  {
    this.rootNodeKey = rootNodeKey;
    this.functions = functions;
    this.nodes = new HashMap<>();
    this.resolver = resolver;
    this.flatten = flatten;
  }

  Expr getAST()
  {
    return (Expr) nodes.get(rootNodeKey);
  }

  private void registerWithFlatten(ExprParser.ExprContext ctx, Expr expr, Expr... params)
  {
    registerWithFlatten(ctx, expr, Arrays.asList(params));
  }

  private void registerWithFlatten(ExprParser.ExprContext ctx, Expr expr, List<Expr> params)
  {
    if (flatten && !Evals.isConstant(expr) && Evals.isAllConstants(params)) {
      expr = Evals.toConstant(expr.eval(null));
    }
    nodes.put(ctx, expr);
  }

  @Override
  public void exitUnaryOpExpr(ExprParser.UnaryOpExprContext ctx)
  {
    final int opCode = ((TerminalNode) ctx.getChild(0)).getSymbol().getType();
    final Expr param = (Expr) nodes.get(ctx.getChild(1));

    final Expr expr;
    switch (opCode) {
      case ExprParser.MINUS:
        expr = new UnaryMinusExpr(param);
        break;
      case ExprParser.NOT:
        expr = new UnaryNotExpr(param);
        break;
      default:
        throw new RuntimeException("Unrecognized unary operator " + ctx.getChild(0).getText());
    }
    registerWithFlatten(ctx, expr, param);
  }

  @Override
  public void exitBooleanExpr(ExprParser.BooleanExprContext ctx)
  {
    nodes.put(ctx, BooleanConst.of(Boolean.valueOf(ctx.getText())));
  }

  @Override
  public void exitLongExpr(ExprParser.LongExprContext ctx)
  {
    String text = ctx.getText();
    char last = text.charAt(text.length() - 1);
    if (last == 'l' || last == 'L') {
      text = text.substring(0, text.length() - 1);
    }
    nodes.put(ctx, new LongConst(Long.parseLong(text)));
  }

  @Override
  public void exitFloatExpr(ExprParser.FloatExprContext ctx)
  {
    nodes.put(ctx, new FloatConst(Float.parseFloat(ctx.getText())));
  }

  @Override
  public void exitDoubleExpr(ExprParser.DoubleExprContext ctx)
  {
    nodes.put(ctx, new DoubleConst(Double.parseDouble(ctx.getText())));
  }

  @Override
  public void exitDecimalExpr(ExprParser.DecimalExprContext ctx)
  {
    final String text = ctx.getText();
    final String value = text.substring(0, text.length() - 1);
    nodes.put(ctx, new DecimalConst(Rows.parseDecimal(value)));
  }

  @Override
  public void exitAddSubExpr(ExprParser.AddSubExprContext ctx)
  {
    final String op = ctx.getChild(1).getText();
    final Expr left = (Expr) nodes.get(ctx.getChild(0));
    final Expr right = (Expr) nodes.get(ctx.getChild(2));

    final int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();

    final Expr expr;
    switch (opCode) {
      case ExprParser.PLUS:
        expr = new BinPlusExpr(op, left, right);
        break;
      case ExprParser.MINUS:
        expr = new BinMinusExpr(op, left, right);
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + op);
    }
    registerWithFlatten(ctx, expr, left, right);
  }

  @Override
  public void exitLogicalAndOrExpr(ExprParser.LogicalAndOrExprContext ctx)
  {
    final String op = ctx.getChild(1).getText();
    final Expr left = (Expr) nodes.get(ctx.getChild(0));
    final Expr right = (Expr) nodes.get(ctx.getChild(2));

    final int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();

    final Expr expr;
    switch (opCode) {
      case ExprParser.AND:
        expr = new BinAndExpr(op, left, right);
        break;
      case ExprParser.OR:
        expr = new BinOrExpr(op, left, right);
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + op);
    }
    registerWithFlatten(ctx, expr, left, right);
  }

  @Override
  public void exitNestedExpr(ExprParser.NestedExprContext ctx)
  {
    nodes.put(ctx, nodes.get(ctx.getChild(1)));
  }

  @Override
  public void exitString(ExprParser.StringContext ctx)
  {
    String text = ctx.getText();
    String value = null;
    if (!"NULL".equals(text)) {
      String unquoted = text.substring(1, text.length() - 1);
      value = unquoted.indexOf('\\') >= 0 ? StringEscapeUtils.unescapeJava(unquoted) : unquoted;
    }
    nodes.put(ctx, new StringConst(value));
  }

  @Override
  public void exitLogicalOpExpr(ExprParser.LogicalOpExprContext ctx)
  {
    final String op = ctx.getChild(1).getText();
    final Expr left = (Expr) nodes.get(ctx.getChild(0));
    final Expr right = (Expr) nodes.get(ctx.getChild(2));
    final boolean equals = left.equals(right);

    final int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();

    final Expr expr;
    switch (opCode) {
      case ExprParser.LT:
        expr = equals ? BooleanConst.FALSE : new BinLtExpr(op, left, right);
        break;
      case ExprParser.LEQ:
        expr = equals ? BooleanConst.TRUE : new BinLeqExpr(op, left, right);
        break;
      case ExprParser.GT:
        expr = equals ? BooleanConst.FALSE : new BinGtExpr(op, left, right);
        break;
      case ExprParser.GEQ:
        expr = equals ? BooleanConst.TRUE : new BinGeqExpr(op, left, right);
        break;
      case ExprParser.EQ:
        expr = equals ? BooleanConst.TRUE : new BinEqExpr(op, left, right);
        break;
      case ExprParser.NEQ:
        expr = equals ? BooleanConst.FALSE : new BinNeqExpr(op, left, right);
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + op);
    }
    registerWithFlatten(ctx, expr, left, right);
  }

  @Override
  public void exitMulDivModuloExpr(ExprParser.MulDivModuloExprContext ctx)
  {
    final String op = ctx.getChild(1).getText();
    final Expr left = (Expr) nodes.get(ctx.getChild(0));
    final Expr right = (Expr) nodes.get(ctx.getChild(2));

    final int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    final Expr expr;
    switch (opCode) {
      case ExprParser.MUL:
        expr = new BinMulExpr(op, left, right);
        break;
      case ExprParser.DIV:
        expr = new BinDivExpr(op, left, right);
        break;
      case ExprParser.MODULO:
        expr = new BinModuloExpr(op, left, right);
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + op);
    }
    registerWithFlatten(ctx, expr, left, right);
  }

  @Override
  public void exitPowOpExpr(ExprParser.PowOpExprContext ctx)
  {
    final Expr left = (Expr) nodes.get(ctx.getChild(0));
    final Expr right = (Expr) nodes.get(ctx.getChild(2));
    final BinPowExpr expr = new BinPowExpr(ctx.getChild(1).getText(), left, right);

    registerWithFlatten(ctx, expr, left, right);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void exitFunctionExpr(ExprParser.FunctionExprContext ctx)
  {
    final String fnName = ctx.getChild(0).getText();
    final Function.Factory factory = functions.get(fnName.toLowerCase());
    if (factory == null) {
      throw new IAE("function '%s' is not defined.", fnName);
    }

    final List<Expr> args = ctx.getChildCount() > 3
                            ? (List<Expr>) nodes.get(ctx.getChild(2))
                            : Collections.<Expr>emptyList();

    final FunctionExpr expr = new FunctionExpr(fnName, args, Suppliers.memoize(() -> factory.create(args, resolver)));
    if (factory instanceof Function.External) {
      nodes.put(ctx, expr);
    } else {
      registerWithFlatten(ctx, expr, args);
    }
  }

  @Override
  public void exitIdentifierExpr(ExprParser.IdentifierExprContext ctx)
  {
    nodes.put(ctx, makeIdentifier(ctx));
  }

  private IdentifierExpr makeIdentifier(ExprParser.IdentifierExprContext ctx)
  {
    List<TerminalNode> identifiers = ctx.IDENTIFIER();
    String text = normalize(identifiers.get(0).getText());
    TerminalNode indexed = ctx.LONG();
    if (indexed != null) {
      int index = Integer.parseInt(indexed.getText());
      if (ctx.getToken(ExprParser.MINUS, 0) != null) {
        index = -index;
      }
      ValueDesc type = resolver.resolve(text, ValueDesc.UNKNOWN).unwrapDimension();
      return new IdentifierExpr(text, type, index);
    }
    if (identifiers.size() > 1) {
      StringBuilder builder = new StringBuilder(text);
      for (int i = 1; i < identifiers.size(); i++) {
        if (builder.length() > 0) {
          builder.append('.');
        }
        builder.append(normalize(identifiers.get(i).getText()));
      }
      text = builder.toString();
    }
    ValueDesc type = resolver.resolve(text, ValueDesc.UNKNOWN).unwrapDimension();
    return new IdentifierExpr(text, type);
  }

  protected String normalize(String identifier)
  {
    return StringUtils.unquote(identifier);
  }

  @Override
  public void exitFunctionArgs(ExprParser.FunctionArgsContext ctx)
  {
    List<Expr> args = new ArrayList<>();
    int x = -1;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode && child.getText().equals(",")) {
        args.add(x < 0 ? new StringConst(null) : (Expr) nodes.get(ctx.getChild(x)));
        x = -1;
        continue;
      }
      x = i;
    }
    if (x >= 0) {
      args.add((Expr) nodes.get(ctx.getChild(x)));
    }

    nodes.put(ctx, args);
  }

  @Override
  public void exitAssignExpr(ExprParser.AssignExprContext ctx)
  {
    nodes.put(
        ctx, new AssignExpr((Expr) nodes.get(ctx.getChild(0)), (Expr) nodes.get(ctx.getChild(2)))
    );
  }
}
