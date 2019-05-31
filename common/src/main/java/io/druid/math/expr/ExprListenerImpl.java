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

import com.metamx.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.antlr.ExprBaseListener;
import io.druid.math.expr.antlr.ExprParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
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

  ExprListenerImpl(ParseTree rootNodeKey, Map<String, Function.Factory> functions, TypeResolver resolver)
  {
    this.rootNodeKey = rootNodeKey;
    this.functions = functions;
    this.nodes = new HashMap<>();
    this.resolver = resolver;
  }

  Expr getAST()
  {
    return (Expr) nodes.get(rootNodeKey);
  }

  @Override
  public void exitUnaryOpExpr(ExprParser.UnaryOpExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(0)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.MINUS:
        nodes.put(ctx, new UnaryMinusExpr((Expr) nodes.get(ctx.getChild(1))));
        break;
      case ExprParser.NOT:
        nodes.put(ctx, new UnaryNotExpr((Expr) nodes.get(ctx.getChild(1))));
        break;
      default:
        throw new RuntimeException("Unrecognized unary operator " + ctx.getChild(0).getText());
    }
  }

  @Override
  public void exitBooleanExpr(ExprParser.BooleanExprContext ctx)
  {
    nodes.put(
        ctx,
        new BooleanExpr(Boolean.valueOf(ctx.getText()))
    );
  }

  @Override
  public void exitFloatExpr(ExprParser.FloatExprContext ctx)
  {
    nodes.put(
        ctx,
        new FloatExpr(Float.parseFloat(ctx.getText()))
    );
  }

  @Override
  public void exitDoubleExpr(ExprParser.DoubleExprContext ctx)
  {
    nodes.put(
        ctx,
        new DoubleExpr(Double.parseDouble(ctx.getText()))
    );
  }

  @Override
  public void exitAddSubExpr(ExprParser.AddSubExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.PLUS:
        nodes.put(
            ctx,
            new BinPlusExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.MINUS:
        nodes.put(
            ctx,
            new BinMinusExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitLongExpr(ExprParser.LongExprContext ctx)
  {
    nodes.put(
        ctx,
        new LongExpr(Long.parseLong(ctx.getText()))
    );
  }

  @Override
  public void exitLogicalAndOrExpr(ExprParser.LogicalAndOrExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.AND:
        nodes.put(
            ctx,
            new BinAndExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.OR:
        nodes.put(
            ctx,
            new BinOrExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
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
    nodes.put(ctx, new StringExpr(value));
  }

  @Override
  public void exitLogicalOpExpr(ExprParser.LogicalOpExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.LT:
        nodes.put(
            ctx,
            new BinLtExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.LEQ:
        nodes.put(
            ctx,
            new BinLeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.GT:
        nodes.put(
            ctx,
            new BinGtExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.GEQ:
        nodes.put(
            ctx,
            new BinGeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.EQ:
        nodes.put(
            ctx,
            new BinEqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.NEQ:
        nodes.put(
            ctx,
            new BinNeqExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitMulDivModuloExpr(ExprParser.MulDivModuloExprContext ctx)
  {
    int opCode = ((TerminalNode) ctx.getChild(1)).getSymbol().getType();
    switch (opCode) {
      case ExprParser.MUL:
        nodes.put(
            ctx,
            new BinMulExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.DIV:
        nodes.put(
            ctx,
            new BinDivExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      case ExprParser.MODULO:
        nodes.put(
            ctx,
            new BinModuloExpr(
                ctx.getChild(1).getText(),
                (Expr) nodes.get(ctx.getChild(0)),
                (Expr) nodes.get(ctx.getChild(2))
            )
        );
        break;
      default:
        throw new RuntimeException("Unrecognized binary operator " + ctx.getChild(1).getText());
    }
  }

  @Override
  public void exitPowOpExpr(ExprParser.PowOpExprContext ctx)
  {
    nodes.put(
        ctx,
        new BinPowExpr(
            ctx.getChild(1).getText(),
            (Expr) nodes.get(ctx.getChild(0)),
            (Expr) nodes.get(ctx.getChild(2))
        )
    );
  }

  @Override
  public void exitFunctionExpr(ExprParser.FunctionExprContext ctx)
  {
    final String fnName = ctx.getChild(0).getText();
    final Function.Factory factory = functions.get(fnName.toLowerCase());
    if (factory == null) {
      throw new IAE("function '%s' is not defined.", fnName);
    }

    @SuppressWarnings("unchecked")
    final List<Expr> args = ctx.getChildCount() > 3
                            ? (List<Expr>) nodes.get(ctx.getChild(2))
                            : Collections.<Expr>emptyList();
    nodes.put(
        ctx,
        new FunctionExpr(new java.util.function.Function<List<Expr>, Function>()
        {
          @Override
          public Function apply(List<Expr> exprs)
          {
            return factory.create(args, resolver);
          }
        }, fnName, args)
    );
  }

  @Override
  public void exitIdentifierExpr(ExprParser.IdentifierExprContext ctx)
  {
    nodes.put(ctx, makeIdentifier(ctx));
  }

  private IdentifierExpr makeIdentifier(ExprParser.IdentifierExprContext ctx)
  {
    String text = ctx.getChild(0).getText();
    if (text.charAt(0) == '"' && text.charAt(text.length() - 1) == '"') {
      text = text.substring(1, text.length() - 1);  // strip off
    }
    text = normalize(text);
    ValueDesc type = resolver.resolve(text, ValueDesc.UNKNOWN);
    if (type.isDimension()) {
      type = ValueDesc.STRING;    // todo
    }
    if (ctx.getChildCount() == 5 &&
        ctx.getChild(1).getText().equals("[") &&
        ctx.getChild(2).getText().equals("-") &&
        ctx.getChild(4).getText().equals("]")) {
      int index = Integer.parseInt(ctx.getChild(3).getText());
      return new IdentifierExpr(text, type, -index);
    } else if (ctx.getChildCount() == 4 &&
               ctx.getChild(1).getText().equals("[") &&
               ctx.getChild(3).getText().equals("]")) {
      int index = Integer.parseInt(ctx.getChild(2).getText());
      return new IdentifierExpr(text, type, index);
    } else {
      return new IdentifierExpr(text, type);
    }
  }

  protected String normalize(String identifier)
  {
    return identifier;
  }

  @Override
  public void exitFunctionArgs(ExprParser.FunctionArgsContext ctx)
  {
    List<Expr> args = new ArrayList<>();
    int x = -1;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      ParseTree child = ctx.getChild(i);
      if (child instanceof TerminalNode && child.getText().equals(",")) {
        args.add(x < 0 ? new StringExpr(null) : (Expr) nodes.get(ctx.getChild(x)));
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
