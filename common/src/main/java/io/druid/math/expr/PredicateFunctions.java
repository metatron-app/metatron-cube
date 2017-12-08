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

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface PredicateFunctions extends Function.Library
{
  abstract class PredicateFunc extends Function.NewInstance
  {
    @Override
    public ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }
  }

  final class Like extends PredicateFunc
  {
    private Pair<RegexUtils.PatternType, Object> matcher;

    @Override
    public String name()
    {
      return "like";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
  }

  final class InFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "in";
    }

    private transient Set<Object> set;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
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
  }

  final class BetweenFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "between";
    }

    private transient ExprType type;
    private transient Range<Comparable> range;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (range == null) {
        if (args.size() != 3) {
          throw new RuntimeException("function 'between' needs 3 arguments");
        }
        Expr param1 = args.get(1);
        Expr param2 = args.get(2);
        if (!Evals.isConstant(param1) || !Evals.isConstant(param2)) {
          throw new RuntimeException("needs constants for range values");
        }
        ExprEval eval1 = param1.eval(bindings);
        ExprEval eval2 = Evals.castTo(param2.eval(bindings), eval1.type());
        range = Range.closed((Comparable) eval1.value(), (Comparable) eval2.value());
        type = eval1.type();
      }
      ExprEval eval = Evals.castTo(args.get(0).eval(bindings), type);
      return ExprEval.of(range.contains((Comparable) eval.value()));
    }
  }

  final class StartsWithFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "startsWith";
    }

    private transient boolean initialized;
    private transient String prefix;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'startsWith' needs 2 arguments");
        }
        prefix = Evals.getConstantString(args.get(1));
        initialized = true;
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.startsWith(prefix));
    }
  }

  final class EndsWithFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "endsWith";
    }

    private transient boolean initialized;
    private transient String prefix;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'endsWith' needs 2 arguments");
        }
        prefix = Evals.getConstantString(args.get(1));
        initialized = true;
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.endsWith(prefix));
    }
  }

  final class StartsWithIgnoreCaseFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "startsWithIgnoreCase";
    }

    private transient boolean initialized;
    private transient String prefix;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'startsWithIgnoreCase' needs 2 arguments");
        }
        String prefix = Evals.getConstantString(args.get(1));
        this.prefix = prefix == null ? null : prefix.toLowerCase();
        initialized = true;
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.toLowerCase().startsWith(prefix));
    }
  }

  final class EndsWithIgnoreCaseFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "endsWithIgnoreCase";
    }

    private transient boolean initialized;
    private transient String prefix;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'endsWithIgnoreCase' needs 2 arguments");
        }
        String prefix = Evals.getConstantString(args.get(1));
        this.prefix = prefix == null ? null : prefix.toLowerCase();
        initialized = true;
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.toLowerCase().endsWith(prefix));
    }
  }

  final class ContainsFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "contains";
    }

    private transient boolean initialized;
    private transient String contained;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (!initialized) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'contains' needs 2 arguments");
        }
        this.contained = Evals.getConstantString(args.get(1));
        initialized = true;
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval == null ? contained == null : contained != null && eval.contains(contained));
    }
  }

  final class MatchFunc extends PredicateFunc
  {
    @Override
    public String name()
    {
      return "match";
    }

    private transient Matcher matcher;

    @Override
    public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
    {
      if (matcher == null) {
        if (args.size() != 2) {
          throw new RuntimeException("function 'match' needs 2 arguments");
        }
        matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      }
      String eval = args.get(0).eval(bindings).asString();
      return ExprEval.of(eval != null && matcher.reset(eval).matches());
    }
  }
}
