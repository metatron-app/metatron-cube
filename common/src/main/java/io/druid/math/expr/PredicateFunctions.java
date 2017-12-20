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
  @Function.Named("like")
  final class Like extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 2 arguments");
      }
      final Pair<RegexUtils.PatternType, Object> matcher = RegexUtils.parse(Evals.getConstantString(args.get(1)));
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          return ExprEval.of(RegexUtils.evaluate(eval.asString(), matcher.lhs, matcher.rhs));
        }
      };
    }
  }

  @Function.Named("in")
  final class InFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() < 2) {
        throw new RuntimeException("function 'in' needs at least 2 arguments");
      }
      final Set<Object> set = Sets.newHashSet();
      for (int i = 1; i < args.size(); i++) {
        set.add(Evals.getConstant(args.get(i)));
      }
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(set.contains(args.get(0).eval(bindings).value()));
        }
      };
    }
  }

  @Function.Named("between")
  final class BetweenFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'between' needs 3 arguments");
      }
      ExprEval eval1 = Evals.getConstantEval(args.get(1));
      ExprEval eval2 = Evals.castTo(Evals.getConstantEval(args.get(2)), eval1.type());
      final Range<Comparable> range = Range.closed((Comparable) eval1.value(), (Comparable) eval2.value());
      final ExprType type = eval1.type();
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = Evals.castTo(args.get(0).eval(bindings), type);
          return ExprEval.of(range.contains((Comparable) eval.value()));
        }
      };
    }
  }

  @Function.Named("startsWith")
  final class StartsWithFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'startsWith' needs 2 arguments");
      }
      final String prefix = Evals.getConstantString(args.get(1));
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWith")
  final class EndsWithFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'endsWith' needs 2 arguments");
      }
      final String suffix = Evals.getConstantString(args.get(1));
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("startsWithIgnoreCase")
  final class StartsWithIgnoreCaseFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'startsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String prefix = value == null ? null : value.toLowerCase();
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.toLowerCase().startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWithIgnoreCase")
  final class EndsWithIgnoreCaseFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'endsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String suffix = value == null ? null : value.toLowerCase();
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.toLowerCase().endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("contains")
  final class ContainsFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'contains' needs 2 arguments");
      }
      final String contained = Evals.getConstantString(args.get(1));
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? contained == null : contained != null && eval.contains(contained));
        }
      };
    }
  }

  @Function.Named("match")
  final class MatchFunc extends Function.AbstractFactory
  {
    @Override
    public Function create(List<Expr> args)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function 'match' needs 2 arguments");
      }
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      return new LongChild()
      {
        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval != null && matcher.reset(eval).matches());
        }
      };
    }
  }
}
