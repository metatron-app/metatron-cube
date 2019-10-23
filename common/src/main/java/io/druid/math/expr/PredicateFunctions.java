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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Function.NamedFactory;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public interface PredicateFunctions extends Function.Library
{
  abstract class SingleParamBooleanFactory extends Function.NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 1) {
        throw new IAE("function '%s' needs 1 argument", name());
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(SingleParamBooleanFactory.this.evaluate(Evals.eval(args.get(0), bindings)));
        }
      };
    }

    protected abstract boolean evaluate(ExprEval param);
  }

  @Function.Named("isNull")
  final class IsNullFunc extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      return param.isNull();
    }
  }

  @Function.Named("isNotNull")
  final class IsNotNullFunc extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      return !param.isNull();
    }
  }

  @Function.Named("IsTrue")
  final class IsTrue extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      return param.asBoolean();
    }
  }

  @Function.Named("isFalse")
  final class isFalse extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      return !param.asBoolean();
    }
  }

  @Function.Named("isNaN")
  final class isNaN extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      final Double value = param.asDouble();
      return value != null && Double.isNaN(value);
    }
  }

  @Function.Named("isInfinite")
  final class isInfinite extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      final Double value = param.asDouble();
      return value != null && Double.isInfinite(value);
    }
  }

  @Function.Named("isFinite")
  final class isFinite extends SingleParamBooleanFactory
  {
    @Override
    public boolean evaluate(ExprEval param)
    {
      final Double value = param.asDouble();
      return value != null && Double.isFinite(value);
    }
  }

  @Function.Named("like")
  final class Like extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function '%s' needs 2 arguments", name());
      }
      final Pair<RegexUtils.PatternType, Object> matcher = RegexUtils.parse(Evals.getConstantString(args.get(1)));
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = args.get(0).eval(bindings);
          return ExprEval.of(RegexUtils.evaluate(eval.asString(), matcher.lhs, matcher.rhs));
        }
      };
    }
  }

  @Function.Named("in")
  final class InFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("function 'in' needs at least 2 arguments");
      }
      final Set<Object> set = Sets.newHashSet();
      for (int i = 1; i < args.size(); i++) {
        set.add(Evals.getConstant(args.get(i)));
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          return ExprEval.of(set.contains(args.get(0).eval(bindings).value()));
        }
      };
    }
  }

  @Function.Named("between")
  final class BetweenFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 3) {
        throw new IAE("function 'between' needs 3 arguments");
      }
      ExprEval eval1 = Evals.getConstantEval(args.get(1));
      ExprEval eval2 = Evals.castTo(Evals.getConstantEval(args.get(2)), eval1.type());
      final Range<Comparable> range = Range.closed((Comparable) eval1.value(), (Comparable) eval2.value());
      final ValueDesc type = eval1.type();
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          ExprEval eval = Evals.castTo(args.get(0).eval(bindings), type);
          return ExprEval.of(range.contains((Comparable) eval.value()));
        }
      };
    }
  }

  @Function.Named("startsWith")
  final class StartsWithFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'startsWith' needs 2 arguments");
      }
      final String prefix = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWith")
  final class EndsWithFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'endsWith' needs 2 arguments");
      }
      final String suffix = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("startsWithIgnoreCase")
  final class StartsWithIgnoreCaseFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'startsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String prefix = value == null ? null : value.toLowerCase();
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? prefix == null : prefix != null && eval.toLowerCase().startsWith(prefix));
        }
      };
    }
  }

  @Function.Named("endsWithIgnoreCase")
  final class EndsWithIgnoreCaseFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'endsWithIgnoreCase' needs 2 arguments");
      }
      String value = Evals.getConstantString(args.get(1));
      final String suffix = value == null ? null : value.toLowerCase();
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? suffix == null : suffix != null && eval.toLowerCase().endsWith(suffix));
        }
      };
    }
  }

  @Function.Named("contains")
  final class ContainsFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'contains' needs 2 arguments");
      }
      final String contained = Evals.getConstantString(args.get(1));
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval == null ? contained == null : contained != null && eval.contains(contained));
        }
      };
    }
  }

  @Function.Named("match")
  final class MatchFunc extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() != 2) {
        throw new IAE("function 'match' needs 2 arguments");
      }
      final Matcher matcher = Pattern.compile(Evals.getConstantString(args.get(1))).matcher("");
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String eval = args.get(0).eval(bindings).asString();
          return ExprEval.of(eval != null && matcher.reset(eval).find());
        }
      };
    }
  }

  @Function.Named("ipv4_in")
  final class IPv4In extends NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      if (args.size() < 2) {
        throw new IAE("function 'ipv4_in' needs at least 2 arguments");
      }
      final byte[] start = InetAddresses.forString(Evals.getConstantString(args.get(1))).getAddress();
      final byte[] end;
      Preconditions.checkArgument(start.length == 4);
      if (args.size() > 2) {
        end = InetAddresses.forString(Evals.getConstantString(args.get(2))).getAddress();
        Preconditions.checkArgument(end.length == 4);
      } else {
        end = Ints.toByteArray(-1);
      }
      for (int i = 0; i < 4; i++) {
        if (UnsignedBytes.compare(start[i], end[i]) > 0) {
          throw new IllegalArgumentException("start[n] <= end[n]");
        }
      }
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          String ipString = Evals.evalString(args.get(0), bindings);
          try {
            return ExprEval.of(evaluate(ipString));
          }
          catch (Exception e) {
            return ExprEval.of(false);
          }
        }

        private boolean evaluate(String ipString)
        {
          final byte[] address = InetAddresses.forString(ipString).getAddress();
          if (address.length != 4) {
            return false;
          }
          for (int i = 0; i < 4; i++) {
            if (UnsignedBytes.compare(address[i], start[i]) < 0 || UnsignedBytes.compare(address[i], end[i]) > 0) {
              return false;
            }
          }
          return true;
        }
      };
    }
  }
}
