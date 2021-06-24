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

import com.google.common.collect.Lists;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;

import java.util.BitSet;
import java.util.List;

public interface BitSetFunctions extends Function.Library
{
  public static abstract class BitSetType extends Function.NamedFactory implements Function.FixedTyped
  {
    public abstract class BitSetChild extends Child
    {
      @Override
      public final ValueDesc returns() { return ValueDesc.BITSET;}
    }

    @Override
    public abstract BitSetChild create(List<Expr> args, TypeResolver resolver);

    @Override
    public final ValueDesc returns()
    {
      return ValueDesc.BITSET;
    }
  }

  @Function.Named("bitset.cardinality")
  final class Cardinality extends Function.NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args, ValueDesc.BITSET);
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval value = Evals.eval(args.get(0), bindings);
          if (value.isNull()) {
            return ExprEval.of(0);
          }
          return ExprEval.of(((BitSet) value.value()).cardinality());
        }
      };
    }
  }

  @Function.Named("bitset.unwrap")
  final class Unwrap extends Function.NamedFactory implements Function.FixedTyped
  {
    @Override
    public ValueDesc returns()
    {
      return ValueDesc.LONG_ARRAY;
    }

    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactOne(args, ValueDesc.BITSET);
      return new Child()
      {
        public ValueDesc returns()
        {
          return ValueDesc.LONG_ARRAY;
        }

        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = args.get(0).eval(bindings);
          if (eval.isNull()) {
            return ExprEval.of(null, ValueDesc.LONG_ARRAY);
          }
          final BitSet bitSet = (BitSet) eval.value();
          final List<Integer> values = Lists.newArrayListWithExpectedSize(bitSet.cardinality());
          for (int x = bitSet.nextSetBit(0); x >= 0; x = bitSet.nextSetBit(x + 1)) {
            values.add(x);
          }
          return ExprEval.of(values, ValueDesc.LONG_ARRAY);
        }
      };
    }
  }

  @Function.Named("bitset.get")
  final class Get extends Function.NamedFactory.BooleanType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args, ValueDesc.BITSET, ValueDesc.LONG);
      return new BooleanChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = args.get(0).eval(bindings);
          if (eval.isNull()) {
            return ExprEval.NULL_BOOL;
          }
          final BitSet bitSet = (BitSet) eval.value();
          final Integer ix = Evals.evalInt(args.get(1), bindings);
          return ix == null ? ExprEval.NULL_BOOL : ExprEval.of(bitSet.get(ix));
        }
      };
    }
  }

  abstract class BitSetIndexOp extends Function.NamedFactory.LongType
  {
    @Override
    public Function create(List<Expr> args, TypeResolver resolver)
    {
      oneOrTwo(args, ValueDesc.BITSET);
      return new LongChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval = args.get(0).eval(bindings);
          if (eval.isNull()) {
            return ExprEval.NULL_LONG;
          }
          final BitSet bitSet = (BitSet) eval.value();
          if (args.size() == 1) {
            return ExprEval.of(eval(bitSet, defaultIx(bitSet)), ValueDesc.LONG);
          }
          final Integer ix = Evals.evalInt(args.get(1), bindings);
          return ExprEval.of(eval(bitSet, ix == null ? defaultIx(bitSet) : ix), ValueDesc.LONG);
        }
      };
    }

    protected abstract int defaultIx(BitSet bitSet);
    protected abstract int eval(BitSet bitSet, int x);
  }

  @Function.Named("nextSetBit")
  final class NextSetBit extends BitSetIndexOp
  {
    @Override
    protected int defaultIx(BitSet bitSet)
    {
      return 0;
    }

    @Override
    protected int eval(BitSet bitSet, int x)
    {
      return bitSet.nextSetBit(x);
    }
  }
  @Function.Named("nextClearBit")
  final class NextClearBit extends BitSetIndexOp
  {
    @Override
    protected int defaultIx(BitSet bitSet)
    {
      return 0;
    }

    protected int eval(BitSet bitSet, int x)
    {
      return bitSet.nextClearBit(x);
    }
  }

  @Function.Named("previousSetBit")
  final class PreviousSetBit extends BitSetIndexOp
  {
    @Override
    protected int defaultIx(BitSet bitSet)
    {
      return bitSet.length() - 1;
    }

    @Override
    protected int eval(BitSet bitSet, int x)
    {
      return bitSet.previousSetBit(x);
    }
  }
  @Function.Named("previousClearBit")
  final class PreviousClearBit extends BitSetIndexOp
  {
    @Override
    protected int defaultIx(BitSet bitSet)
    {
      return bitSet.length();
    }

    protected int eval(BitSet bitSet, int x)
    {
      return bitSet.previousClearBit(x);
    }
  }

  abstract class BitSetBinaryOP extends BitSetType
  {
    @Override
    public BitSetChild create(List<Expr> args, TypeResolver resolver)
    {
      exactTwo(args, ValueDesc.BITSET, ValueDesc.BITSET);
      return new BitSetChild()
      {
        @Override
        public ExprEval evaluate(List<Expr> args, Expr.NumericBinding bindings)
        {
          final ExprEval eval0 = Evals.eval(args.get(0), bindings);
          final ExprEval eval1 = Evals.eval(args.get(1), bindings);
          if (eval0.isNull()) {
            return eval1;
          }
          if (eval1.isNull()) {
            return eval0;
          }
          BitSet bs0 = (BitSet) eval0.value();
          BitSet bs1 = (BitSet) eval1.value();
          return ExprEval.of(operate(bs0, bs1), ValueDesc.BITSET);
        }
      };
    }

    protected abstract BitSet operate(BitSet bs0, BitSet bs1);
  }

  @Function.Named("bitset.or")
  final class BitSetOr extends BitSetBinaryOP
  {
    @Override
    protected BitSet operate(BitSet bs0, BitSet bs1)
    {
      BitSet copy = BitSet.valueOf(bs0.toLongArray());
      copy.or(bs1);
      return copy;
    }
  }

  @Function.Named("bitset.and")
  final class BitSetAnd extends BitSetBinaryOP
  {
    @Override
    protected BitSet operate(BitSet bs0, BitSet bs1)
    {
      BitSet copy = BitSet.valueOf(bs0.toLongArray());
      copy.and(bs1);
      return copy;
    }
  }

  @Function.Named("bitset.xor")
  final class BitSetXor extends BitSetBinaryOP
  {
    @Override
    protected BitSet operate(BitSet bs0, BitSet bs1)
    {
      BitSet copy = BitSet.valueOf(bs0.toLongArray());
      copy.xor(bs1);
      return copy;
    }
  }

  @Function.Named("bitset.andNot")
  final class BitSetAndNot extends BitSetBinaryOP
  {
    @Override
    protected BitSet operate(BitSet bs0, BitSet bs1)
    {
      BitSet copy = BitSet.valueOf(bs0.toLongArray());
      copy.andNot(bs1);
      return copy;
    }
  }
}
