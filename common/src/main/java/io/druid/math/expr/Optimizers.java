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

import com.google.common.base.Supplier;
import io.druid.common.guava.DSuppliers.TypedSupplier;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.ISE;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;

import java.util.Map;

public class Optimizers
{
  private static final Expr.FloatOptimized FLOAT_NULL = new Expr.FloatOptimized(h -> false);
  private static final Expr.DoubleOptimized DOUBLE_NULL = new Expr.DoubleOptimized(h -> false);
  private static final Expr.LongOptimized LONG_NULL = new Expr.LongOptimized(h -> false);

  static TypedSupplier toTypedSupplier(Expr expr, Map<String, TypedSupplier> suppliers, ValueDesc type)
  {
    if (Evals.isIdentifier(expr)) {
      return cast(suppliers.getOrDefault(Evals.getIdentifier(expr), null), type);
    } else if (Evals.isConstant(expr)) {
      ExprEval eval = Evals.getConstantEval(expr);
      Object value = type.equals(eval.type()) ? eval.value() : Evals.castTo(eval, type).value();
      return asTypedSupplier(type, value);
    } else if (expr instanceof Expr.Optimized) {
      return (TypedSupplier) expr;
    }
    return null;
  }

  private static TypedSupplier cast(TypedSupplier supplier, ValueDesc castTo)
  {
    if (supplier == null || castTo.equals(supplier.type())) {
      return supplier;
    }
    if (supplier instanceof FloatColumnSelector && castTo.isDouble()) {
      if (supplier instanceof Expr.FloatOptimized) {
        final MutableFloat fv = new MutableFloat();
        final Expr.FloatOptimized selector = (Expr.FloatOptimized) supplier;
        return new Expr.DoubleOptimized(h -> {
          if (selector.getFloat(fv)) {
            h.setValue(fv.doubleValue());
            return true;
          }
          return false;
        });
      } else {
        final MutableFloat fv = new MutableFloat();
        final FloatColumnSelector selector = (FloatColumnSelector) supplier;
        return new Expr.DoubleOptimized(h -> {
          if (selector.getFloat(fv)) {
            h.setValue(fv.doubleValue());
            return true;
          }
          return false;
        });
      }
    } else if (supplier instanceof LongColumnSelector && castTo.isDouble()) {
      if (supplier instanceof Expr.LongOptimized) {
        final MutableLong lv = new MutableLong();
        final Expr.LongOptimized selector = (Expr.LongOptimized) supplier;
        return new Expr.DoubleOptimized(h -> {
          if (selector.getLong(lv)) {
            h.setValue(lv.doubleValue());
            return true;
          }
          return false;
        });
      } else {
        final MutableLong lv = new MutableLong();
        final LongColumnSelector selector = (LongColumnSelector) supplier;
        return new Expr.DoubleOptimized(h -> {
          if (selector.getLong(lv)) {
            h.setValue(lv.doubleValue());
            return true;
          }
          return false;
        });
      }
    }
    return supplier;
  }

  private static TypedSupplier asTypedSupplier(ValueDesc type, Object constant)
  {
    switch (type.type()) {
      case FLOAT:
        if (constant == null) {
          return FLOAT_NULL;
        }
        final float fv = ((Number) constant).floatValue();
        return new Expr.FloatOptimized(h -> {
          h.setValue(fv);
          return true;
        });
      case DOUBLE:
        if (constant == null) {
          return DOUBLE_NULL;
        }
        final double dv = ((Number) constant).doubleValue();
        return new Expr.DoubleOptimized(h -> {
          h.setValue(dv);
          return true;
        });
      case LONG:
        if (constant == null) {
          return LONG_NULL;
        }
        final long lv = ((Number) constant).longValue();
        return new Expr.LongOptimized(h -> {
          h.setValue(lv);
          return true;
        });
    }
    return new TypedSupplier.FixedTyped(type)
    {
      @Override
      public Object get() {return constant;}
    };
  }

  static Expr optimize(ValueType type, Supplier supplier1, Supplier supplier2, Arithmetics calculator)
  {
    switch (type) {
      case FLOAT:
        return floatOp(supplier1, supplier2, calculator);
      case LONG:
        return longOp(supplier1, supplier2, calculator);
      case DOUBLE:
        return doubleOp(supplier1, supplier2, calculator);
    }
    throw new ISE("Not supported type %s", type);
  }

  private static Expr.DoubleOptimized doubleOp(Supplier supplier1, Supplier supplier2, Arithmetics calculator)
  {
    if (supplier1 instanceof DoubleColumnSelector && supplier2 instanceof DoubleColumnSelector) {
      if (supplier1 instanceof Expr.DoubleOptimized && supplier2 instanceof Expr.DoubleOptimized) {
        final Expr.DoubleOptimized selector1 = (Expr.DoubleOptimized) supplier1;
        final Expr.DoubleOptimized selector2 = (Expr.DoubleOptimized) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final double dv1 = h.doubleValue();
            if (selector2.getDouble(h)) {
              h.setValue(calculator.op(dv1, h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier1 instanceof Expr.DoubleOptimized) {
        final Expr.DoubleOptimized selector1 = (Expr.DoubleOptimized) supplier1;
        final DoubleColumnSelector selector2 = (DoubleColumnSelector) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final double dv1 = h.doubleValue();
            if (selector2.getDouble(h)) {
              h.setValue(calculator.op(dv1, h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier2 instanceof Expr.DoubleOptimized) {
        final DoubleColumnSelector selector1 = (DoubleColumnSelector) supplier1;
        final Expr.DoubleOptimized selector2 = (Expr.DoubleOptimized) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final double dv1 = h.doubleValue();
            if (selector2.getDouble(h)) {
              h.setValue(calculator.op(dv1, h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final DoubleColumnSelector selector1 = (DoubleColumnSelector) supplier1;
        final DoubleColumnSelector selector2 = (DoubleColumnSelector) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final double dv1 = h.doubleValue();
            if (selector2.getDouble(h)) {
              h.setValue(calculator.op(dv1, h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier1 instanceof DoubleColumnSelector) {
      if (supplier1 instanceof Expr.DoubleOptimized) {
        final Expr.DoubleOptimized selector1 = (Expr.DoubleOptimized) supplier1;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final Number dv2 = (Number) supplier2.get();
            if (dv2 != null) {
              h.setValue(calculator.op(h.doubleValue(), dv2.doubleValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final DoubleColumnSelector selector1 = (DoubleColumnSelector) supplier1;
        return new Expr.DoubleOptimized(h -> {
          if (selector1.getDouble(h)) {
            final Number dv2 = (Number) supplier2.get();
            if (dv2 != null) {
              h.setValue(calculator.op(h.doubleValue(), dv2.doubleValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier2 instanceof DoubleColumnSelector) {
      if (supplier2 instanceof Expr.DoubleOptimized) {
        final Expr.DoubleOptimized selector2 = (Expr.DoubleOptimized) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector2.getDouble(h)) {
            final Number dv1 = (Number) supplier1.get();
            if (dv1 != null) {
              h.setValue(calculator.op(dv1.doubleValue(), h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final DoubleColumnSelector selector2 = (DoubleColumnSelector) supplier2;
        return new Expr.DoubleOptimized(h -> {
          if (selector2.getDouble(h)) {
            final Number dv1 = (Number) supplier1.get();
            if (dv1 != null) {
              h.setValue(calculator.op(dv1.doubleValue(), h.doubleValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else {
      return new Expr.DoubleOptimized(h -> {
        final Number v1 = (Number) supplier1.get();
        final Number v2 = (Number) supplier2.get();
        if (v1 == null || v2 == null) {
          return false;
        }
        h.setValue(calculator.op(v1.doubleValue(), v2.doubleValue()));
        return true;
      });
    }
  }

  private static Expr.FloatOptimized floatOp(Supplier supplier1, Supplier supplier2, Arithmetics calculator)
  {
    if (supplier1 instanceof FloatColumnSelector && supplier2 instanceof FloatColumnSelector) {
      if (supplier1 instanceof Expr.FloatOptimized && supplier2 instanceof Expr.FloatOptimized) {
        final Expr.FloatOptimized selector1 = (Expr.FloatOptimized) supplier1;
        final Expr.FloatOptimized selector2 = (Expr.FloatOptimized) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final float fv1 = h.floatValue();
            if (selector2.getFloat(h)) {
              h.setValue(calculator.op(fv1, h.floatValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier1 instanceof Expr.FloatOptimized) {
        final Expr.FloatOptimized selector1 = (Expr.FloatOptimized) supplier1;
        final FloatColumnSelector selector2 = (FloatColumnSelector) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final float fv1 = h.floatValue();
            if (selector2.getFloat(h)) {
              h.setValue(calculator.op(fv1, h.floatValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier2 instanceof Expr.FloatOptimized) {
        final FloatColumnSelector selector1 = (FloatColumnSelector) supplier1;
        final Expr.FloatOptimized selector2 = (Expr.FloatOptimized) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final float fv1 = h.floatValue();
            if (selector2.getFloat(h)) {
              h.setValue(calculator.op(fv1, h.floatValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final FloatColumnSelector selector1 = (FloatColumnSelector) supplier1;
        final FloatColumnSelector selector2 = (FloatColumnSelector) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final float fv1 = h.floatValue();
            if (selector2.getFloat(h)) {
              h.setValue(calculator.op(fv1, h.floatValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier1 instanceof FloatColumnSelector) {
      if (supplier1 instanceof Expr.FloatOptimized) {
        final Expr.FloatOptimized selector1 = (Expr.FloatOptimized) supplier1;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final Number fv2 = (Number) supplier2.get();
            if (fv2 != null) {
              h.setValue(calculator.op(h.floatValue(), fv2.floatValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final FloatColumnSelector selector1 = (FloatColumnSelector) supplier1;
        return new Expr.FloatOptimized(h -> {
          if (selector1.getFloat(h)) {
            final Number fv2 = (Number) supplier2.get();
            if (fv2 != null) {
              h.setValue(calculator.op(h.floatValue(), fv2.floatValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier2 instanceof FloatColumnSelector) {
      if (supplier2 instanceof Expr.FloatOptimized) {
        final Expr.FloatOptimized selector2 = (Expr.FloatOptimized) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector2.getFloat(h)) {
            final Number fv1 = (Number) supplier1.get();
            if (fv1 != null) {
              h.setValue(calculator.op(fv1.floatValue(), h.floatValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final FloatColumnSelector selector2 = (FloatColumnSelector) supplier2;
        return new Expr.FloatOptimized(h -> {
          if (selector2.getFloat(h)) {
            final Number fv1 = (Number) supplier1.get();
            if (fv1 != null) {
              h.setValue(calculator.op(fv1.floatValue(), h.floatValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else {
      return new Expr.FloatOptimized(h -> {
        final Number v1 = (Number) supplier1.get();
        final Number v2 = (Number) supplier2.get();
        if (v1 == null || v2 == null) {
          return false;
        }
        h.setValue(calculator.op(v1.floatValue(), v2.floatValue()));
        return true;
      });
    }
  }

  private static Expr.LongOptimized longOp(Supplier supplier1, Supplier supplier2, Arithmetics calculator)
  {
    if (supplier1 instanceof LongColumnSelector && supplier2 instanceof LongColumnSelector) {
      if (supplier1 instanceof Expr.LongOptimized && supplier2 instanceof Expr.LongOptimized) {
        final Expr.LongOptimized selector1 = (Expr.LongOptimized) supplier1;
        final Expr.LongOptimized selector2 = (Expr.LongOptimized) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final long lv1 = h.longValue();
            if (selector2.getLong(h)) {
              h.setValue(calculator.op(lv1, h.longValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier1 instanceof Expr.LongOptimized) {
        final Expr.LongOptimized selector1 = (Expr.LongOptimized) supplier1;
        final LongColumnSelector selector2 = (LongColumnSelector) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final long lv1 = h.longValue();
            if (selector2.getLong(h)) {
              h.setValue(calculator.op(lv1, h.longValue()));
              return true;
            }
          }
          return false;
        });
      } else if (supplier2 instanceof Expr.LongOptimized) {
        final LongColumnSelector selector1 = (LongColumnSelector) supplier1;
        final Expr.LongOptimized selector2 = (Expr.LongOptimized) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final long lv1 = h.longValue();
            if (selector2.getLong(h)) {
              h.setValue(calculator.op(lv1, h.longValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final LongColumnSelector selector1 = (LongColumnSelector) supplier1;
        final LongColumnSelector selector2 = (LongColumnSelector) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final long lv1 = h.longValue();
            if (selector2.getLong(h)) {
              h.setValue(calculator.op(lv1, h.longValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier1 instanceof LongColumnSelector) {
      if (supplier1 instanceof Expr.LongOptimized) {
        final Expr.LongOptimized selector1 = (Expr.LongOptimized) supplier1;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final Number lv2 = (Number) supplier2.get();
            if (lv2 != null) {
              h.setValue(calculator.op(h.longValue(), lv2.longValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final LongColumnSelector selector1 = (LongColumnSelector) supplier1;
        return new Expr.LongOptimized(h -> {
          if (selector1.getLong(h)) {
            final Number lv2 = (Number) supplier2.get();
            if (lv2 != null) {
              h.setValue(calculator.op(h.longValue(), lv2.longValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else if (supplier2 instanceof LongColumnSelector) {
      if (supplier2 instanceof Expr.LongOptimized) {
        final Expr.LongOptimized selector2 = (Expr.LongOptimized) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector2.getLong(h)) {
            final Number lv1 = (Number) supplier1.get();
            if (lv1 != null) {
              h.setValue(calculator.op(lv1.longValue(), h.longValue()));
              return true;
            }
          }
          return false;
        });
      } else {
        final LongColumnSelector selector2 = (LongColumnSelector) supplier2;
        return new Expr.LongOptimized(h -> {
          if (selector2.getLong(h)) {
            final Number lv1 = (Number) supplier1.get();
            if (lv1 != null) {
              h.setValue(calculator.op(lv1.longValue(), h.longValue()));
              return true;
            }
          }
          return false;
        });
      }
    } else {
      return new Expr.LongOptimized(h -> {
        final Number v1 = (Number) supplier1.get();
        final Number v2 = (Number) supplier2.get();
        if (v1 == null || v2 == null) {
          return false;
        }
        h.setValue(calculator.op(v1.longValue(), v2.longValue()));
        return true;
      });
    }
  }
}
