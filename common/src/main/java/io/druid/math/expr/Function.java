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

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import io.druid.math.expr.Expr.NumericBinding;

import java.util.List;

/**
 */
public interface Function
{
  String name();

  ExprEval apply(List<Expr> args, NumericBinding bindings);

  interface Factory extends Supplier<Function>
  {
    String name();
  }

  abstract class NewInstance implements Factory, Function
  {
    public Function get()
    {
      try {
        return getClass().newInstance();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  interface Library
  {
  }
}
