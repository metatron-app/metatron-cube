/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.data.input;

import io.druid.data.TypeResolver;
import io.druid.math.expr.ExprEval;
import io.druid.query.RowBinding;

/**
 */
public class RowExprBinding extends RowBinding
{
  private final String defaultColumn;
  private volatile boolean evaluated;
  private volatile ExprEval tempResult;

  public RowExprBinding(String defaultColumn, TypeResolver types)
  {
    super(types);
    this.defaultColumn = defaultColumn;
  }

  public void set(ExprEval eval)
  {
    this.evaluated = true;
    this.tempResult = eval;
  }

  @Override
  public Object get(String name)
  {
    if ("_".equals(name)) {
      return evaluated ? tempResult.value() : super.get(defaultColumn);
    }
    return super.get(name);
  }

  @Override
  public void reset(Row row)
  {
    super.reset(row);
    this.evaluated = false;
    this.tempResult = null;
  }

  public ExprEval get()
  {
    return tempResult;
  }
}
