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

import java.util.List;

/**
 */
public interface Expression
{
  interface RelationExpression extends Expression
  {
    <T extends Expression> List<T> getChildren();
  }

  interface NotExpression extends RelationExpression
  {
    <T extends Expression> T getChild();
  }

  interface AndExpression extends RelationExpression
  {
  }

  interface OrExpression extends RelationExpression
  {
  }

  // optional
  interface FuncExpression extends Expression
  {
    String op();

    <T extends Expression> List<T> getChildren();
  }

  // optional
  interface ConstExpression extends Expression
  {
    Object get();
  }

  interface Factory<T extends Expression>
  {
    T or(List<T> children);
    T and(List<T> children);
    T not(T expression);
  }
}
