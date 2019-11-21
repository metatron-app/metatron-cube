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

package io.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;

import java.util.Objects;

public class OperatorKey
{
  private final String name;
  private final SqlSyntax syntax;
  private final boolean external;

  private OperatorKey(final String name, final SqlSyntax syntax, final boolean external)
  {
    this.name = StringUtils.toLowerCase(Preconditions.checkNotNull(name, "name"));
    this.syntax = normalizeSyntax(Preconditions.checkNotNull(syntax, "syntax"));
    this.external = external;
  }

  private static SqlSyntax normalizeSyntax(final SqlSyntax syntax)
  {
    // Treat anything other than prefix/suffix/binary syntax as function syntax.
    if (syntax == SqlSyntax.PREFIX || syntax == SqlSyntax.BINARY || syntax == SqlSyntax.POSTFIX) {
      return syntax;
    } else {
      return SqlSyntax.FUNCTION;
    }
  }

  public static OperatorKey of(final String name, final SqlSyntax syntax)
  {
    return new OperatorKey(name, syntax, false);
  }

  public static OperatorKey of(final SqlOperator operator)
  {
    return new OperatorKey(operator.getName(), operator.getSyntax(), false);
  }

  public static OperatorKey of(final SqlOperator operator, final boolean external)
  {
    return new OperatorKey(operator.getName(), operator.getSyntax(), external);
  }

  public String getName()
  {
    return name;
  }

  public SqlSyntax getSyntax()
  {
    return syntax;
  }

  public boolean isExternal()
  {
    return external;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final OperatorKey that = (OperatorKey) o;
    return Objects.equals(name, that.name) &&
           syntax == that.syntax;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, syntax);
  }

  @Override
  public String toString()
  {
    return "OperatorKey{" +
           "name='" + name + '\'' +
           ", syntax=" + syntax +
           ", external=" + external +
           '}';
  }
}
