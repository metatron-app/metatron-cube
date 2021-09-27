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

package org.apache.calcite.sql.type;

import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.sql.calcite.DruidType;
import io.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.calcite.sql.SqlCollation;

import java.nio.charset.Charset;

public class DruidAnyType extends BasicSqlType implements DruidType
{
  public static DruidAnyType of(ValueDesc druidType, boolean nullable)
  {
    return new DruidAnyType(druidType, nullable);
  }

  private final ValueDesc druidType;
  private final boolean nullable;

  private DruidAnyType(ValueDesc druidType, boolean nullable)
  {
    super(DruidTypeSystem.INSTANCE, SqlTypeName.ANY, 0, 0);
    this.druidType = Preconditions.checkNotNull(druidType);
    this.nullable = nullable;
    computeDigest();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail)
  {
    sb.append(druidType);
  }

  @Override
  public boolean isNullable()
  {
    return nullable;
  }

  @Override
  public Charset getCharset()
  {
    return StringUtils.UTF8_CHARSET;
  }

  @Override
  public SqlCollation getCollation()
  {
    return SqlCollation.IMPLICIT;
  }

  @Override
  public DruidAnyType createWithNullability(boolean nullable)
  {
    return this.nullable ^ nullable ? of(druidType, nullable) : this;
  }

  @Override
  public ValueDesc getDruidType()
  {
    return druidType;
  }
}
