/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.sql.calcite.ddl;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlDropTable extends SqlDrop
{
  private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

  private final boolean isTemporary;
  private final SqlIdentifier name;

  public SqlDropTable(SqlParserPos pos, boolean isTemporary, boolean ifExists, SqlIdentifier name)
  {
    super(OPERATOR, pos, ifExists);
    this.isTemporary = isTemporary;
    this.name = name;
  }

  public boolean isTemporary()
  {
    return isTemporary;
  }

  public SqlIdentifier getName()
  {
    return name;
  }

  @Override
  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.of(name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("DROP");
    if (isTemporary) {
      writer.keyword("TEMPORARY");
    }
    writer.keyword("TABLE");
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }
}
