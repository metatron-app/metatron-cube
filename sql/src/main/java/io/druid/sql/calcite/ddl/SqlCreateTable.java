/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * SK Telecom licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.sql.calcite.ddl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate
{
  private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  private final boolean isTemporary;
  private final SqlNode table;
  private final SqlNodeList columnList;
  private final SqlNode query;

  /**
   * Creates a SqlCreateTable.
   */
  public SqlCreateTable(
      SqlParserPos pos,
      boolean replace,
      boolean isTemporary,
      boolean ifNotExists,
      SqlNode table,
      SqlNodeList columnList,
      SqlNode query
  )
  {
    super(OPERATOR, pos, replace, ifNotExists);
    this.isTemporary = isTemporary;
    this.table = Objects.requireNonNull(table);
    this.columnList = columnList;
    this.query = Objects.requireNonNull(query);
  }

  public boolean isTemporary()
  {
    return isTemporary;
  }

  public boolean ifNotExists()
  {
    return ifNotExists;
  }

  public SqlNode getTable()
  {
    return table;
  }

  public SqlNodeList getColumnList()
  {
    return columnList;
  }

  public SqlNode getQuery()
  {
    return query;
  }

  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.of(table, columnList, query);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    table.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      columnList.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
  }
}
