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

package io.druid.sql.calcite.ddl;

import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.schema.InformationSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;

public class SqlDescTable
{
  public static SqlNode rewrite(
      SqlParserPos pos,
      SqlIdentifier table,
      SqlIdentifier column,
      SqlNode columnPattern
  )
  {
    List<SqlNode> select = Arrays.asList(
        Utils.zero(InformationSchema.COLUMN_NAME),
        Utils.zero(InformationSchema.DATA_TYPE),
        Utils.zero(InformationSchema.DATA_TYPE_EXTENDED),
        Utils.zero(InformationSchema.IS_NULLABLE),
        Utils.zero(InformationSchema.COLUMN_DESCS)
    );

    SqlNode fromClause = new SqlIdentifier(
        Arrays.asList(InformationSchema.NAME, InformationSchema.COLUMNS_TABLE), SqlParserPos.ZERO
    );

    String tableName = Util.last(table.names);
    SqlNode tableNameColumn = Utils.zero(InformationSchema.TABLE_NAME);

    SqlNode where = Utils.createCondition(
        tableNameColumn,
        SqlStdOperatorTable.EQUALS,
        SqlLiteral.createCharString(tableName, Util.getDefaultCharset().name(), SqlParserPos.ZERO)
    );

    SqlNode columnFilter = null;
    if (column != null) {
      columnFilter =
          Utils.createCondition(
              SqlStdOperatorTable.LOWER.createCall(
                  SqlParserPos.ZERO,
                  Utils.zero(InformationSchema.COLUMN_NAME)
              ),
              SqlStdOperatorTable.EQUALS,
              SqlLiteral.createCharString(column.toString().toLowerCase(), SqlParserPos.ZERO)
          );
    } else if (columnPattern != null) {
      SqlNode columnNameColumn = Utils.zero(InformationSchema.COLUMN_NAME);
      // for case insensitive schema
//      if (columnPattern instanceof SqlCharStringLiteral) {
//        NlsString conditionString = ((SqlCharStringLiteral) columnPattern).getNlsString();
//        columnPattern = SqlCharStringLiteral.createCharString(
//            conditionString.getValue().toLowerCase(),
//            conditionString.getCharsetName(),
//            columnPattern.getParserPosition()
//        );
//        columnNameColumn = SqlStdOperatorTable.LOWER.createCall(SqlParserPos.ZERO, columnNameColumn);
//      }
      columnFilter = Utils.createCondition(columnNameColumn, SqlStdOperatorTable.LIKE, columnPattern);
    }

    if (columnFilter != null) {
      where = Utils.createCondition(where, SqlStdOperatorTable.AND, columnFilter);
    }

    SqlNodeList selectList = new SqlNodeList(select, SqlParserPos.ZERO);
    return new SqlSelect(
        SqlParserPos.ZERO, null, selectList, fromClause, where, null, null, null, null, null, null
    );
  }
}
