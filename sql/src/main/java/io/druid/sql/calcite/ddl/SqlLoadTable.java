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

import io.druid.sql.calcite.SqlProperties;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlLoadTable extends SqlCall
{
  private static final SqlOperator OPERATOR = new SqlSpecialOperator("LOAD TABLE", SqlKind.CREATE_TABLE);

  private final boolean isOverwrite;
  private final boolean isTemporary;
  private final SqlNode path;
  private final SqlIdentifier table;
  private final SqlProperties properties;

  public SqlLoadTable(
      SqlParserPos pos,
      SqlNode path,
      boolean isOverwrite,
      boolean isTemporary,
      SqlIdentifier table,
      SqlProperties properties
  )
  {
    super(pos);
    this.path = Objects.requireNonNull(path);
    this.isOverwrite = isOverwrite;
    this.isTemporary = isTemporary;
    this.table = Objects.requireNonNull(table);
    this.properties = properties;
  }

  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  public boolean isOverwrite()
  {
    return isOverwrite;
  }

  public boolean isTemporary()
  {
    return isTemporary;
  }

  public String getPath()
  {
    return ((SqlLiteral) path).getValueAs(String.class);
  }

  public SqlIdentifier getTable()
  {
    return table;
  }

  public Map<String, Object> getProperties()
  {
    return properties == null ? Collections.emptyMap() : properties.asMap();
  }

  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.of(path, table, properties);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("LOAD");
    writer.keyword("DATA");
    writer.keyword("PATH");
    path.unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();
    if (isOverwrite) {
      writer.keyword("OVERWRITE");
    }
    writer.keyword("INTO");
    if (isTemporary) {
      writer.keyword("TEMPORARY");
    }
    table.unparse(writer, 0, 0);
    if (properties != null) {
      writer.keyword("WITH");
    }
    properties.unparse(writer, 0, 0);
  }
}
