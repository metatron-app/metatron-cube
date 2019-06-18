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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlInsertDirectory extends SqlCall
{
  private final boolean isOverwrite;
  private final SqlNode directory;
  private final SqlNode format;
  private final Map properties;
  private final SqlNode query;

  public SqlInsertDirectory(
      SqlParserPos pos,
      boolean isOverwrite,
      SqlNode directory,
      SqlNode format,
      Map properties,
      SqlNode query
  )
  {
    super(pos);
    this.isOverwrite = isOverwrite;
    this.directory = Objects.requireNonNull(directory);
    this.format = format;
    this.properties = properties;
    this.query = Objects.requireNonNull(query);
  }

  @Override
  public SqlOperator getOperator()
  {
    return SqlInsert.OPERATOR;
  }

  public boolean isOverwrite()
  {
    return isOverwrite;
  }

  public String getDirectory()
  {
    return ((SqlLiteral) directory).getValueAs(String.class);
  }

  public String getFormat()
  {
    return format == null ? "csv" : ((SqlLiteral) format).getValueAs(String.class);
  }

  public Map getProperties()
  {
    return properties;
  }

  public SqlNode getQuery()
  {
    return query;
  }

  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.of(directory, query);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("INSERT");
    if (isOverwrite) {
      writer.keyword("OVERWRITE");
    }
    writer.keyword("INTO");
    writer.keyword("DIRECTORY");
    directory.unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
  }
}
