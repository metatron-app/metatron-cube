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

package io.druid.sql.calcite;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Map;

public class SqlProperties extends SqlCall
{
  protected final List<SqlNode> values;

  public SqlProperties(List<SqlNode> values, SqlParserPos pos)
  {
    super(pos);
    this.values = values;
  }

  @Override
  public SqlOperator getOperator()
  {
    return SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR;
  }

  @Override
  public List<SqlNode> getOperandList()
  {
    return values;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (int i = 0; i < values.size(); i += 2) {
      writer.sep(",");
      writer.literal(((SqlLiteral) values.get(i)).toValue());
      writer.keyword("=>");
      writer.literal(((SqlLiteral) values.get(i + 1)).toValue());
    }
    writer.endList(frame);
  }

  public Map<String, Object> asMap()
  {
    Map<String, Object> properties = Maps.newHashMap();
    for (int i = 0; i < values.size(); i += 2) {
      String key = ((SqlLiteral) values.get(i)).toValue();
      Comparable value = SqlLiteral.value(values.get(i + 1));
      if (value instanceof NlsString) {
        value = ((NlsString) value).getValue();
      }
      properties.put(key, value);
    }
    return properties;
  }
}
