package io.druid.sql.calcite.ddl;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;

import java.util.List;

public class Utils
{
  static SqlNode createCondition(SqlNode left, SqlOperator op, SqlNode right)
  {
    List<Object> listCondition = Lists.newArrayList();
    listCondition.add(left);
    listCondition.add(new SqlParserUtil.ToTreeListItem(op, SqlParserPos.ZERO));
    listCondition.add(right);

    return SqlParserUtil.toTree(listCondition);
  }
}
