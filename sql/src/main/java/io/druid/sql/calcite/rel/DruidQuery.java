package io.druid.sql.calcite.rel;

import io.druid.query.Query;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.type.RelDataType;

public interface DruidQuery
{
  RelDataType getOutputRowType();

  RowSignature getOutputRowSignature();

  Query getQuery();
}
