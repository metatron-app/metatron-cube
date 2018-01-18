/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.table;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.sql.calcite.expression.SimpleExtraction;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Type signature for a row in a Druid dataSource ("DruidTable") or query result. Rows have an ordering and every
 * column has a defined type. This is a little bit of a fiction in the Druid world (where rows do not _actually_ have
 * well defined types) but we do impose types for the SQL layer.
 */
public class RowSignature implements io.druid.query.RowSignature, Function<String, ValueDesc>
{
  public static RowSignature from(IncrementalIndexSchema schema)
  {
    RowSignature.Builder builder = RowSignature.builder();
    for (DimensionSchema dimensionSchema : schema.getDimensionsSpec().getDimensions()) {
      builder.add(dimensionSchema.getName(), ValueDesc.ofDimension(dimensionSchema.getValueType()));
    }
    for (AggregatorFactory factory : schema.getMetrics()) {
      builder.add(factory.getName(), ValueDesc.of(factory.getTypeName()));
    }
    return builder.build();
  }

  private final Map<String, ValueDesc> columnTypes;
  private final List<String> columnNames;

  private RowSignature(final List<Pair<String, ValueDesc>> columnTypeList)
  {
    final Map<String, ValueDesc> columnTypes0 = Maps.newHashMap();
    final ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();

    for (Pair<String, ValueDesc> pair : columnTypeList) {
      final ValueDesc existingType = columnTypes0.get(pair.lhs);
      if (existingType != null && existingType != pair.rhs) {
        throw new IAE("Column[%s] has conflicting types [%s] and [%s]", pair.lhs, existingType, pair.rhs);
      }

      columnTypes0.put(pair.lhs, pair.rhs);
      columnNamesBuilder.add(pair.lhs);
    }

    this.columnTypes = ImmutableMap.copyOf(columnTypes0);
    this.columnNames = columnNamesBuilder.build();
  }

  public static RowSignature from(final List<String> rowOrder, final RelDataType rowType)
  {
    if (rowOrder.size() != rowType.getFieldCount()) {
      throw new IAE("Field count %d != %d", rowOrder.size(), rowType.getFieldCount());
    }

    final RowSignature.Builder rowSignatureBuilder = builder();

    for (int i = 0; i < rowOrder.size(); i++) {
      final RelDataTypeField field = rowType.getFieldList().get(i);
      final SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
      final ValueDesc valueDesc = Calcites.getValueDescForSqlTypeName(sqlTypeName);
      if (valueDesc == null) {
        throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, rowOrder.get(i));
      }

      rowSignatureBuilder.add(rowOrder.get(i), valueDesc);
    }

    return rowSignatureBuilder.build();
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public ValueDesc getColumnType(final String name)
  {
    return columnTypes.get(name);
  }

  @Override
  public ValueDesc apply(String input)
  {
    return getColumnType(input);
  }

  /**
   * Returns the rowOrder for this signature, which is the list of column names in row order.
   *
   * @return row order
   */
  public List<String> getRowOrder()
  {
    return columnNames;
  }

  /**
   * Return the "natural" {@link StringComparator} for an extraction from this row signature. This will be a
   * lexicographic comparator for String types and a numeric comparator for Number types.
   *
   * @param simpleExtraction extraction from this kind of row
   *
   * @return natural comparator
   */
  @Nonnull
  public String naturalStringComparator(final SimpleExtraction simpleExtraction)
  {
    Preconditions.checkNotNull(simpleExtraction, "simpleExtraction");
    if (simpleExtraction.getExtractionFn() != null
        || ValueDesc.isDimension(getColumnType(simpleExtraction.getColumn()))
        || ValueDesc.isString(getColumnType(simpleExtraction.getColumn()))) {
      return StringComparators.LEXICOGRAPHIC_NAME;
    } else {
      return StringComparators.NUMERIC_NAME;
    }
  }

  /**
   * Returns a Calcite RelDataType corresponding to this row signature.
   *
   * @param typeFactory factory for type construction
   *
   * @return Calcite row type
   */
  public RelDataType getRelDataType(final RelDataTypeFactory typeFactory)
  {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (final String columnName : columnNames) {
      ValueDesc columnType = getColumnType(columnName);
      if (ValueDesc.isDimension(columnType)) {
        columnType = columnType.subElement();
      }
      final RelDataType type;

      if (Column.TIME_COLUMN_NAME.equals(columnName)) {
        type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      } else if (ValueDesc.isPrimitive(columnType)) {
        switch (columnType.type()) {
          case STRING:
            // Note that there is no attempt here to handle multi-value in any special way. Maybe one day...
            type = typeFactory.createTypeWithNullability(
                typeFactory.createTypeWithCharsetAndCollation(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    Calcites.defaultCharset(),
                    SqlCollation.IMPLICIT
                ),
                true
            );
            break;
          case LONG:
            type = typeFactory.createSqlType(SqlTypeName.BIGINT);
            break;
          case FLOAT:
            type = typeFactory.createSqlType(SqlTypeName.FLOAT);
            break;
          case DOUBLE:
            type = typeFactory.createSqlType(SqlTypeName.DOUBLE);
            break;
          case COMPLEX:
            // Loses information about exactly what kind of complex column this is.
            type = typeFactory.createSqlType(SqlTypeName.OTHER);
            break;
          default:
            throw new ISE("WTF?! ValueDesc[%s] not translatable?", columnType);
        }
      } else {
        type = new ObjectSqlType(
            SqlTypeName.OTHER, new SqlIdentifier(columnType.typeName(), null, SqlParserPos.ZERO), true, null,
            RelDataTypeComparability.NONE
        );
      }

      builder.add(columnName, type);
    }

    return builder.build();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RowSignature that = (RowSignature) o;

    if (columnTypes != null ? !columnTypes.equals(that.columnTypes) : that.columnTypes != null) {
      return false;
    }
    return columnNames != null ? columnNames.equals(that.columnNames) : that.columnNames == null;
  }

  @Override
  public int hashCode()
  {
    int result = columnTypes != null ? columnTypes.hashCode() : 0;
    result = 31 * result + (columnNames != null ? columnNames.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder s = new StringBuilder("{");
    for (int i = 0; i < columnNames.size(); i++) {
      if (i > 0) {
        s.append(", ");
      }
      final String columnName = columnNames.get(i);
      s.append(columnName).append(":").append(getColumnType(columnName));
    }
    return s.append("}").toString();
  }

  @Override
  public List<String> getColumnNames()
  {
    return ImmutableList.copyOf(columnNames);
  }

  @Override
  public List<ValueDesc> getColumnTypes()
  {
    return ImmutableList.copyOf(Lists.transform(columnNames, this));
  }

  public static class Builder
  {
    private final List<Pair<String, ValueDesc>> columnTypeList;

    private Builder()
    {
      this.columnTypeList = Lists.newArrayList();
    }

    public Builder add(String columnName, ValueDesc columnType)
    {
      Preconditions.checkNotNull(columnName, "columnName");
      Preconditions.checkNotNull(columnType, "columnType");

      columnTypeList.add(Pair.of(columnName, columnType));
      return this;
    }

    public RowSignature build()
    {
      return new RowSignature(columnTypeList);
    }
  }
}