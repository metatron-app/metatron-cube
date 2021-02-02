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

package io.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.sql.calcite.expression.SimpleExtraction;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Type signature for a row in a Druid dataSource ("DruidTable") or query result. Rows have an ordering and every
 * column has a defined type. This is a little bit of a fiction in the Druid world (where rows do not _actually_ have
 * well defined types) but we do impose types for the SQL layer.
 */
public class RowSignature extends io.druid.query.RowSignature
{
  public static final RowSignature EMPTY = new RowSignature(ImmutableList.of(), ImmutableList.of());

  private RowSignature(List<String> columnNames, List<ValueDesc> columnTypes)
  {
    super(columnNames, columnTypes);
  }

  // this is only possible on array output functions.. so it's not right for using for Windowing (todo)
  public static RowSignature from(final RelDataType rowType)
  {
    final Builder rowSignatureBuilder = builder();
    for (RelDataTypeField field : rowType.getFieldList()) {
      final String columnName = field.getName();
      final RelDataType dataType = field.getType();
      final ValueDesc valueType = Calcites.getValueDescForRelDataType(dataType);
      rowSignatureBuilder.add(columnName, valueType);
    }
    return rowSignatureBuilder.build();
  }

  public static RowSignature from(final List<String> rowOrder, final RelDataType rowType)
  {
    return from(rowOrder, rowType, null);
  }

  public static RowSignature from(final List<String> rowOrder, final RelDataType rowType, final TypeResolver resolver)
  {
    if (rowOrder.size() != rowType.getFieldCount()) {
      throw new IAE("Field count %d != %d", rowOrder.size(), rowType.getFieldCount());
    }

    final Builder rowSignatureBuilder = builder();

    for (int i = 0; i < rowOrder.size(); i++) {
      final RelDataTypeField field = rowType.getFieldList().get(i);
      final SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
      if (resolver != null && sqlTypeName == SqlTypeName.NULL) {
        // unknown types are mapped to null type.. we cannot know exact return type a-priori for some UDAFs
        ValueDesc resolved = resolver.resolve(rowOrder.get(i), ValueDesc.UNKNOWN);
        if (!resolved.isUnknown()) {
          rowSignatureBuilder.add(rowOrder.get(i), resolved);
          continue;
        }
      }
      final ValueDesc valueType = Calcites.getValueDescForRelDataType(field.getType());

      rowSignatureBuilder.add(rowOrder.get(i), valueType);
    }

    return rowSignatureBuilder.build();
  }

  public RowSignature subset(ImmutableIntList indices)
  {
    List<String> newColumnNames = Lists.newArrayList();
    List<ValueDesc> newColumnTypes = Lists.newArrayList();
    for (int i = 0; i < indices.size(); i++) {
      int index = indices.get(i);
      newColumnNames.add(columnNames.get(index));
      newColumnTypes.add(columnTypes.get(index));
    }
    return new RowSignature(newColumnNames, newColumnTypes);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builderFrom(RowSignature signature)
  {
    Builder builder = new Builder();
    Iterables.addAll(builder.columnTypeList, signature.columnAndTypes());
    return builder;
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
        || ValueDesc.isStringOrDimension(resolve(simpleExtraction.getColumn()))) {
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
  public RelDataType toRelDataType(final RelDataTypeFactory typeFactory)
  {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      ValueDesc columnType = columnTypes.get(i);
      if (ValueDesc.isDimension(columnType)) {
        columnType = columnType.subElement();
      }
      RelDataType type;
      if (Row.TIME_COLUMN_NAME.equals(columnName)) {
        type = Calcites.createSqlType(typeFactory, SqlTypeName.TIMESTAMP);
      } else {
        type = toRelDataType(typeFactory, columnType);
      }
      builder.add(columnName, type);
    }

    return builder.build();
  }

  private static RelDataType toRelDataType(RelDataTypeFactory typeFactory, ValueDesc columnType)
  {
    switch (columnType.type()) {
      case STRING:
        // Note that there is no attempt here to handle multi-value in any special way. Maybe one day...
        return Calcites.createSqlTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, true);
      case BOOLEAN:
        return Calcites.createSqlType(typeFactory, SqlTypeName.BOOLEAN);
      case LONG:
        return Calcites.createSqlType(typeFactory, SqlTypeName.BIGINT);
      case FLOAT:
        return Calcites.createSqlType(typeFactory, SqlTypeName.FLOAT);
      case DOUBLE:
        return Calcites.createSqlType(typeFactory, SqlTypeName.DOUBLE);
      case COMPLEX:
        final String[] description = columnType.getDescription();
        if (columnType.isStruct() && description != null) {
          final List<String> fieldNames = Lists.newArrayList();
          final List<RelDataType> fieldTypes = Lists.newArrayList();
          for (int i = 1; i < description.length; i++) {
            int index = description[i].indexOf(':');
            fieldNames.add(description[i].substring(0, index));
            fieldTypes.add(toRelDataType(typeFactory, ValueDesc.of(description[i].substring(index + 1))));
          }
          return typeFactory.createStructType(StructKind.PEEK_FIELDS, fieldTypes, fieldNames);
        }
        // Loses information about exactly what kind of complex column this is.
        SqlTypeName typeName = SqlTypeName.OTHER;
        if (columnType.isMap()) {
          typeName = SqlTypeName.MAP;
        } else if (columnType.isList() || columnType.isStruct()) {
          typeName = SqlTypeName.ARRAY;
        }
        return Calcites.createSqlTypeWithNullability(typeFactory, typeName, true);
      default:
        throw new ISE("valueType[%s] not translatable?", columnType);
    }
  }

  public RowSignature replaceColumnNames(List<String> newColumnNames)
  {
    Preconditions.checkArgument(columnNames.size() == newColumnNames.size(), "inconsistent");
    return new RowSignature(newColumnNames, columnTypes);
  }

  public static class Builder
  {
    private final List<Pair<String, ValueDesc>> columnTypeList;

    private Builder()
    {
      this.columnTypeList = new ArrayList<>();
    }

    public Builder add(String columnName, ValueDesc columnType)
    {
      Preconditions.checkNotNull(columnName, "columnName");
      Preconditions.checkNotNull(columnType, "columnType");

      columnTypeList.add(Pair.of(columnName, columnType));
      return this;
    }

    public Builder sort()
    {
      Collections.sort(columnTypeList, new Comparator<Pair<String, ValueDesc>>()
      {
        @Override
        public int compare(Pair<String, ValueDesc> o1, Pair<String, ValueDesc> o2)
        {
          return o1.lhs.compareTo(o2.lhs);
        }
      });
      return this;
    }

    public RowSignature build()
    {
      List<String> columnNames = Lists.newArrayList(Iterables.transform(columnTypeList, Pair.lhsFn()));
      List<ValueDesc> columnTypes = Lists.newArrayList(Iterables.transform(columnTypeList, Pair.rhsFn()));
      return new RowSignature(columnNames, columnTypes);
    }
  }
}
