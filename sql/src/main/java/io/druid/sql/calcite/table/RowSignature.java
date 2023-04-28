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
import io.druid.collections.IntList;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Type signature for a row in a Druid dataSource ("DruidTable") or query result. Rows have an ordering and every
 * column has a defined type. This is a little bit of a fiction in the Druid world (where rows do not _actually_ have
 * well-defined types) but we do impose types for the SQL layer.
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
      final ValueDesc valueType = Calcites.asValueDesc(dataType);
      rowSignatureBuilder.add(columnName, valueType);
    }
    return rowSignatureBuilder.build();
  }

  public static RowSignature fromTypeString(String typeString, ValueDesc defaultType)
  {
    return from(io.druid.query.RowSignature.fromTypeString(typeString, defaultType));
  }

  public static RowSignature from(io.druid.data.RowSignature signature)
  {
    return new RowSignature(signature.getColumnNames(), signature.getColumnTypes());
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
      final ValueDesc valueType = Calcites.asValueDesc(field.getType());

      rowSignatureBuilder.add(rowOrder.get(i), valueType);
    }

    return rowSignatureBuilder.build();
  }

  public RowSignature subset(ImmutableIntList indices)
  {
    return subset(IntList.of(indices.toIntArray()));
  }

  public RowSignature subset(IntList indices)
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

  @Override
  public RowSignature append(String name, ValueDesc type)
  {
    return new RowSignature(
        GuavaUtils.concat(getColumnNames(), name),
        GuavaUtils.concat(getColumnTypes(), type)
    );
  }

  @Override
  public RowSignature append(List<String> names, List<ValueDesc> types)
  {
    return new RowSignature(
        GuavaUtils.concat(getColumnNames(), names),
        GuavaUtils.concat(getColumnTypes(), types)
    );
  }

  public RowSignature prepend(String name, ValueDesc type)
  {
    return new RowSignature(
        GuavaUtils.concat(name, getColumnNames()),
        GuavaUtils.concat(type, getColumnTypes())
    );
  }

  public RowSignature append(RowSignature signature)
  {
    return append(signature.getColumnNames(), signature.getColumnTypes());
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builderFrom(io.druid.data.RowSignature signature)
  {
    return new Builder(signature);
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
      builder.add(columnName, Calcites.asRelDataType(columnName, columnTypes.get(i)));
    }
    return builder.build();
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

    private Builder(io.druid.data.RowSignature signature)
    {
      this.columnTypeList = Lists.newArrayList(signature.columnAndTypes());
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
