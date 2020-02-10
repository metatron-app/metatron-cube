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

package io.druid.query;

import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.segment.column.Column;

import java.util.Collection;

/**
 */
public class RowBinding implements Expr.NumericBinding
{
  private final TypeResolver resolver;
  private volatile Row row;

  public RowBinding(TypeResolver resolver)
  {
    this.resolver = resolver;
  }

  @Override
  public Collection<String> names()
  {
    return row.getColumns();
  }

  @Override
  public Object get(String name)
  {
    if (row == null) {
      return null;
    }
    if (Column.TIME_COLUMN_NAME.equals(name)) {
      return row.getTimestampFromEpoch();
    }
    final Object value = row.getRaw(name);
    if (StringUtils.isNullOrEmpty(value)) {
      return null;
    }
    if (value instanceof String) {
      final ValueType type = resolver.resolve(name, ValueDesc.UNKNOWN).type();
      if (type.isNumeric()) {
        return type.castIfPossible(value);
      }
    }
    return value;
  }

  public void reset(Row row)
  {
    this.row = row;
  }
}
