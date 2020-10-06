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

package io.druid.query.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;

import java.util.List;
import java.util.Objects;

public class ValuesFilter extends DimFilter.FilterFactory implements DimFilter.Rewriting
{
  public static ValuesFilter fieldNames(List<String> fieldNames, Supplier<List<Object[]>> fieldValues)
  {
    return new ValuesFilter(fieldNames, fieldValues);
  }

  private final List<String> fieldNames;
  private final Supplier<List<Object[]>> fieldValues;

  public ValuesFilter(List<String> fieldNames, Supplier<List<Object[]>> fieldValues)
  {
    this.fieldNames = fieldNames;
    this.fieldValues = fieldValues;
    Preconditions.checkArgument(fieldNames.size() > 0, "'fieldNames' is empty");
  }

  @Override
  public DimFilter rewrite(QuerySegmentWalker walker, Query parent)
  {
    final List<Object[]> values = fieldValues.get();
    if (fieldNames.size() == 1) {
      return new InDimFilter(
          fieldNames.get(0), GuavaUtils.transform(values, array -> Objects.toString(array[0], null)), null
      );
    } else {
      List<List<String>> valuesList = Lists.newArrayList();
      for (int i = 0; i < fieldNames.size(); i++) {
        valuesList.add(Lists.newArrayList());
      }
      for (Object[] array : values) {
        for (int i = 0; i < fieldNames.size(); i++) {
          valuesList.get(i).add(Objects.toString(array[i], null));
        }
      }
      return new InDimsFilter(fieldNames, valuesList);
    }
  }

  @Override
  public String toString()
  {
    return "ValuesFilter{fieldNames=" + fieldNames + ", fieldValues=" + fieldValues + '}';
  }
}
