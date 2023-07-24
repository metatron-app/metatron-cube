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

package io.druid.segment;

import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface ComplexColumnSelector<T> extends ObjectColumnSelector<T>
{
  Offset offset();

  default ObjectColumnSelector asSelector(Column column)
  {
    return ColumnSelectors.asSelector(column, offset());
  }

  interface Nested<T> extends ComplexColumnSelector<T>
  {
    Column resolve(String expression);

    default ObjectColumnSelector selector(String expression)
    {
      return asSelector(resolve(expression));
    }
  }

  interface StructColumnSelector extends ComplexColumnSelector.Nested<List>
  {
    List<String> getFieldNames();

    Column getField(String field);

    ValueDesc getType(String field);
  }

  interface MapColumnSelector extends ComplexColumnSelector.Nested<Map>
  {
    Column getKey();

    Column getValue();

    default ObjectColumnSelector keySelector()
    {
      return asSelector(getKey());
    }

    default ObjectColumnSelector valueSelector()
    {
      return asSelector(getValue());
    }

    default DimensionSelector keyDimensionSelector(
        ScanContext context,
        ExtractionFn extractionFn,
        Function<IndexedInts, IndexedInts> indexer
    )
    {
      Preconditions.checkArgument(getKey().hasDictionaryEncodedColumn());
      return DimensionSelector.asSelector(getKey().getDictionaryEncoded(), extractionFn, context, offset(), indexer);
    }
  }

  interface ArrayColumnSelector extends ComplexColumnSelector.Nested<List>
  {
    int numElements();

    ValueDesc getType(int ix);

    Column getElement(int ix);
  }
}