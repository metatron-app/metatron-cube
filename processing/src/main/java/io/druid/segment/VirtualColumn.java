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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.common.Cacheable;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ExprVirtualColumn.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "map", value = MapVirtualColumn.class),
    @JsonSubTypes.Type(name = "expr", value = ExprVirtualColumn.class),
    @JsonSubTypes.Type(name = "index", value = KeyIndexedVirtualColumn.class),
    @JsonSubTypes.Type(name = "lateral", value = LateralViewVirtualColumn.class),
    @JsonSubTypes.Type(name = "array", value = ArrayVirtualColumn.class),
    @JsonSubTypes.Type(name = "struct", value = StructVirtualColumn.class),
    @JsonSubTypes.Type(name = "dateTime", value = DateTimeVirtualColumn.class),
    @JsonSubTypes.Type(name = "dimensionSpec", value = DimensionSpecVirtualColumn.class),
    @JsonSubTypes.Type(name = "$attachment", value = AttachmentVirtualColumn.class)
})
public interface VirtualColumn extends Cacheable
{
  String getOutputName();

  default ValueDesc resolveType(TypeResolver resolver)
  {
    return resolveType(getOutputName(), resolver);
  }

  ValueDesc resolveType(String column, TypeResolver resolver);

  ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory);

  default FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asFloat(asMetric(dimension, factory));
  }

  default DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asDouble(asMetric(dimension, factory));
  }

  default LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asLong(asMetric(dimension, factory));
  }

  default DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    return VirtualColumns.toDimensionSelector(asMetric(dimension, factory), extractionFn);
  }

  VirtualColumn duplicate();
}
