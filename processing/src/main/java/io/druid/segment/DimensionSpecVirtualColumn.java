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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;

import java.util.Objects;

/**
 */
public class DimensionSpecVirtualColumn implements VirtualColumn
{
  public static VirtualColumn wrap(DimensionSpec dimensionSpec, String outputName)
  {
    return new DimensionSpecVirtualColumn(dimensionSpec, outputName);
  }

  private static final byte VC_TYPE_ID = 0x07;

  private final String outputName;
  private final DimensionSpec dimensionSpec;

  @JsonCreator
  public DimensionSpecVirtualColumn(
      @JsonProperty("dimensionSpec") DimensionSpec dimensionSpec,
      @JsonProperty("outputName") String outputName
  )
  {
    this.dimensionSpec = Preconditions.checkNotNull(dimensionSpec, "dimensionSpec should not be null");
    this.outputName = outputName == null ? dimensionSpec.getOutputName() : outputName;
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    return ColumnSelectors.asMultiValued(asDimension(DefaultDimensionSpec.of(dimension), factory));
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("asFloatMetric");
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("asDoubleMetric");
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("asLongMetric");
  }

  @Override
  public DimensionSelector asDimension(DimensionSpec dimension, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(outputName.equals(dimension.getDimension()));
    DimensionSelector selector = factory.makeDimensionSelector(dimensionSpec);
    ExtractionFn extractionFn = dimension.getExtractionFn();
    if (extractionFn == null) {
      return selector;
    }
    return new DimensionSelector.Delegated(selector)
    {
      @Override
      public Object lookupName(int id)
      {
        return extractionFn.apply(delegate.lookupName(id));
      }

      @Override
      public ValueDesc type()
      {
        return ValueDesc.STRING;
      }

      @Override
      public int lookupId(Object name)
      {
        throw new UnsupportedOperationException("lookupId");
      }
    };
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.equals(outputName));
    ValueDesc valueDesc = dimensionSpec.resolve(Suppliers.ofInstance(types));
    if (valueDesc.isDimension()) {
      return ValueDesc.ofMultiValued(valueDesc.subElement());
    }
    if (!valueDesc.isMultiValued()) {
      return ValueDesc.ofMultiValued(valueDesc);
    }
    return valueDesc;
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new DimensionSpecVirtualColumn(dimensionSpec, outputName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(dimensionSpec).sp()
                  .append(outputName);
  }

  @JsonProperty
  public DimensionSpec getDimensionSpec()
  {
    return dimensionSpec;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DimensionSpecVirtualColumn)) {
      return false;
    }

    DimensionSpecVirtualColumn that = (DimensionSpecVirtualColumn) o;

    if (!dimensionSpec.equals(that.dimensionSpec)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensionSpec, outputName);
  }

  @Override
  public String toString()
  {
    return "DimensionSpecVirtualColumn{" +
           "dimensionSpec='" + dimensionSpec + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
