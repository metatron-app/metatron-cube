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
import com.google.common.collect.Maps;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.data.IndexedInts;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class MapVirtualColumn implements VirtualColumn
{
  private static final String MAP_KEY = "__key";
  private static final String MAP_VALUE = "__value";

  private static final byte VC_TYPE_ID = 0x00;

  private final String outputName;
  private final String keyDimension;
  private final String valueDimension;
  private final String valueMetric;

  @JsonCreator
  public MapVirtualColumn(
      @JsonProperty("keyDimension") String keyDimension,
      @JsonProperty("valueDimension") String valueDimension,
      @JsonProperty("valueMetric") String valueMetric,
      @JsonProperty("outputName") String outputName
  )
  {
    Preconditions.checkArgument(keyDimension != null, "key dimension should not be null");
    Preconditions.checkArgument(
        valueDimension == null ^ valueMetric == null,
        "Must have a valid, non-null valueDimension or valueMetric"
    );
    Preconditions.checkArgument(outputName != null, "output name should not be null");

    this.keyDimension = keyDimension;
    this.valueDimension = valueDimension;
    this.valueMetric = valueMetric;
    this.outputName = outputName;
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    final int index = column.indexOf('.', outputName.length());
    if (index < 0) {
      ValueDesc keyType = types.resolve(keyDimension);
      ValueDesc valueType = types.resolve(valueDimension != null ? valueDimension : valueMetric);
      return ValueDesc.ofMap(keyType, valueType);
    }
    if (valueDimension != null) {
      return ValueDesc.STRING;
    }
    return types.resolve(valueMetric).subElement(ValueDesc.UNKNOWN);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(column.startsWith(outputName));
    final int index = column.indexOf('.', outputName.length());
    final DimensionSelector keySelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    if (valueDimension != null) {
      final DimensionSelector valueSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of(valueDimension));

      if (index < 0) {
        return new ObjectColumnSelector<Map>()
        {
          @Override
          public ValueDesc type()
          {
            return ValueDesc.MAP;
          }

          @Override
          public Map get()
          {
            final IndexedInts keyIndices = keySelector.getRow();
            final IndexedInts valueIndices = valueSelector.getRow();
            if (keyIndices == null || valueIndices == null) {
              return null;
            }
            final int limit = Math.min(keyIndices.size(), valueIndices.size());
            final Map<String, Object> map = Maps.newHashMapWithExpectedSize(limit);
            for (int i = 0; i < limit; i++) {
              map.put(
                  Objects.toString(keySelector.lookupName(keyIndices.get(i)), null),
                  valueSelector.lookupName(valueIndices.get(i))
              );
            }
            return map;
          }
        };
      }

      final int keyId = keySelector.lookupId(column.substring(index + 1));
      if (keyId < 0) {
        return ColumnSelectors.nullObjectSelector(ValueDesc.STRING);
      }

      return new ObjectColumnSelector()
      {
        @Override
        public ValueDesc type()
        {
          return ValueDesc.assertPrimitive(valueSelector.type());
        }

        @Override
        public Object get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final IndexedInts valueIndices = valueSelector.getRow();
          if (keyIndices == null || valueIndices == null) {
            return null;
          }
          final int limit = Math.min(keyIndices.size(), valueIndices.size());
          for (int i = 0; i < limit; i++) {
            if (keyIndices.get(i) == keyId) {
              return valueSelector.lookupName(valueIndices.get(i));
            }
          }
          return null;
        }
      };
    }

    @SuppressWarnings("unchecked")
    final ObjectColumnSelector<List> valueSelector = factory.makeObjectColumnSelector(valueMetric);
    if (valueSelector == null) {
      return ColumnSelectors.nullObjectSelector(index < 0 ? ValueDesc.MAP : ValueDesc.UNKNOWN);
    }
    if (index < 0) {
      return new ObjectColumnSelector<Map>()
      {
        @Override
        public ValueDesc type()
        {
          return ValueDesc.MAP;
        }

        @Override
        public Map get()
        {
          final IndexedInts keyIndices = keySelector.getRow();
          final List values = valueSelector.get();
          if (keyIndices == null || values == null) {
            return null;
          }
          final int limit = Math.min(keyIndices.size(), values.size());
          final Map<String, Object> map = Maps.newLinkedHashMap();
          for (int i = 0; i < limit; i++) {
            map.put(Objects.toString(keySelector.lookupName(keyIndices.get(i)), null), values.get(i));
          }
          return map;
        }
      };
    }

    ValueDesc type = valueSelector.type();
    Preconditions.checkArgument(type.hasSubElement(), "cannot resolve element type");
    final ValueDesc elementType = type.subElement();
    final int keyId = keySelector.lookupId(column.substring(index + 1));
    if (keyId < 0) {
      return ColumnSelectors.nullObjectSelector(elementType);
    }

    return new ObjectColumnSelector<Object>()
    {
      @Override
      public ValueDesc type()
      {
        return elementType;
      }

      @Override
      public Object get()
      {
        final IndexedInts keyIndices = keySelector.getRow();
        final List values = valueSelector.get();
        if (keyIndices == null || values == null) {
          return null;
        }
        final int limit = Math.min(keyIndices.size(), values.size());
        for (int i = 0; i < limit; i++) {
          if (keyIndices.get(i) == keyId) {
            return values.get(i);
          }
        }
        return null;
      }
    };
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asFloatMetric");
    }
    return ColumnSelectors.asFloat(selector);
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asDoubleMetric");
    }
    return ColumnSelectors.asDouble(selector);
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);
    if (ValueDesc.isMap(selector.type())) {
      throw new UnsupportedOperationException("asLongMetric");
    }
    return ColumnSelectors.asLong(selector);
  }

  @Override
  public DimensionSelector asDimension(String dimension, ExtractionFn extractionFn, ColumnSelectorFactory factory)
  {
    Preconditions.checkArgument(dimension.startsWith(outputName));
    final int index = dimension.indexOf('.', outputName.length());
    if (index < 0) {
      return asKeyIndexed().asDimension(dimension, extractionFn, factory);
    }
    String target = dimension.substring(index + 1);
    if (MAP_KEY.equals(target)) {
      return factory.makeDimensionSelector(DimensionSpecs.of(keyDimension, extractionFn));
    }
    if (MAP_VALUE.equals(target)) {
      return factory.makeDimensionSelector(DimensionSpecs.of(valueDimension, extractionFn));
    }
    ObjectColumnSelector selector = asMetric(dimension, factory);
    if (!ValueDesc.isDimension(selector.type()) && !ValueDesc.isPrimitive(selector.type())) {
      throw new UnsupportedOperationException(dimension + " cannot be used as dimension");
    }
    return VirtualColumns.toDimensionSelector(selector, extractionFn);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new MapVirtualColumn(keyDimension, valueDimension, valueMetric, outputName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(keyDimension, valueDimension, valueMetric, outputName);
  }

  public KeyIndexedVirtualColumn asKeyIndexed()
  {
    return new KeyIndexedVirtualColumn(
        keyDimension,
        valueDimension == null ? null : Arrays.asList(valueDimension),
        valueMetric == null ? null : Arrays.asList(valueMetric),
        null,
        outputName
    );
  }

  @JsonProperty
  public String getKeyDimension()
  {
    return keyDimension;
  }

  @JsonProperty
  public String getValueDimension()
  {
    return valueDimension;
  }

  @JsonProperty
  public String getValueMetric()
  {
    return valueMetric;
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
    if (!(o instanceof MapVirtualColumn)) {
      return false;
    }

    MapVirtualColumn that = (MapVirtualColumn) o;

    if (!keyDimension.equals(that.keyDimension)) {
      return false;
    }
    if (!Objects.equals(valueDimension, that.valueDimension)) {
      return false;
    }
    if (!Objects.equals(valueMetric, that.valueMetric)) {
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
    int result = keyDimension.hashCode();
    result = 31 * result + Objects.hashCode(valueDimension);
    result = 31 * result + Objects.hashCode(valueMetric);
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "MapVirtualColumn{" +
           "keyDimension='" + keyDimension + '\'' +
           ", valueDimension='" + valueDimension + '\'' +
           ", valueMetric='" + valueMetric + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
