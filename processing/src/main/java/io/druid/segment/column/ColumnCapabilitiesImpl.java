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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;
import io.druid.data.ValueType;

import java.util.Objects;

/**
 */
public class ColumnCapabilitiesImpl implements ColumnCapabilities
{
  public static ColumnCapabilitiesImpl of(ValueType type)
  {
    return new ColumnCapabilitiesImpl().setType(type);
  }

  public static ColumnCapabilitiesImpl copyOf(ColumnCapabilities capabilities)
  {
    ColumnCapabilitiesImpl copy = new ColumnCapabilitiesImpl();
    copy.setType(capabilities.getType());
    copy.setDictionaryEncoded(capabilities.isDictionaryEncoded());
    copy.setRunLengthEncoded(capabilities.isRunLengthEncoded());
    copy.setHasBitmapIndexes(capabilities.hasBitmapIndexes());
    copy.setHasSpatialIndexes(capabilities.hasSpatialIndexes());
    copy.setHasMultipleValues(capabilities.hasMultipleValues());
    copy.setHasMetricBitmap(capabilities.hasMetricBitmap());
    copy.setHasLuceneIndex(capabilities.hasLuceneIndex());
    return copy;
  }

  private ValueType type = null;
  private String typeName = null;
  private boolean dictionaryEncoded = false;
  private boolean runLengthEncoded = false;
  private boolean hasInvertedIndexes = false;
  private boolean hasSpatialIndexes = false;
  private boolean hasMultipleValues = false;
  private boolean hasMetricBitmap = false;
  private boolean hasBitSlicedBitmap = false;
  private boolean hasLuceneIndex = false;

  @Override
  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  public ColumnCapabilitiesImpl setType(ValueType type)
  {
    this.type = type;
    return this;
  }

  public ColumnCapabilitiesImpl setTypeName(String typeName)
  {
    this.typeName = typeName;
    return this;
  }

  @Override
  @JsonProperty
  public boolean isDictionaryEncoded()
  {
    return dictionaryEncoded;
  }

  public ColumnCapabilitiesImpl setDictionaryEncoded(boolean dictionaryEncoded)
  {
    this.dictionaryEncoded = dictionaryEncoded;
    return this;
  }

  @Override
  @JsonProperty
  public boolean isRunLengthEncoded()
  {
    return runLengthEncoded;
  }

  public ColumnCapabilitiesImpl setRunLengthEncoded(boolean runLengthEncoded)
  {
    this.runLengthEncoded = runLengthEncoded;
    return this;
  }

  @Override
  @JsonProperty("hasBitmapIndexes")
  public boolean hasBitmapIndexes()
  {
    return hasInvertedIndexes;
  }

  public ColumnCapabilitiesImpl setHasBitmapIndexes(boolean hasInvertedIndexes)
  {
    this.hasInvertedIndexes = hasInvertedIndexes;
    return this;
  }

  @Override
  @JsonProperty("hasSpatialIndexes")
  public boolean hasSpatialIndexes()
  {
    return hasSpatialIndexes;
  }

  public ColumnCapabilitiesImpl setHasSpatialIndexes(boolean hasSpatialIndexes)
  {
    this.hasSpatialIndexes = hasSpatialIndexes;
    return this;
  }

  @Override
  @JsonProperty("hasMultipleValues")
  public boolean hasMultipleValues()
  {
    return hasMultipleValues;
  }

  public ColumnCapabilitiesImpl setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  @Override
  @JsonProperty("hasMetricBitmap")
  public boolean hasMetricBitmap()
  {
    return hasMetricBitmap;
  }

  public ColumnCapabilitiesImpl setHasMetricBitmap(boolean hasMetricBitmap)
  {
    this.hasMetricBitmap = hasMetricBitmap;
    return this;
  }

  @Override
  @JsonProperty("hasMetricBitmap")
  public boolean hasBitSlicedBitmap()
  {
    return hasBitSlicedBitmap;
  }

  public ColumnCapabilitiesImpl setHasBitSlicedBitmap(boolean hasBitSlicedBitmap)
  {
    this.hasBitSlicedBitmap = hasBitSlicedBitmap;
    return this;
  }

  @Override
  @JsonProperty("hasLuceneIndex")
  public boolean hasLuceneIndex()
  {
    return hasLuceneIndex;
  }

  public ColumnCapabilitiesImpl setHasLuceneIndex(boolean hasLuceneIndex)
  {
    this.hasLuceneIndex = hasLuceneIndex;
    return this;
  }

  @Override
  public ColumnCapabilitiesImpl merge(ColumnCapabilities other)
  {
    if (other == null) {
      return this;
    }

    if (type == null) {
      type = other.getType();
    }

    if (!type.equals(other.getType())) {
      throw new ISE("Cannot merge columns of type[%s] and [%s]", type, other.getType());
    }

    if (typeName == null) {
      typeName = other.getTypeName();
    }

    if (!Objects.equals(typeName, other.getTypeName())) {
      throw new ISE("Cannot merge columns of typeName[%s] and [%s]", typeName, other.getTypeName());
    }

    this.dictionaryEncoded |= other.isDictionaryEncoded();
    this.runLengthEncoded |= other.isRunLengthEncoded();
    this.hasInvertedIndexes |= other.hasBitmapIndexes();
    this.hasSpatialIndexes |= other.hasSpatialIndexes();
    this.hasMultipleValues |= other.hasMultipleValues();
    this.hasMetricBitmap &= other.hasMetricBitmap();
    this.hasLuceneIndex &= other.hasLuceneIndex();

    return this;
  }
}
