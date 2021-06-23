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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.ISE;
import io.druid.data.ValueType;

import java.util.Objects;

/**
 */
public class ColumnCapabilities
{
  public static ColumnCapabilities of(ValueType type)
  {
    return new ColumnCapabilities().setType(type);
  }

  public static ColumnCapabilities copyOf(ColumnCapabilities capabilities)
  {
    ColumnCapabilities copy = new ColumnCapabilities();
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

  private ValueType type;
  private String typeName;
  private boolean dictionaryEncoded;
  private boolean runLengthEncoded;
  private boolean hasBitmapIndexes;
  private boolean hasSpatialIndexes;
  private boolean hasMultipleValues;
  private boolean hasMetricBitmap;
  private boolean hasBitSlicedBitmap;
  private boolean hasLuceneIndex;
  private boolean hasDictionaryFST;

  public ColumnCapabilities() {}

  @JsonCreator
  public ColumnCapabilities(
      @JsonProperty("type") ValueType type,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("dictionaryEncoded") boolean dictionaryEncoded,
      @JsonProperty("runLengthEncoded") boolean runLengthEncoded,
      @JsonProperty("hasBitmapIndexes") boolean hasBitmapIndexes,
      @JsonProperty("hasSpatialIndexes") boolean hasSpatialIndexes,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("hasMetricBitmap") boolean hasMetricBitmap,
      @JsonProperty("hasLuceneIndex") boolean hasLuceneIndex,
      @JsonProperty("hasDictionaryFST") boolean hasDictionaryFST
  )
  {
    this.type = type;
    this.typeName = typeName;
    this.dictionaryEncoded = dictionaryEncoded;
    this.runLengthEncoded = runLengthEncoded;
    this.hasBitmapIndexes = hasBitmapIndexes;
    this.hasSpatialIndexes = hasSpatialIndexes;
    this.hasMultipleValues = hasMultipleValues;
    this.hasMetricBitmap = hasMetricBitmap;
    this.hasLuceneIndex = hasLuceneIndex;
    this.hasDictionaryFST = hasDictionaryFST;
  }

  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  public ColumnCapabilities setType(ValueType type)
  {
    this.type = type;
    return this;
  }

  public ColumnCapabilities setTypeName(String typeName)
  {
    this.typeName = typeName;
    return this;
  }

  @JsonProperty
  public boolean isDictionaryEncoded()
  {
    return dictionaryEncoded;
  }

  public ColumnCapabilities setDictionaryEncoded(boolean dictionaryEncoded)
  {
    this.dictionaryEncoded = dictionaryEncoded;
    return this;
  }

  @JsonProperty
  public boolean isRunLengthEncoded()
  {
    return runLengthEncoded;
  }

  public ColumnCapabilities setRunLengthEncoded(boolean runLengthEncoded)
  {
    this.runLengthEncoded = runLengthEncoded;
    return this;
  }

  @JsonProperty
  public boolean hasBitmapIndexes()
  {
    return hasBitmapIndexes;
  }

  public ColumnCapabilities setHasBitmapIndexes(boolean hasBitmapIndexes)
  {
    this.hasBitmapIndexes = hasBitmapIndexes;
    return this;
  }

  @JsonProperty
  public boolean hasSpatialIndexes()
  {
    return hasSpatialIndexes;
  }

  public ColumnCapabilities setHasSpatialIndexes(boolean hasSpatialIndexes)
  {
    this.hasSpatialIndexes = hasSpatialIndexes;
    return this;
  }

  @JsonProperty
  public boolean hasMultipleValues()
  {
    return hasMultipleValues;
  }

  public ColumnCapabilities setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  @JsonProperty
  public boolean hasMetricBitmap()
  {
    return hasMetricBitmap;
  }

  public ColumnCapabilities setHasMetricBitmap(boolean hasMetricBitmap)
  {
    this.hasMetricBitmap = hasMetricBitmap;
    return this;
  }

  @JsonProperty
  public boolean hasBitSlicedBitmap()
  {
    return hasBitSlicedBitmap;
  }

  public ColumnCapabilities setHasBitSlicedBitmap(boolean hasBitSlicedBitmap)
  {
    this.hasBitSlicedBitmap = hasBitSlicedBitmap;
    return this;
  }

  @JsonProperty
  public boolean hasLuceneIndex()
  {
    return hasLuceneIndex;
  }

  public ColumnCapabilities setHasLuceneIndex(boolean hasLuceneIndex)
  {
    this.hasLuceneIndex = hasLuceneIndex;
    return this;
  }

  @JsonProperty
  public boolean hasDictionaryFST()
  {
    return hasDictionaryFST;
  }

  public ColumnCapabilities setHasDictionaryFST(boolean hasDictionaryFST)
  {
    this.hasDictionaryFST = hasDictionaryFST;
    return this;
  }

  public ColumnCapabilities merge(ColumnCapabilities other)
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
    this.hasBitmapIndexes |= other.hasBitmapIndexes();
    this.hasSpatialIndexes |= other.hasSpatialIndexes();
    this.hasMultipleValues |= other.hasMultipleValues();
    this.hasMetricBitmap &= other.hasMetricBitmap();
    this.hasLuceneIndex &= other.hasLuceneIndex();
    this.hasDictionaryFST &= other.hasDictionaryFST();

    return this;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        type,
        typeName,
        dictionaryEncoded,
        runLengthEncoded,
        hasBitmapIndexes,
        hasSpatialIndexes,
        hasMultipleValues,
        hasMetricBitmap,
        hasBitSlicedBitmap,
        hasLuceneIndex,
        hasDictionaryFST
    );
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
    ColumnCapabilities that = (ColumnCapabilities) o;
    return dictionaryEncoded == that.dictionaryEncoded &&
           runLengthEncoded == that.runLengthEncoded &&
           hasBitmapIndexes == that.hasBitmapIndexes &&
           hasSpatialIndexes == that.hasSpatialIndexes &&
           hasMultipleValues == that.hasMultipleValues &&
           hasMetricBitmap == that.hasMetricBitmap &&
           hasBitSlicedBitmap == that.hasBitSlicedBitmap &&
           hasLuceneIndex == that.hasLuceneIndex &&
           hasDictionaryFST == that.hasDictionaryFST &&
           type == that.type &&
           Objects.equals(typeName, that.typeName);
  }
}
