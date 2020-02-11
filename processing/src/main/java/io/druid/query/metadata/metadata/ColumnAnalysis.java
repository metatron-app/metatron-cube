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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.ValueType;

import java.util.Map;
import java.util.Objects;

/**
 */
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static ColumnAnalysis error(String reason)
  {
    return new ColumnAnalysis("STRING", null, false, -1, -1, -1, null, null, ERROR_PREFIX + reason);
  }

  private final String type;
  private final Map<String, String> descriptor;
  private final boolean hasMultipleValues;
  private final long serializedSize;
  private final int cardinality;
  private final int nullCount;
  private final Comparable minValue;
  private final Comparable maxValue;
  private final String errorMessage;

  private boolean numeric;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("type") String type,
      @JsonProperty("descriptor") Map<String, String> descriptor,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("serializedSize") long serializedSize,
      @JsonProperty("cardinality") int cardinality,
      @JsonProperty("nullCount") int nullCount,
      @JsonProperty("minValue") Comparable minValue,
      @JsonProperty("maxValue") Comparable maxValue,
      @JsonProperty("errorMessage") String errorMessage
  )
  {
    this.type = type;
    this.descriptor = descriptor;
    this.hasMultipleValues = hasMultipleValues;
    this.serializedSize = serializedSize;
    this.cardinality = cardinality;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.nullCount = nullCount;
    this.errorMessage = errorMessage;
    this.numeric = ValueType.of(type).isNumeric();
  }

  public ColumnAnalysis(
      String type,
      boolean hasMultipleValues,
      long serializedSize,
      int cardinality,
      Comparable minValue,
      Comparable maxValue,
      String errorMessage
  )
  {
    this(type, null, hasMultipleValues, serializedSize, cardinality, -1, minValue, maxValue, errorMessage);
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getDescriptor()
  {
    return descriptor;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  public long getSerializedSize()
  {
    return serializedSize;
  }

  @JsonProperty
  public int getCardinality()
  {
    return cardinality;
  }

  @JsonProperty
  public int getNullCount()
  {
    return nullCount;
  }

  /**
   * from https://github.com/easysoa/EasySOA-Incubation/blob/master/easysoa-registry-v1/easysoa-registry-rest-core/src/main/java/org/easysoa/registry/rest/SoaNodeInformation.java
   *
   * Its @JsonTypeInfo(use = Id.NAME, include = As.WRAPPER_OBJECT) lets contained
   * objects be written as tercely as possible (ex. of int : 1, to compare with
   * ex. of long : {Long:1}). Alternatively, Id.MINIMAL_CLASS is powerful but far
   * less pretty (shows full Java class names), and As.PROPERTY is not as terce
   * (additional "property=" for ALL objects including ex. int).
   * Its @JsonSubTypes is required, else error ex. :
   * Could not resolve type id 'Long' into a subtype of [simple type, class java.io.Serializable]
   * @return
   */
  @JsonProperty
  @JsonSubTypes({
      @JsonSubTypes.Type(String.class), @JsonSubTypes.Type(Integer.class), @JsonSubTypes.Type(Long.class),
      @JsonSubTypes.Type(Float.class), @JsonSubTypes.Type(Double.class)
  })
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Comparable getMinValue()
  {
    return minValue;
  }

  @JsonProperty
  @JsonSubTypes({
      @JsonSubTypes.Type(String.class), @JsonSubTypes.Type(Integer.class), @JsonSubTypes.Type(Long.class),
      @JsonSubTypes.Type(Float.class), @JsonSubTypes.Type(Double.class)
  })
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Comparable getMaxValue()
  {
    return maxValue;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMessage()
  {
    return errorMessage;
  }

  public boolean isError()
  {
    return (errorMessage != null && !errorMessage.isEmpty());
  }

  public ColumnAnalysis fold(ColumnAnalysis rhs)
  {
    if (rhs == null) {
      return this;
    }

    if (isError() && rhs.isError()) {
      return errorMessage.equals(rhs.getErrorMessage()) ? this : ColumnAnalysis.error("multiple_errors");
    } else if (isError()) {
      return this;
    } else if (rhs.isError()) {
      return rhs;
    }

    if (!Objects.equals(type, rhs.getType())) {
      return ColumnAnalysis.error("cannot_merge_diff_types");
    }
    if (!Objects.equals(descriptor, rhs.descriptor)) {
      return ColumnAnalysis.error("cannot_merge_diff_descs");
    }

    return new ColumnAnalysis(
        type,
        descriptor,
        hasMultipleValues || rhs.isHasMultipleValues(),
        serializedSize < 0 || rhs.serializedSize < 0 ? -1 : serializedSize + rhs.serializedSize,
        cardinality < 0 || rhs.cardinality < 0 ? -1 : Math.max(cardinality, rhs.cardinality),
        nullCount < 0 || rhs.nullCount < 0 ? -1 : nullCount +  rhs.nullCount,
        choose(minValue, rhs.minValue, false),
        choose(maxValue, rhs.maxValue, true),
        null
    );
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable> T choose(T obj1, T obj2, boolean max)
  {
    if (numeric && (obj1 == null || obj2 == null)) {
      // null means N/A for numeric types..
      return null;
    }
    if (obj1 == null) {
      return max ? obj2 : null;
    }
    if (obj2 == null) {
      return max ? obj1 : null;
    }
    int compare = max ? obj1.compareTo(obj2) : obj2.compareTo(obj1);
    return compare > 0 ? obj1 : obj2;
  }

  @Override
  public String toString()
  {
    return "ColumnAnalysis{" +
           "type='" + type + '\'' +
           (descriptor == null ? "" : ", descriptor=" + descriptor) +
           ", hasMultipleValues=" + hasMultipleValues +
           ", serializedSize=" + serializedSize +
           ", cardinality=" + cardinality +
           ", minValue=" + minValue +
           ", maxValue=" + maxValue +
           ", nullCount=" + nullCount +
           ", errorMessage='" + errorMessage + '\'' +
           '}';
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
    ColumnAnalysis that = (ColumnAnalysis) o;
    return hasMultipleValues == that.hasMultipleValues &&
           serializedSize == that.serializedSize &&
           Objects.equals(type, that.type) &&
           Objects.equals(descriptor, that.descriptor) &&
           Objects.equals(cardinality, that.cardinality) &&
           Objects.equals(nullCount, that.nullCount) &&
           Objects.equals(minValue, that.minValue) &&
           Objects.equals(maxValue, that.maxValue) &&
           Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        type,
        descriptor,
        hasMultipleValues,
        serializedSize,
        cardinality,
        nullCount,
        minValue,
        maxValue,
        errorMessage
    );
  }
}
