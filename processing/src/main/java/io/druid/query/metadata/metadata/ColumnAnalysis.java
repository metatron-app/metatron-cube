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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import io.druid.common.BooleanFunction;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ColumnAnalysis
{
  private static final String ERROR_PREFIX = "error:";

  public static ColumnAnalysis error(String reason)
  {
    return new ColumnAnalysis("STRING", null, false, -1, null, -1, null, null, ERROR_PREFIX + reason);
  }

  private final String type;
  private final Map<String, String> descriptor;
  private final boolean hasMultipleValues;
  private final long serializedSize;
  private final long[] cardinality;
  private final int nullCount;
  private final Comparable minValue;
  private final Comparable maxValue;
  private final String errorMessage;

  private final boolean numeric;

  @JsonCreator
  public ColumnAnalysis(
      @JsonProperty("type") String type,
      @JsonProperty("descriptor") Map<String, String> descriptor,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("serializedSize") long serializedSize,
      @JsonProperty("cardinality") long[] cardinality,
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
      Boolean hasMultipleValues,
      long serializedSize,
      long[] cardinality,
      Comparable minValue,
      Comparable maxValue,
      String errorMessage
  )
  {
    this(type, null, hasMultipleValues, serializedSize, cardinality, -1, minValue, maxValue, errorMessage);
  }

  @VisibleForTesting
  public ColumnAnalysis(
      String type,
      boolean hasMultipleValues,
      long serializedSize,
      long cardinality,
      Comparable minValue,
      Comparable maxValue,
      String errorMessage
  )
  {
    this(type, null, hasMultipleValues, serializedSize, cardinality < 0 ? null : new long[]{cardinality, cardinality}, -1, minValue, maxValue, errorMessage);
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
  public Boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  public long getSerializedSize()
  {
    return serializedSize;
  }

  @JsonProperty
  public long[] getCardinality()
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
    return fold(rhs, false);
  }

  // schema : keeps lhs
  public ColumnAnalysis fold(ColumnAnalysis rhs, boolean schema)
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

    if (!schema && !Objects.equals(type, rhs.getType())) {
      return ColumnAnalysis.error("cannot_merge_diff_types");
    }
    Map<String, String> mergedDescriptor = null;
    if (descriptor == null) {
      mergedDescriptor = rhs.descriptor;
    } else if (rhs.descriptor == null || Objects.equals(descriptor, rhs.descriptor)) {
      mergedDescriptor = descriptor;
    } else if (!schema) {
      return ColumnAnalysis.error("cannot_merge_diff_descs");
    }

    return new ColumnAnalysis(
        type,
        mergedDescriptor,
        hasMultipleValues || rhs.hasMultipleValues,
        serializedSize < 0 || rhs.serializedSize < 0 ? -1 : serializedSize + rhs.serializedSize,
        mergeCardinality(cardinality, rhs.cardinality),
        nullCount < 0 || rhs.nullCount < 0 ? -1 : nullCount +  rhs.nullCount,
        choose(minValue, rhs.minValue, false),
        choose(maxValue, rhs.maxValue, true),
        null
    );
  }

  public static long[] mergeCardinality(long[] c1, long[] c2)
  {
    if (c1 != null && c2 != null) {
      return new long[]{Math.max(c1[0], c2[0]), c1[1] + c2[1]};
    }
    return null;
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
    int compare;
    if (obj1 instanceof Long || obj1 instanceof Integer) {
      compare = Long.compare(((Number) obj1).longValue(), ((Number) obj2).longValue());
    } else if (obj1 instanceof Float || obj1 instanceof Double) {
      compare = Double.compare(((Number) obj1).doubleValue(), ((Number) obj2).doubleValue());
    } else {
      compare = obj1.compareTo(obj2);
    }
    return (max ? compare : -compare) > 0 ? obj1 : obj2;
  }

  @JsonIgnore
  public ValueDesc toValueDesc(BooleanFunction<ValueDesc> converter)
  {
    ValueDesc desc = ValueDesc.of(type);
    if (converter != null && desc.isDimension()) {
      desc = converter.apply(hasMultipleValues);
    }
    return desc;
  }

  @Override
  public String toString()
  {
    return "ColumnAnalysis{" +
           "type='" + type + '\'' +
           (descriptor == null ? "" : ", descriptor=" + descriptor) +
           ", hasMultipleValues=" + hasMultipleValues +
           ", serializedSize=" + serializedSize +
           (cardinality == null ? "" : ", cardinality=" + Arrays.toString(cardinality)) +
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
    return Objects.equals(hasMultipleValues, that.hasMultipleValues) &&
           serializedSize == that.serializedSize &&
           Objects.equals(type, that.type) &&
           Objects.equals(descriptor, that.descriptor) &&
           Arrays.equals(cardinality, that.cardinality) &&
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
        Arrays.hashCode(cardinality),
        nullCount,
        minValue,
        maxValue,
        errorMessage
    );
  }
}
