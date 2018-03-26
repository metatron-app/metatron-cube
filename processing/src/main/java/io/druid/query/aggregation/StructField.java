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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 */
public class StructField
{
  public static String toTypeName(List<StructField> elements)
  {
    StringBuilder builder = new StringBuilder();
    builder.append("struct<");
    for (int i = 0; i < elements.size(); i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(elements.get(i).fieldType);
    }
    return builder.append('>').toString();
  }

  private final String fieldName;
  private final AggregatorFactory fieldType;

  public StructField(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldType") AggregatorFactory fieldType
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName);
    this.fieldType = Preconditions.checkNotNull(fieldType);
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public AggregatorFactory getFieldType()
  {
    return fieldType;
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

    StructField that = (StructField) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!fieldType.equals(that.fieldType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName.hashCode();
    result = 31 * result + fieldType.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "StructField{" +
           "fieldName='" + fieldName + '\'' +
           ", fieldType=" + fieldType +
           '}';
  }
}
