/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.ISE;

import java.util.Arrays;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringDimensionSchema.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = ValueDesc.STRING_TYPE, value = StringDimensionSchema.class),
    @JsonSubTypes.Type(name = ValueDesc.LONG_TYPE, value = LongDimensionSchema.class),
    @JsonSubTypes.Type(name = ValueDesc.FLOAT_TYPE, value = FloatDimensionSchema.class),
    @JsonSubTypes.Type(name = ValueDesc.DOUBLE_TYPE, value = DoubleDimensionSchema.class),
    @JsonSubTypes.Type(name = DimensionSchema.SPATIAL_TYPE_NAME, value = NewSpatialDimensionSchema.class),
})
public abstract class DimensionSchema
{
  public static DimensionSchema of(String name, ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new FloatDimensionSchema(name);
      case LONG:
        return new LongDimensionSchema(name);
      case DOUBLE:
        return new DoubleDimensionSchema(name);
      case STRING:
        return new StringDimensionSchema(name);
    }
    throw new ISE("not supported type %s [%s]", type, name);
  }

  public static DimensionSchema of(String name, String fieldName, ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new FloatDimensionSchema(name, fieldName, null);
      case LONG:
        return new LongDimensionSchema(name, fieldName, null);
      case DOUBLE:
        return new DoubleDimensionSchema(name, fieldName, null);
      case STRING:
        return new StringDimensionSchema(name, fieldName, null);
    }
    throw new ISE("not supported type %s [%s]", type, name);
  }

  public static final String SPATIAL_TYPE_NAME = "spatial";

  // main druid and druid-api should really use the same ValueType enum.
  // merge them when druid-api is merged back into the main repo

  public static enum MultiValueHandling
  {
    ARRAY,
    SORTED_ARRAY,
    SET {
      @Override
      public int[] rewrite(int[] indices)
      {
        int pos = 1;
        int prev = indices[0];
        for (int i = 1; i < indices.length; i++) {
          if (prev != indices[i]) {
            indices[pos++] = prev = indices[i];
          }
        }
        return pos == indices.length ? indices : Arrays.copyOf(indices, pos);
      }
    };

    @Override
    @JsonValue
    public String toString()
    {
      return this.name().toUpperCase();
    }

    @JsonCreator
    public static MultiValueHandling fromString(String name)
    {
      return valueOf(name.toUpperCase());
    }

    // indices.length > 1
    public int[] rewrite(final int[] indices)
    {
      return indices;
    }
  }

  private final String name;
  private final String fieldName;
  private final MultiValueHandling multiValueHandling;

  protected DimensionSchema(String name, String fieldName, MultiValueHandling multiValueHandling)
  {
    this.name = Preconditions.checkNotNull(name == null ? fieldName : name, "Dimension name cannot be null.");
    this.fieldName = fieldName;
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ARRAY : multiValueHandling;
    Preconditions.checkArgument(!name.contains(","), "Column name should not contain comma");
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public MultiValueHandling getMultiValueHandling()
  {
    return multiValueHandling;
  }

  @JsonIgnore
  public abstract String getTypeName();

  @JsonIgnore
  public abstract ValueType getValueType();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionSchema that = (DimensionSchema) o;

    return name.equals(that.name) && multiValueHandling == that.multiValueHandling;

  }

  @Override
  public int hashCode()
  {
    return name.hashCode() * 31 + multiValueHandling.ordinal();
  }

  @Override
  public String toString()
  {
    return "DimensionSchema{" +
           "type='" + getTypeName() + '\'' +
           ", name='" + name + '\'' +
           ", multiValueHandling=" + multiValueHandling +
           '}';
  }
}
