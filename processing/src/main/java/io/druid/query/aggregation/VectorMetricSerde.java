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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Floats;
import io.druid.data.ValueDesc;
import io.druid.data.input.BytesOutputStream;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

/**
 *
 */
public class VectorMetricSerde implements ComplexMetricSerde
{
  private final int dimension;
  private final ValueDesc type;

  @JsonCreator
  public VectorMetricSerde(
      @JsonProperty("dimension") int dimension
  )
  {
    this.dimension = dimension;
    this.type = ValueDesc.of(String.format("%s(%d)", ValueDesc.VECTOR_TYPE, dimension), float[].class);
  }

  @JsonProperty
  public int getDimension()
  {
    return dimension;
  }

  @Override
  @JsonProperty
  public ValueDesc getType()
  {
    return type;
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy()
    {
      private final BytesOutputStream scratch = new BytesOutputStream(dimension * Floats.BYTES);

      @Override
      public Class getClazz()
      {
        return float[].class;
      }

      @Override
      public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
          vector[i] = buffer.getFloat();
        }
        return vector;
      }

      @Override
      public byte[] toBytes(Object val)
      {
        float[] vector = (float[]) val;
        for (int i = 0; i < vector.length; i++) {
          scratch.writeFloat(vector[i]);
        }
        byte[] bytes = scratch.toByteArray();
        scratch.clear();
        return bytes;
      }
    };
  }

  @Override
  public String toString()
  {
    return "VectorMetricSerde{" + dimension + "}";
  }

  public static class Factory implements ComplexMetricSerde.Factory
  {
    @Override
    public ComplexMetricSerde create(String[] elements)
    {
      if (elements == null || elements.length <= 1) {
        return new VectorMetricSerde(-1);
      }
      return new VectorMetricSerde(Integer.valueOf(elements[1]));
    }
  }
}
