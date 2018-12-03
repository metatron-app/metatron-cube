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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerdeFactory;

import java.nio.ByteBuffer;

/**
 */
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static ComplexColumnPartSerde createDeserializer(
      @JsonProperty("typeName") String complexType
  )
  {
    return new ComplexColumnPartSerde(complexType, null);
  }

  private final String typeName;
  private final ComplexMetricSerde serde;
  private final Serializer serializer;

  public ComplexColumnPartSerde(String typeName, Serializer serializer)
  {
    this.typeName = typeName;
    this.serde = ComplexMetrics.getSerdeForType(typeName);
    this.serializer = serializer;
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  @JsonIgnore
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(
          ByteBuffer buffer,
          ColumnBuilder builder,
          BitmapSerdeFactory serdeFactory
      )
      {
        if (serde != null) {
          serde.deserializeColumn(buffer, builder);
        }
        builder.setType(ValueDesc.of(typeName));
      }
    };
  }
}
