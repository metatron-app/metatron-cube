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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.IntTagged;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.MaterializedDictionary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class EnumColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static EnumColumnPartSerde create(
      @JsonProperty("type") ValueDesc type,
      @JsonProperty("descriptor") ColumnDescriptor descriptor
  )
  {
    return new EnumColumnPartSerde(type, descriptor);
  }

  private final ValueDesc type;
  private final ColumnDescriptor descriptor;

  public EnumColumnPartSerde(ValueDesc type, ColumnDescriptor descriptor)
  {
    this.type = type;
    this.descriptor = descriptor;
  }

  @JsonProperty
  public ValueDesc getType()
  {
    return type;
  }

  @JsonProperty
  public ColumnDescriptor getDescriptor()
  {
    return descriptor;
  }

  @Override
  public Serializer getSerializer()
  {
    return descriptor.asSerializer();
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
      {
        List<IntTagged<String>> elements = TypeUtils.parseEnum(type);
        int[] mapping = new int[elements.size()];
        String[] values = new String[elements.size()];
        for (int i = 0; i < elements.size(); i++) {
          mapping[i] = elements.get(i).tag;
          values[i] = elements.get(i).value;
        }
        builder.setDictionary(Dictionary.asProvider(MaterializedDictionary.sorted(values)));
        descriptor.read(builder, buffer, serdeFactory);
      }
    };
  }
}
