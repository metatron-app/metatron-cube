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
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerdeFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 *
 */
public class NullColumnPartSerde implements ColumnPartSerde
{
  private final ValueDesc type;

  @JsonCreator
  public NullColumnPartSerde(@JsonProperty("type") ValueDesc type)
  {
    this.type = type;
  }

  @JsonProperty
  public ValueDesc getType()
  {
    return type;
  }

  @Override
  public Serializer getSerializer()
  {
    return new Serializer()
    {
      @Override
      public long getSerializedSize()
      {
        return 0;
      }

      @Override
      public long writeToChannel(WritableByteChannel channel) throws IOException
      {
        return 0;
      }
    };
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
      {
        builder.setType(type);
      }
    };
  }
}
