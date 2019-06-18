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
import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class StringGenericColumnPartSerde implements ColumnPartSerde
{
  private final Serializer serializer;

  @JsonCreator
  public StringGenericColumnPartSerde()
  {
    this(null);
  }

  public StringGenericColumnPartSerde(final ComplexColumnSerializer delegate)
  {
    this.serializer = delegate;
  }

  @Override
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
      ) throws IOException
      {
        final GenericIndexed<String> indexed = GenericIndexed.read(buffer, ObjectStrategy.STRING_STRATEGY);
        builder.setType(ValueDesc.STRING)
               .setHasMultipleValues(false)
               .setGenericColumn(new StringColumnPartSupplier(indexed));
      }
    };
  }
}
