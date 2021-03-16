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

package io.druid.data.output;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.druid.common.guava.Sequence;
import io.druid.query.Query;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = OutputDecorator.Default.class)
})
public interface OutputDecorator<T>
{
  @SuppressWarnings("unchecked")
  default Sequence<T> prepare(Query query, Sequence sequence)
  {
    return sequence;
  }

  void start(JsonGenerator jgen, SerializerProvider provider) throws IOException;

  void serialize(JsonGenerator jgen, SerializerProvider provider, T object) throws IOException;

  void end(JsonGenerator jgen, SerializerProvider provider) throws IOException;

  class Default<T> implements OutputDecorator<T>
  {
    @Override
    public void start(JsonGenerator jgen, SerializerProvider provider) throws IOException
    {
      jgen.writeStartArray();
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider, T object) throws IOException
    {
      jgen.writeObject(object);
    }

    @Override
    public void end(JsonGenerator jgen, SerializerProvider provider) throws IOException
    {
      jgen.writeEndArray();
    }
  }
}
