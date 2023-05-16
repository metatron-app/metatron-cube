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

package io.druid.sql.http;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.google.common.collect.Lists;
import io.druid.sql.calcite.planner.Calcites;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ObjectWriter implements ResultFormat.Writer
{
  final JsonGenerator jsonGenerator;
  final OutputStream outputStream;
  final SerializerProvider provider;
  final List<SerializedString> fieldNames;
  final List<JsonSerializer> fieldValues;

  ObjectWriter(OutputStream outputStream, ObjectMapper jsonMapper, RelDataType dataType) throws IOException
  {
    this.jsonGenerator = jsonMapper.getFactory().createGenerator(outputStream);
    this.outputStream = outputStream;
    ObjectMapper codec = (ObjectMapper) jsonGenerator.getCodec();
    DefaultSerializerProvider shared = ((DefaultSerializerProvider) codec.getSerializerProvider()).createInstance(
        codec.getSerializationConfig(), codec.getSerializerFactory()
    );
    this.provider = shared;
    this.fieldNames = Lists.newArrayList();
    this.fieldValues = Lists.newArrayList();
    for (RelDataTypeField field : dataType.getFieldList()) {
      fieldNames.add(new SerializedString(field.getName()));
      fieldValues.add(shared.findTypedValueSerializer(Calcites.toCoercedType(field.getType()).asClass(), false, null));
    }
  }

  @Override
  public void start() throws IOException
  {
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeHeader() throws IOException
  {
    jsonGenerator.writeStartObject();
    for (SerializableString columnName : fieldNames) {
      jsonGenerator.writeFieldName(columnName);
      jsonGenerator.writeNull();
    }
    jsonGenerator.writeEndObject();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void writeRow(final Object[] value) throws IOException
  {
    jsonGenerator.writeStartObject();
    for (int i = 0; i < value.length; i++) {
      jsonGenerator.writeFieldName(fieldNames.get(i));
      fieldValues.get(i).serialize(value[i], jsonGenerator, provider);
    }
    jsonGenerator.writeEndObject();
  }

  @Override
  public void end() throws IOException
  {
    jsonGenerator.writeEndArray();
    jsonGenerator.flush();
    outputStream.write('\n');
  }

  @Override
  public void close() throws IOException
  {
    jsonGenerator.close();
  }
}
