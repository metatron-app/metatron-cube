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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.RowSignature;

import java.io.IOException;
import java.io.OutputStream;

public class ObjectWriter implements ResultFormat.Writer
{
  final JsonGenerator jsonGenerator;
  final OutputStream outputStream;
  final RowSignature signature;

  ObjectWriter(OutputStream outputStream, ObjectMapper jsonMapper, RowSignature signature) throws IOException
  {
    this.jsonGenerator = jsonMapper.getFactory().createGenerator(outputStream);
    this.outputStream = outputStream;
    this.signature = signature;
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
    for (String columnName : signature.getColumnNames()) {
      jsonGenerator.writeNullField(columnName);
    }
    jsonGenerator.writeEndObject();
  }

  @Override
  public void writeRow(final Object[] value) throws IOException
  {
    jsonGenerator.writeStartObject();
    for (int i = 0; i < value.length; i++) {
      jsonGenerator.writeObjectField(signature.columnName(i), value[i]);
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
