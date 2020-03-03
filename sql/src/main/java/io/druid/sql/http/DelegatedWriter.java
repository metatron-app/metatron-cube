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
import io.druid.data.output.OutputDecorator;

import java.io.IOException;

public class DelegatedWriter implements ResultFormat.Writer
{
  private final JsonGenerator jsonGenerator;
  private final OutputDecorator<Object[]> decorator;

  public DelegatedWriter(JsonGenerator jsonGenerator, OutputDecorator<Object[]> decorator)
  {
    this.jsonGenerator = jsonGenerator;
    this.decorator = decorator;
  }

  @Override
  public void start() throws IOException
  {
    decorator.start(jsonGenerator, null);
  }

  @Override
  public void writeHeader(String[] columnNames) throws IOException
  {
    // not in format
  }

  @Override
  public void writeRow(String[] columnNames, Object[] row) throws IOException
  {
    decorator.serialize(jsonGenerator, null, row);
  }

  @Override
  public void end() throws IOException
  {
    decorator.end(jsonGenerator, null);
  }

  @Override
  public void close() throws IOException
  {
    jsonGenerator.close();
  }
}
