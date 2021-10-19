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

import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.RowSignature;

import java.io.IOException;
import java.io.OutputStream;

public class ArrayLinesWriter extends ArrayWriter
{
  ArrayLinesWriter(OutputStream outputStream, ObjectMapper jsonMapper, RowSignature signature) throws IOException
  {
    super(outputStream, jsonMapper, signature);
    jsonGenerator.setRootValueSeparator(new SerializedString("\n"));
  }

  @Override
  public void start() throws IOException
  {
  }

  @Override
  public void end() throws IOException
  {
    jsonGenerator.flush();
    outputStream.write(new byte[]{'\n', '\n'});
  }
}
