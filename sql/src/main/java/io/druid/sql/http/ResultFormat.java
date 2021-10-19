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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.QueryDataSource;
import io.druid.query.RowSignature;
import io.druid.query.UnionDataSource;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = ResultFormat.ARRAY.class, name = "array"),
    @JsonSubTypes.Type(value = ResultFormat.ARRAYLINES.class, name = "arraylines"),
    @JsonSubTypes.Type(value = ResultFormat.OBJECT.class, name = "object"),
    @JsonSubTypes.Type(value = ResultFormat.OBJECTLINES.class, name = "objectlines"),
})
public interface ResultFormat
{
  class ARRAY implements ResultFormat
  {
    public static final ARRAY INSTANCE = new ARRAY();

    @Override
    public Writer createFormatter(
        OutputStream outputStream, com.fasterxml.jackson.databind.ObjectMapper jsonMapper, RowSignature signature
    ) throws IOException
    {
      return new ArrayWriter(outputStream, jsonMapper, signature);
    }
  }

  class ARRAYLINES implements ResultFormat
  {
    public static final ARRAYLINES INSTANCE = new ARRAYLINES();

    @Override
    public Writer createFormatter(
        OutputStream outputStream, com.fasterxml.jackson.databind.ObjectMapper jsonMapper, RowSignature signature
    ) throws IOException
    {
      return new ArrayLinesWriter(outputStream, jsonMapper, signature);
    }
  }

  class OBJECT implements ResultFormat
  {
    public static final OBJECT INSTANCE = new OBJECT();

    @Override
    public Writer createFormatter(
        OutputStream outputStream, com.fasterxml.jackson.databind.ObjectMapper jsonMapper, RowSignature signature
    ) throws IOException
    {
      return new ObjectWriter(outputStream, jsonMapper, signature);
    }
  }

  class OBJECTLINES implements ResultFormat
  {
    public static final OBJECTLINES INSTANCE = new OBJECTLINES();

    @Override
    public Writer createFormatter(
        OutputStream outputStream, com.fasterxml.jackson.databind.ObjectMapper jsonMapper, RowSignature signature
    ) throws IOException
    {
      return new ObjectLinesWriter(outputStream, jsonMapper, signature);
    }
  }

  default String contentType()
  {
    return MediaType.APPLICATION_JSON;
  }

  Writer createFormatter(OutputStream outputStream, ObjectMapper jsonMapper, RowSignature signature) throws IOException;

  interface Text extends ResultFormat
  {
    @Override
    default String contentType() {return MediaType.TEXT_PLAIN;}
  }

  interface Writer extends Closeable
  {
    /**
     * Start of the response, called once per writer.
     */
    default void start() throws IOException {}

    default void writeHeader() throws IOException {}

    /**
     * Field within a row.
     */
    void writeRow(Object[] row) throws IOException;

    /**
     * End of the response. Must allow the user to know that they have read all data successfully.
     */
    void end() throws IOException;

    default void close() throws IOException {}
  }
}
