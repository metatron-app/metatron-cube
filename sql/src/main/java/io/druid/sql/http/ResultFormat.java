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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import org.apache.calcite.rel.type.RelDataType;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ResultFormat.DELEGATE.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = ResultFormat.ARRAY.class, name = "array"),
    @JsonSubTypes.Type(value = ResultFormat.ARRAYLINES.class, name = "arraylines"),
    @JsonSubTypes.Type(value = ResultFormat.OBJECT.class, name = "object"),
    @JsonSubTypes.Type(value = ResultFormat.OBJECTLINES.class, name = "objectlines"),
    @JsonSubTypes.Type(value = ResultFormat.NULL.class, name = "null"),
})
public interface ResultFormat
{
  class DELEGATE implements ResultFormat
  {
    private final ResultFormat type;

    @JsonCreator
    public DELEGATE(String type)
    {
      this.type = StringUtils.isNullOrEmpty(type) ? ARRAY.INSTANCE : fromString(type);
    }

    private static ResultFormat fromString(String type)
    {
      switch (type) {
        case "array":
          return ARRAY.INSTANCE;
        case "arraylines":
          return ARRAYLINES.INSTANCE;
        case "object":
          return OBJECT.INSTANCE;
        case "objectlines":
          return OBJECTLINES.INSTANCE;
        case "null":
          return NULL.INSTANCE;
      }
      throw new IAE("Not supported type [%s]", type);
    }

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType)
        throws IOException
    {
      return type.createFormatter(output, mapper, rowType);
    }

    @Override
    public String contentType()
    {
      return type.contentType();
    }
  }

  class ARRAY implements ResultFormat
  {
    public static final ARRAY INSTANCE = new ARRAY();

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException
    {
      return new ArrayWriter(output, mapper, rowType);
    }
  }

  class ARRAYLINES implements ResultFormat
  {
    public static final ARRAYLINES INSTANCE = new ARRAYLINES();

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException
    {
      return new ArrayLinesWriter(output, mapper, rowType);
    }
  }

  class OBJECT implements ResultFormat
  {
    public static final OBJECT INSTANCE = new OBJECT();

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException
    {
      return new ObjectWriter(output, mapper, rowType);
    }
  }

  class OBJECTLINES implements ResultFormat
  {
    public static final OBJECTLINES INSTANCE = new OBJECTLINES();

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException
    {
      return new ObjectLinesWriter(output, mapper, rowType);
    }
  }

  class NULL implements ResultFormat
  {
    public static final ResultFormat INSTANCE = new NULL();

    @Override
    public Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException
    {
      return Writer.NULL;
    }
  }

  default String contentType()
  {
    return MediaType.APPLICATION_JSON;
  }

  Writer createFormatter(OutputStream output, ObjectMapper mapper, RelDataType rowType) throws IOException;

  interface Text extends ResultFormat
  {
    @Override
    default String contentType() {return MediaType.TEXT_PLAIN;}
  }

  interface Writer extends Closeable
  {
    Writer NULL = new Writer() {};

    /**
     * Start of the response, called once per writer.
     */
    default void start() throws IOException {}

    default void writeHeader() throws IOException {}

    /**
     * Field within a row.
     */
    default void writeRow(Object[] row) throws IOException {}

    /**
     * End of the response. Must allow the user to know that they have read all data successfully.
     */
    default void end() throws IOException {}

    default void close() throws IOException {}
  }
}
