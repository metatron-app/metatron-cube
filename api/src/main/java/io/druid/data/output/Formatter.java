/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 */
public interface Formatter extends Closeable
{
  String NEW_LINE = System.lineSeparator();

  void write(Map<String, Object> datum) throws IOException;

  class XSVFormatter implements Formatter
  {
    private final String separator;
    private final String nullValue;
    private final String[] columns;
    private final boolean header;

    private final OutputStream output;
    private final ObjectMapper mapper;

    private final StringBuilder builder = new StringBuilder();
    private boolean firstLine;

    public XSVFormatter(
        OutputStream output,
        ObjectMapper mapper,
        String separator,
        String nullValue,
        String[] columns,
        boolean header
    )
    {
      this.separator = separator == null ? "," : separator;
      this.nullValue = nullValue == null ? "NULL" : nullValue;
      this.columns = columns;
      this.output = output;
      this.mapper = mapper;
      this.header = header;
      firstLine = true;
    }

    @Override
    public void write(Map<String, Object> datum) throws IOException
    {
      if (firstLine && header) {
        writeHeader(columns == null ? datum.keySet() : Arrays.asList(columns));
      }
      builder.setLength(0);

      if (columns == null) {
        boolean first = true;
        for (Object value : datum.values()) {
          appendObject(value, first);
          first = false;
        }
      } else {
        boolean first = true;
        for (String dimension : columns) {
          appendObject(datum.get(dimension), first);
          first = false;
        }
      }
      if (builder.length() > 0) {
        builder.append(NEW_LINE);
        output.write(builder.toString().getBytes());
        firstLine = false;
      }
    }

    private void appendObject(Object value, boolean first) throws JsonProcessingException
    {
      if (!first) {
        builder.append(separator);
      }
      if (value == null) {
        builder.append(nullValue);
      } else if (value instanceof String) {
        builder.append((String)value);
      } else if (value instanceof Number) {
        builder.append(String.valueOf(value));
      } else {
        final String str = mapper.writeValueAsString(value);
        builder.append(str.substring(1, str.length() - 1));   // strip quotation
      }
    }

    private void writeHeader(Collection<String> dimensions) throws IOException
    {
      for (String dimension : dimensions) {
        if (builder.length() > 0) {
          builder.append(separator);
        }
        builder.append(dimension);
      }
      builder.append(NEW_LINE);
      output.write(builder.toString().getBytes());
    }

    @Override
    public void close() throws IOException
    {
      output.close();
    }
  }

  class JsonFormatter implements Formatter
  {
    private static final byte[] HEAD = "[".getBytes();
    private static final byte[] NEW_LINE = System.lineSeparator().getBytes();
    private static final byte[] NEXT_LINE = (", " + System.lineSeparator()).getBytes();
    private static final byte[] TAIL = ("]" + System.lineSeparator()).getBytes();

    private final ObjectMapper jsonMapper;
    private final String[] columns;
    private final boolean withWrapping;

    private final OutputStream output;
    private boolean firstLine;

    public JsonFormatter(OutputStream output, ObjectMapper jsonMapper, String[] columns, boolean withWrapping)
        throws IOException
    {
      this.jsonMapper = jsonMapper;
      this.columns = columns;
      this.withWrapping = withWrapping;
      this.output = output;
      if (withWrapping) {
        output.write(HEAD);
      }
      firstLine = true;
    }

    @Override
    public void write(Map<String, Object> datum) throws IOException
    {
      if (!firstLine) {
        output.write(withWrapping ? NEXT_LINE : NEW_LINE);
      }
      // jsonMapper.writeValue(output, datum) closes stream
      if (columns != null && columns.length != 0) {
        Map<String, Object> retained = Maps.newLinkedHashMap();
        for (String column : columns) {
          retained.put(column, datum.get(column));
        }
        datum = retained;
      }
      output.write(jsonMapper.writeValueAsBytes(datum));
      firstLine = false;
    }

    @Override
    public void close() throws IOException
    {
      try (OutputStream finishing = output) {
        if (!firstLine) {
          finishing.write(NEW_LINE);
        }
        if (withWrapping) {
          finishing.write(TAIL);
        }
      }
    }
  }
}
