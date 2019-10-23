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

package io.druid.data.output;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSink;
import com.google.common.io.CountingOutputStream;
import io.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "xsv", value = Formatter.XSVFormatter.class),
    @JsonSubTypes.Type(name = "json", value = Formatter.JsonFormatter.class),
})
public interface Formatter
{
  void write(Map<String, Object> datum) throws IOException;

  Map<String, Object> close() throws IOException;

  class XSVFormatter implements Formatter
  {
    private static final String NEW_LINE = System.lineSeparator();
    private static final char LF = '\n';
    private static final char CR = '\r';

    private static final char CSV_QUOTE = '"';
    private static final String CSV_QUOTE_STR = String.valueOf(CSV_QUOTE);
    private static final String CSV_ESCAPED_QUOTE_STR = CSV_QUOTE_STR + CSV_QUOTE_STR;
    private static final Pattern PATTERN = Pattern.compile(CSV_QUOTE_STR, Pattern.LITERAL);

    private final String separator;
    private final String nullValue;
    private final String[] columns;
    private final String[] mappedColumns;
    private final boolean header;
    private final String charset;

    private final ByteSink sink;
    private final ObjectMapper mapper;
    private final CountingOutputStream output;

    private final Matcher matcher;

    private final StringBuilder builder = new StringBuilder();
    private boolean firstLine;
    private int counter;

    public XSVFormatter(ByteSink sink, ObjectMapper mapper, String separator) throws IOException
    {
      this(sink, mapper, separator, null, null, null, false, null);
    }

    public XSVFormatter(
        ByteSink sink,
        ObjectMapper mapper,
        String separator,
        String nullValue,
        String[] columns,
        String[] mappedColumns,
        boolean header,
        String charset
    )
        throws IOException
    {
      this.separator = separator == null ? "," : separator;
      this.nullValue = nullValue == null ? "NULL" : nullValue;
      this.columns = columns;
      this.mappedColumns = mappedColumns == null ? columns : mappedColumns;
      this.sink = sink;
      this.mapper = mapper;
      this.output = new CountingOutputStream(sink.openBufferedStream());
      this.header = header;
      this.charset = charset == null ? StringUtils.UTF8_STRING : charset;
      this.matcher = PATTERN.matcher("");
      firstLine = true;
    }

    @Override
    public void write(Map<String, Object> datum) throws IOException
    {
      if (firstLine && header) {
        Collection<String> header = mappedColumns == null ? datum.keySet() : Arrays.asList(mappedColumns);
        writeHeader(header);
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
        output.write(builder.toString().getBytes(charset));
        firstLine = false;
      }
      counter++;
    }

    @Override
    public Map<String, Object> close() throws IOException
    {
      output.close();
      return ImmutableMap.<String, Object>of(
          "rowCount", counter,
          "data", ImmutableMap.of(sink.toString(), output.getCount())
      );
    }

    private void appendObject(Object value, boolean first) throws JsonProcessingException
    {
      if (!first) {
        builder.append(separator);
      }
      if (value == null) {
        builder.append(nullValue);
      } else if (value instanceof String) {
        String string = (String) value;
        final boolean quote;
        if (string.indexOf(CSV_QUOTE) < 0) {
          quote = needQuote(string);
        } else {
          quote = true;
          string = matcher.reset(string).replaceAll(CSV_ESCAPED_QUOTE_STR);
        }
        if (quote) {
          builder.append(CSV_QUOTE);
        }
        builder.append(string);
        if (quote) {
          builder.append(CSV_QUOTE);
        }
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
        final boolean quote = needQuote(dimension);
        if (quote) {
          builder.append(CSV_QUOTE);
        }
        builder.append(dimension);
        if (quote) {
          builder.append(CSV_QUOTE);
        }
      }
      builder.append(NEW_LINE);
      output.write(builder.toString().getBytes(charset));
    }

    private boolean needQuote(String string)
    {
      return string.contains(separator) || string.indexOf(CR) >= 0 || string.indexOf(LF) >= 0;
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

    private final ByteSink sink;
    private final CountingOutputStream output;

    private boolean firstLine;
    private int counter;

    public JsonFormatter(ByteSink sink, ObjectMapper jsonMapper, String[] columns, boolean withWrapping)
        throws IOException
    {
      this.jsonMapper = jsonMapper;
      this.columns = columns;
      this.withWrapping = withWrapping;
      this.sink = sink;
      this.output = new CountingOutputStream(sink.openBufferedStream());
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
      counter++;
    }

    @Override
    public Map<String, Object> close() throws IOException
    {
      try (OutputStream finishing = output) {
        if (!firstLine) {
          finishing.write(NEW_LINE);
        }
        if (withWrapping) {
          finishing.write(TAIL);
        }
      }
      return ImmutableMap.<String, Object>of(
          "rowCount", counter,
          "data", ImmutableMap.of(sink.toString(), output.getCount())
      );
    }
  }
}
