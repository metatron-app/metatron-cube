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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.ParserInitializationFail;
import io.druid.data.ParsingFail;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Rows;
import io.druid.data.input.TimestampSpec;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.Charsets;
import org.joda.time.DateTime;

import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSVInputRowParser implements InputRowParser.Streaming<Object>
{
  private static final Logger LOG = new Logger(CSVInputRowParser.class);

  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final List<String> columns;
  private final Character delimiter;
  private final Character quoteCharacter;
  private final Character escapeCharacter;
  private final String recordSeparator;
  private final String nullString;
  private final Boolean skipHeaderRecord;
  private final Boolean ignoreSurroundingSpaces;
  private final Boolean ignoreHeaderCase;
  private final boolean checkConsistency;
  private final boolean ignoreInvalidRows;
  private final boolean trim;
  private final String charset;

  @JsonCreator
  public CSVInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("delimiter") Character delimiter,
      @JsonProperty("quoteCharacter") Character quoteCharacter,
      @JsonProperty("escapeCharacter") Character escapeCharacter,
      @JsonProperty("recordSeparator") String recordSeparator,
      @JsonProperty("nullString") String nullString,
      @JsonProperty("skipHeaderRecord") Boolean skipHeaderRecord,
      @JsonProperty("ignoreSurroundingSpaces") Boolean ignoreSurroundingSpaces,
      @JsonProperty("ignoreHeaderCase") Boolean ignoreHeaderCase,
      @JsonProperty("checkConsistency") boolean checkConsistency,
      @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      @JsonProperty("trim") boolean trim,
      @JsonProperty("charset") String charset
  ) {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.columns = columns == null ? Collections.<String>emptyList() : columns;
    this.delimiter = delimiter;
    this.quoteCharacter = quoteCharacter;
    this.escapeCharacter = escapeCharacter;
    this.recordSeparator = recordSeparator;
    this.nullString = nullString;
    this.skipHeaderRecord = skipHeaderRecord;
    this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    this.ignoreHeaderCase = ignoreHeaderCase;
    this.checkConsistency = checkConsistency;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.trim = trim;
    this.charset = charset;
  }

  @Override
  public InputRow parse(Object input)
  {
    throw new UnsupportedOperationException("parse");
  }

  @Override
  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @Override
  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new CSVInputRowParser(
        timestampSpec,
        DimensionsSpec.withExclusions(dimensionsSpec, exclusions),
        columns,
        delimiter,
        quoteCharacter,
        escapeCharacter,
        recordSeparator,
        nullString,
        skipHeaderRecord,
        ignoreSurroundingSpaces,
        ignoreHeaderCase,
        checkConsistency,
        ignoreInvalidRows,
        trim,
        charset
    );
  }

  @Override
  public Streaming<Object> withIgnoreInvalidRows(boolean ignoreInvalidRows)
  {
    return new CSVInputRowParser(
        timestampSpec,
        dimensionsSpec,
        columns,
        delimiter,
        quoteCharacter,
        escapeCharacter,
        recordSeparator,
        nullString,
        skipHeaderRecord,
        ignoreSurroundingSpaces,
        ignoreHeaderCase,
        checkConsistency,
        ignoreInvalidRows,
        trim,
        charset
    );
  }

  @Override
  public boolean accept(Object input)
  {
    return input instanceof InputStream || input instanceof Reader;
  }

  @Override
  public Iterator<InputRow> parseStream(final Object input)
  {
    if (input == null) {
      return Iterators.emptyIterator();
    }
    final CSVParser parser = makeParser(input);
    final List<String> dimensions = dimensionsSpec.getDimensionNames();
    final List<String> columns = asNames(parser.getHeaderMap());
    return Iterators.transform(parser.iterator(), new Function<CSVRecord, InputRow>()
    {
      private int columnInconsistency;
      private int timestampError;

      @Override
      public InputRow apply(final CSVRecord input)
      {
        if (checkConsistency && !input.isConsistent()) {
          if (!ignoreInvalidRows) {
            throw new ParsingFail(input.toMap(), "Inconsistent row in %d th line", input.getRecordNumber());
          }
          if (columnInconsistency++ == 0) {
            LOG.info(
                "Inconsistent row %s in %d th line.. similar cases will not be logged further",
                input.toMap(), input.getRecordNumber()
            );
          }
          return null;
        }
        final int max = Math.min(columns.size(), input.size());
        final Map<String, Object> event = Maps.newHashMapWithExpectedSize(max);
        for (int i = 0; i < max; i++) {
          event.put(columns.get(i), trimIfNeeded(input.get(i)));
        }
        Map<String, Object> merged = Rows.mergePartitions(event);
        try {
          DateTime dateTime = Preconditions.checkNotNull(timestampSpec.extractTimestamp(merged));
          return new MapBasedInputRow(dateTime, dimensions, merged);
        }
        catch (Exception e) {
          if (!ignoreInvalidRows) {
            throw ParsingFail.propagate(merged, e);
          }
          if (timestampError++ == 0) {
            LOG.info(
                "Exception extracting timestamp from row %s in %d th line.. similar cases will not be logged further",
                input.toMap(), input.getRecordNumber()
            );
          }
          return null;
        }
      }
    });
  }

  private String trimIfNeeded(String value)
  {
    return trim && value != null ? value.trim() : value;
  }

  private CSVParser makeParser(Object input)
  {
    try {
      if (input instanceof InputStream) {
        return CSVParser.parse((InputStream) input, Charsets.toCharset(charset), makeFormat());
      } else if (input instanceof Reader) {
        return CSVParser.parse((Reader) input, makeFormat());
      }
    }
    catch (Exception e) {
      throw new ParserInitializationFail(e);
    }
    throw new ParserInitializationFail(new IAE("not supported type ", input));
  }

  private List<String> asNames(Map<String, Integer> headerMap)
  {
    List<String> names = Lists.newArrayList();
    for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
      int index = entry.getValue();
      while (names.size() <= index) {
        names.add(null);
      }
      names.set(index, entry.getKey());
    }
    return names;
  }

  private CSVFormat makeFormat()
  {
    CSVFormat format = CSVFormat.DEFAULT.withSystemRecordSeparator();
    if (columns != null) {
      format = format.withHeader(columns.toArray(new String[0]));
    }
    if (delimiter != null) {
      format = format.withDelimiter(delimiter);
    }
    if (quoteCharacter != null) {
      format = format.withQuote(quoteCharacter);
    }
    if (escapeCharacter != null) {
      format = format.withEscape(escapeCharacter);
    }
    if (recordSeparator != null) {
      format = format.withRecordSeparator(recordSeparator);
    }
    if (nullString != null) {
      format = format.withNullString(nullString);
    }
    if (skipHeaderRecord != null) {
      format = format.withSkipHeaderRecord(skipHeaderRecord);
    }
    if (ignoreSurroundingSpaces != null) {
      format = format.withIgnoreSurroundingSpaces(ignoreSurroundingSpaces);
    }
    if (ignoreHeaderCase != null) {
      format = format.withIgnoreHeaderCase(ignoreHeaderCase);
    }
    return format;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Character getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Character getQuoteCharacter()
  {
    return quoteCharacter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Character getEscapeCharacter()
  {
    return escapeCharacter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getRecordSeparator()
  {
    return recordSeparator;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getNullString()
  {
    return nullString;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean getIgnoreSurroundingSpaces()
  {
    return ignoreSurroundingSpaces;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean getIgnoreHeaderCase()
  {
    return ignoreHeaderCase;
  }

  @JsonProperty
  public boolean isCheckConsistency()
  {
    return checkConsistency;
  }

  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  @JsonProperty
  public boolean isTrim()
  {
    return trim;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getCharset()
  {
    return charset;
  }
}
