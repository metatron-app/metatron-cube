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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.CountingOutputStream;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Formatters implements ForwardConstants
{
  public static String getFormat(Map<String, Object> context)
  {
    String format = Objects.toString(context.get(FORMAT), JSON_FORMAT);
    return Preconditions.checkNotNull(format, "format is null").toLowerCase();
  }

  public static boolean isIndexFormat(Map<String, Object> context)
  {
    return INDEX_FORMAT.equalsIgnoreCase(getFormat(context));
  }

  public static CountingAccumulator toBasicExporter(
      Map<String, Object> context,
      ObjectMapper jsonMapper,
      ByteSink output
  ) throws IOException
  {
    if (EXCEL_FORMAT.equalsIgnoreCase(getFormat(context))) {
      return toExcelExporter(output, context);
    }
    final Formatter formatter = toBasicFormatter(output, context, jsonMapper);
    if (formatter != null) {
      return wrapToExporter(formatter);
    }
    return null;
  }

  private static final int DEFAULT_FLUSH_INTERVAL = 4096;
  private static final int DEFAULT_MAX_ROWS_PER_SHEET = 0;  // MAX

  public static CountingAccumulator toExcelExporter(final ByteSink sink, final Map<String, Object> context)
      throws IOException
  {
    final String[] columns = parseStrings(context.get(COLUMNS));
    final String[] mappedColumns = parseStrings(context.get(MAPPED_COLUMNS), columns);
    final int flushInterval = parseInt(context.get(FLUSH_INTERVAL), DEFAULT_FLUSH_INTERVAL);
    final int maxRowsPerSheet = parseInt(context.get(MAX_ROWS_PER_SHEET), DEFAULT_MAX_ROWS_PER_SHEET);
    final boolean header = parseBoolean(context.get(WITH_HEADER), true);    // backward compatible -_-;

    if (columns != null) {
      return new ExcelAccumulator(sink, flushInterval, maxRowsPerSheet)
      {
        @Override
        public void nextSheet()
        {
          super.nextSheet();
          if (header) {
            Row r = nextRow(true);
            for (int i = 0; i < mappedColumns.length; i++) {
              Cell c = r.createCell(i);
              c.setCellValue(mappedColumns[i]);
            }
          }
        }

        @Override
        public Void accumulate(Void accumulated, Map<String, Object> in)
        {
          Row r = nextRow(false);
          for (int i = 0; i < columns.length; i++) {
            Object o = in.get(columns[i]);
            if (o == null) {
              continue;
            }
            addToCell(r.createCell(i), o);
          }
          flushIfNeeded();
          return null;
        }
      };
    }
    return new ExcelAccumulator(sink, flushInterval, maxRowsPerSheet)
    {
      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        Row r = nextRow(false);
        int i = 0;
        for (Object o : in.values()) {
          addToCell(r.createCell(i++), o);
        }
        flushIfNeeded();
        return null;
      }
    };
  }

  private static abstract class ExcelAccumulator implements CountingAccumulator
  {
    private static final int MAX_ROW_INDEX = SpreadsheetVersion.EXCEL2007.getLastRowIndex();

    private final ByteSink sink;
    private final int flushInterval;
    private final int maxRowsPerSheet;
    private final SXSSFWorkbook wb = new SXSSFWorkbook(-1);

    private SXSSFSheet sheet;
    private int rowNumInSheet;
    private int rowNum;

    protected ExcelAccumulator(ByteSink sink, int flushInterval, int maxRowsPerSheet) throws IOException
    {
      this.sink = sink;
      this.flushInterval = flushInterval;
      this.maxRowsPerSheet = maxRowsPerSheet > 0 ? Math.min(maxRowsPerSheet, MAX_ROW_INDEX) : MAX_ROW_INDEX;
    }

    protected Row nextRow(boolean headerLine)
    {
      if (sheet == null || rowNumInSheet >= maxRowsPerSheet) {
        nextSheet();
      }
      Row r = sheet.createRow(rowNumInSheet++);
      if (!headerLine) {
        rowNum++;
      }
      return r;
    }

    protected void addToCell(Cell c, Object o)
    {
      if (o instanceof Number) {
        c.setCellValue(((Number) o).doubleValue());
      } else if (o instanceof String) {
        c.setCellValue((String) o);
      } else if (o instanceof Date) {
        c.setCellValue((Date) o);
      } else if (o instanceof Boolean) {
        c.setCellValue((Boolean) o);
      } else {
        c.setCellValue(String.valueOf(o));
      }
    }

    protected void flush()
    {
      try {
        sheet.flushRows();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    protected void flushIfNeeded()
    {
      if (rowNumInSheet % flushInterval == 0) {
        flush();
      }
    }

    protected void nextSheet()
    {
      sheet = wb.createSheet();
      rowNumInSheet = 0;
    }

    public int count()
    {
      return rowNum;
    }

    @Override
    public CountingAccumulator init()
    {
      return this;
    }

    @Override
    public Map<String, Object> close() throws IOException
    {
      if (rowNum == 0) {
        nextSheet();    // make empty sheet.. or poi makes invalid excel file
      }
      try (CountingOutputStream output = new CountingOutputStream(sink.openBufferedStream())) {
        try {
          wb.write(output);
        }
        finally {
          wb.dispose();
          wb.close();
        }
        return ImmutableMap.<String, Object>of(
            "rowCount", count(),
            "data", ImmutableMap.of(sink.toString(), output.getCount())
        );
      }
    }
  }

  public static CountingAccumulator wrapToExporter(final Formatter formatter)
  {
    Preconditions.checkNotNull(formatter, "Invalid formatter");
    return new CountingAccumulator()
    {
      @Override
      public CountingAccumulator init() throws IOException
      {
        return this;
      }

      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        try {
          formatter.write(in);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }

      @Override
      public Map<String, Object> close() throws IOException
      {
        return formatter.close();
      }
    };
  }

  private static Formatter toBasicFormatter(ByteSink output, Map<String, Object> context, ObjectMapper jsonMapper)
      throws IOException
  {
    String[] columns = parseStrings(context.get(COLUMNS));
    String[] mappedColumns = parseStrings(context.get(MAPPED_COLUMNS), columns);
    String format = Formatters.getFormat(context);
    if (format.equalsIgnoreCase(JSON_FORMAT)) {
      boolean wrapAsList = parseBoolean(context.get(WRAP_AS_LIST), false);
      return new Formatter.JsonFormatter(output, jsonMapper, columns, wrapAsList);
    }

    String separator;
    if (format.equalsIgnoreCase(CSV_FORMAT)) {
      separator = ",";
    } else if (format.equalsIgnoreCase(TSV_FORMAT)) {
      separator = "\t";
    } else {
      return null;
    }
    boolean header = parseBoolean(context.get(WITH_HEADER), false);
    boolean quote = parseBoolean(context.get(WITH_QUOTE), false);
    String nullValue = Objects.toString(context.get(NULL_VALUE), null);
    String charset = Objects.toString(context.get(CHARSET), null);

    return new Formatter.XSVFormatter(
        output,
        jsonMapper,
        separator,
        nullValue,
        columns,
        mappedColumns,
        header,
        quote,
        charset
    );
  }

  private static boolean isNullOrEmpty(String string)
  {
    return string == null || string.isEmpty();
  }

  private static boolean parseBoolean(Object input, boolean defaultValue)
  {
    return input == null ? defaultValue :
           input instanceof Boolean ? (Boolean) input : Boolean.valueOf(String.valueOf(input));
  }

  private static int parseInt(Object input, int defaultValue)
  {
    return input == null ? defaultValue :
           input instanceof Number ? ((Number) input).intValue() : Integer.valueOf(String.valueOf(input));
  }

  public static String[] parseStrings(Object input)
  {
    return parseStrings(input, null);
  }

  public static String[] parseStrings(Object input, String[] defaultValue)
  {
    if (input instanceof List) {
      List<String> stringList = Lists.transform((List<?>) input, Functions.toStringFunction());
      return stringList.toArray(new String[0]);
    }
    String stringVal = Objects.toString(input, null);
    if (isNullOrEmpty(stringVal)) {
      return defaultValue;
    }
    return Iterables.toArray(
        Iterables.transform(
            Arrays.asList(stringVal.split(",")), new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return input.trim();
              }
            }
        ),
        String.class
    );
  }
}
