/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.data.input;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ExcelParser
{
  private static final Logger log = new Logger(ExcelParser.class);

  public static interface ParsedIterator extends CloseableIterator<Row>
  {
    Sheet currentSheet();

    String[] columnNames();

    float progress();
  }

  public static int[] toIndices(Workbook workbook, String[] sheetNames)
  {
    List<Integer> indices = Lists.newArrayList();
    if (sheetNames == null || sheetNames.length == 0) {
      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        indices.add(i);
      }
    } else {
      for (String sheetName : sheetNames) {
        int index = workbook.getSheetIndex(sheetName);
        if (index >= 0) {
          indices.add(index);
        }
      }
    }
    return Ints.toArray(indices);
  }

  public static ParsedIterator parse(Workbook workbook, boolean extractColumns, int[] sheetIndices)
      throws IOException
  {
    ParsedIterator parsed = toRowIterator(workbook, Ints.asList(sheetIndices));
    if (extractColumns) {
      parsed = wrapWithColumnExtractor(parsed);
    }
    return parsed;
  }

  private static ParsedIterator toRowIterator(final Workbook workbook, final Iterable<Integer> sheetIndices)
  {
    int total = 0;
    try {
      for (Integer index : sheetIndices) {
        Sheet sheet = workbook.getSheetAt(index);
        total += sheet.getLastRowNum() - sheet.getFirstRowNum();
      }
    }
    catch (UnsupportedOperationException e) {
      // ok, it's streaming
    }

    final int totalRow = total;
    final Iterator<Integer> indices = sheetIndices.iterator();
    return new ParsedIterator()
    {
      private int rowNum;
      private Sheet current;
      private Iterator<Row> currentRows = Collections.emptyIterator();

      @Override
      public Sheet currentSheet()
      {
        return current;
      }

      @Override
      public String[] columnNames()
      {
        return null;
      }

      @Override
      public float progress()
      {
        return totalRow == 0 ? 1f : rowNum / (float) totalRow;
      }

      @Override
      public boolean hasNext()
      {
        while (!currentRows.hasNext() && indices.hasNext()) {
          current = workbook.getSheetAt(indices.next());
          currentRows = current.rowIterator();
          log.info("reading from sheet '%s'", current.getSheetName());
        }
        return currentRows.hasNext();
      }

      @Override
      public Row next()
      {
        rowNum++;
        return currentRows.next();
      }

      @Override
      public void close() throws IOException
      {
        workbook.close();
      }
    };
  }

  private static ParsedIterator wrapWithColumnExtractor(final ParsedIterator iterator)
  {
    final PeekingIterator<Row> peek = Iterators.peekingIterator(iterator);
    return new ParsedIterator()
    {
      private Sheet current;
      private String[] columns;

      @Override
      public Sheet currentSheet()
      {
        return current;
      }

      @Override
      public String[] columnNames()
      {
        return columns;
      }

      @Override
      public float progress()
      {
        return iterator.progress();
      }

      @Override
      public boolean hasNext()
      {
        if (!peek.hasNext()) {
          return false;
        }
        Row next = peek.peek();
        if (current == null || current != iterator.currentSheet()) {
          current = iterator.currentSheet();
          columns = new String[next.getLastCellNum()];
          for (Cell cell : peek.next()) {
            Preconditions.checkArgument(cell.getCellType() == Cell.CELL_TYPE_STRING);
            columns[cell.getColumnIndex()] = cell.getStringCellValue();
          }
          log.info("Extracted column names %s from sheet '%s'", Arrays.toString(columns), current.getSheetName());
          return peek.hasNext();
        }
        return true;
      }

      @Override
      public Row next()
      {
        return peek.next();
      }

      @Override
      public void close() throws IOException
      {
        iterator.close();
      }
    };
  }

  public static Map<String, Object> convert(
      final Row row,
      final String sheetNameColumn,
      final Supplier<String[]> columnNamesSupplier
  )
  {
    Map<String, Object> parsed = Maps.newLinkedHashMap();

    if (sheetNameColumn != null) {
      parsed.put(sheetNameColumn, row.getSheet().getSheetName());
    }
    final String[] columnNames = columnNamesSupplier.get();
    if (columnNames == null) {
      throw new IllegalArgumentException(
          "cannot find column names.. use excel.extract.column.names=true or set it in 'columnNames' in parser spec"
      );
    }
    for (Cell cell : row) {
      final String columnName = columnNames[cell.getColumnIndex()];
      switch (cell.getCellType()) {
        case Cell.CELL_TYPE_BOOLEAN:
          parsed.put(columnName, cell.getBooleanCellValue());
          continue;
        case Cell.CELL_TYPE_NUMERIC:
          parsed.put(columnName, cell.getNumericCellValue());
          continue;
        case Cell.CELL_TYPE_BLANK:
        case Cell.CELL_TYPE_STRING:
          parsed.put(columnName, cell.getStringCellValue());
          continue;
        default:
          // log?
      }
    }
    return parsed;
  }
}
