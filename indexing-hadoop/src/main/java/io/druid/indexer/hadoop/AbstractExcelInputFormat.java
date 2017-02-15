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

package io.druid.indexer.hadoop;

import com.metamx.common.logger.Logger;
import com.monitorjbl.xlsx.StreamingReader;
import io.druid.data.input.ExcelParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;

/**
 */
public abstract class AbstractExcelInputFormat<T> extends FileInputFormat<NullWritable, T>
{
  protected final Logger log = new Logger(getClass());

  public static final String SHEET_NAMES = "excel.sheet.names";
  public static final String SHEET_INDICES = "excel.sheet.indices";
  public static final String EXTRACT_COLUMN_NAMES = "excel.extract.column.names";

  @Override
  protected boolean isSplitable(JobContext context, Path filename)
  {
    return false;
  }

  @Override
  public RecordReader<NullWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    return toRecordReader(createParser((FileSplit) split, context), context);
  }

  protected abstract RecordReader<NullWritable, T> toRecordReader(
      ExcelParser.ParsedIterator iterator,
      TaskAttemptContext context
  );

  protected ExcelParser.ParsedIterator createParser(FileSplit split, TaskAttemptContext context) throws IOException
  {
    Configuration configuration = context.getConfiguration();
    log.info("%s = %s", SHEET_NAMES, configuration.get(SHEET_NAMES));
    log.info("%s = %s", SHEET_INDICES, configuration.get(SHEET_INDICES));
    log.info("%s = %s", EXTRACT_COLUMN_NAMES, configuration.get(EXTRACT_COLUMN_NAMES));

    final Workbook workbook = createWorkBook(split, configuration);
    return toParsedIterator(workbook, configuration);
  }

  private Workbook createWorkBook(FileSplit input, Configuration configuration) throws IOException
  {
    FileSystem fs = input.getPath().getFileSystem(configuration);
    FSDataInputStream inputStream = fs.open(input.getPath());
//    return new XSSFWorkbook(inputStream);

    return StreamingReader.builder()
        .rowCacheSize(2048)    // number of rows to keep in memory (defaults to 10)
        .bufferSize(65536)     // buffer size to use when reading InputStream to file (defaults to 1024)
        .open(inputStream);
  }

  private ExcelParser.ParsedIterator toParsedIterator(Workbook workbook, Configuration configuration)
      throws IOException
  {
    String[] sheetNames = configuration.getStrings(SHEET_NAMES, (String[])null);
    int[] sheetIndices = configuration.getInts(SHEET_INDICES);
    if (sheetNames == null || sheetIndices.length == 0) {
      sheetIndices = ExcelParser.toIndices(workbook, sheetNames);
    }
    boolean extractColumns = configuration.getBoolean(EXTRACT_COLUMN_NAMES, false);
    return ExcelParser.parse(workbook, extractColumns, sheetIndices);
  }
}