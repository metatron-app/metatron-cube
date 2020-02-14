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

package io.druid.indexer.hadoop;

import io.druid.data.input.ExcelParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;

/**
 */
public class ExcelInputFormat extends AbstractExcelInputFormat<Row>
{
  @Override
  protected RecordReader<NullWritable, Row> toRecordReader(
      final ExcelParser.ParsedIterator iterator,
      TaskAttemptContext context
  )
  {
    return new RecordReader<NullWritable, Row>()
    {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        return iterator.hasNext();
      }

      @Override
      public NullWritable getCurrentKey() throws IOException, InterruptedException
      {
        return NullWritable.get();
      }

      @Override
      public Row getCurrentValue() throws IOException, InterruptedException
      {
        return iterator.next();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        return iterator.progress();
      }

      @Override
      public void close() throws IOException
      {
        iterator.close();
      }
    };
  }
}
