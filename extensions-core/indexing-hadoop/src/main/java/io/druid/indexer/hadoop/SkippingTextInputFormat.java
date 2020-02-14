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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class SkippingTextInputFormat extends TextInputFormat
{
  public static final String SKIP_ROW_NUM = "SKIP_ROW_NUM";

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
  {
    final RecordReader<LongWritable, Text> reader = super.createRecordReader(split, context);
    return new RecordReader<LongWritable, Text>()
    {
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        reader.initialize(split, context);
        FileSplit fileSplit = (FileSplit) split;
        int skip = context.getConfiguration().getInt(SKIP_ROW_NUM, -1);
        if (fileSplit.getStart() == 0 && skip > 0) {
          for (int i = 0; reader.nextKeyValue() && i < skip; i++) {
          }
        }
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        return reader.nextKeyValue();
      }

      @Override
      public LongWritable getCurrentKey() throws IOException, InterruptedException
      {
        return reader.getCurrentKey();
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException
      {
        return reader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        return reader.getProgress();
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }
    };
  }
}
