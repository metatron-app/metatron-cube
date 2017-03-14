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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

/**
 */
public class InputFormatWrapper extends InputFormat
{
  private final org.apache.hadoop.mapred.InputFormat inputFormat;

  public InputFormatWrapper(org.apache.hadoop.mapred.InputFormat inputFormat)
  {
    this.inputFormat = inputFormat;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    throw new UnsupportedOperationException("getSplits");
  }

  @Override
  public RecordReader createRecordReader(
      InputSplit split, TaskAttemptContext context
  ) throws IOException, InterruptedException
  {
    FileSplit fileSplit = (FileSplit) split;
    org.apache.hadoop.mapred.FileSplit changed = new org.apache.hadoop.mapred.FileSplit(
        fileSplit.getPath(),
        fileSplit.getStart(),
        fileSplit.getLength(),
        fileSplit.getLocations()
    );
    return new RecordReaderWrapper(
        inputFormat.getRecordReader(changed, new JobConf(context.getConfiguration()), Reporter.NULL)
    );
  }
}
