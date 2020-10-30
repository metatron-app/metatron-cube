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

package io.druid.storage.hdfs;

import com.google.common.collect.Lists;
import io.druid.indexer.hadoop.RecordReaderWrapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
    final JobConf jobConf = new JobConf(context.getConfiguration());
    final List<InputSplit> splits = Lists.newArrayList();
    for (org.apache.hadoop.mapred.InputSplit split : inputFormat.getSplits(jobConf, -1)) {
      splits.add(new Wrapper(split));
    }
    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    final JobConf jobConf = new JobConf(context.getConfiguration());
    return new RecordReaderWrapper(inputFormat.getRecordReader(((Wrapper) split).split, jobConf, Reporter.NULL));
  }

  private static class Wrapper extends InputSplit
  {
    private final org.apache.hadoop.mapred.InputSplit split;

    private Wrapper(org.apache.hadoop.mapred.InputSplit split) {this.split = split;}

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      return split.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return split.getLocations();
    }
  }
}
