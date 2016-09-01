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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;
import java.util.List;

/**
 */
public class CombinePerPathInputFormat extends CombineTextInputFormat
{
  @Override
  protected boolean isSplitable(JobContext context, Path file)
  {
    return false;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    Configuration conf = context.getConfiguration();
    Job cloned = new Job(conf);
    cloned.getConfiguration().set(MultipleInputs.DIR_FORMATS, "");
    cloned.getConfiguration().set(MultipleInputs.DIR_MAPPERS, "");
    cloned.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", 0L);

    List<InputSplit> splits = Lists.newArrayList();
    for (String pathMapping : conf.get(MultipleInputs.DIR_FORMATS).split(",")) {
      FileInputFormat.setInputPaths(cloned, new Path(pathMapping.split(";")[0]));
      splits.addAll(super.getSplits(cloned));
    }

    return splits;
  }
}
