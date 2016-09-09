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

package io.druid.indexer.path;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
public class HynixCombineInputFormat extends FileInputFormat
{
  public static final ThreadLocal<String> CURRENT_DATASOURCE = new ThreadLocal<>();

  @Override
  protected boolean isSplitable(JobContext context, Path file)
  {
    return false;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    Configuration conf = context.getConfiguration();
    boolean combine = conf.getBoolean(HynixPathSpec.COMBINE_PER_PATH, false);

    List<InputSplit> result = Lists.newArrayList();

    Job cloned = new Job(conf);
    for (String specString : conf.get(HynixPathSpec.PATH_SPECS).split(",")) {
      String[] spec = specString.split(";");
      FileInputFormat.setInputPaths(cloned, StringUtils.join(",", HadoopGlobPathSplitter.splitGlob(spec[1])));
      List<FileSplit> splits = super.getSplits(cloned);
      if (combine) {
        result.add(new HynixSplit(spec[0], splits));
      } else {
        for (FileSplit split : splits) {
          result.add(new HynixSplit(spec[0], Arrays.asList(split)));
        }
      }
    }

    return result;
  }

  public static class HynixSplit extends InputSplit implements Writable
  {
    private String dataSource;
    private List<FileSplit> splits;

    public HynixSplit()
    {
      splits = Lists.newArrayList();
    }

    public HynixSplit(String dataSource, List<FileSplit> splits)
    {
      this.dataSource = dataSource;
      this.splits = splits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      long length = 0;
      for (InputSplit split : splits) {
        length += split.getLength();
      }
      return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return DatasourceInputFormat.getFrequentLocations(
          Iterables.concat(
              Iterables.transform(
                  splits, new Function<FileSplit, Iterable<String>>()
                  {
                    @Override
                    public Iterable<String> apply(FileSplit input)
                    {
                      try {
                        return Arrays.asList(input.getLocations());
                      }
                      catch (IOException e) {
                        throw new UnsupportedOperationException(e);
                      }
                    }
                  }
              )
          ), 3
      );
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(dataSource);
      out.writeInt(splits.size());
      for (FileSplit split : splits) {
        split.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      dataSource = in.readUTF();
      int length = in.readInt();
      for (int i = 0; i < length; i++) {
        FileSplit split = new FileSplit();
        split.readFields(in);
        splits.add(split);
      }
    }
  }

  @Override
  public RecordReader createRecordReader(
      final InputSplit split,
      final TaskAttemptContext context
  ) throws IOException
  {
    final Configuration conf = context.getConfiguration();

    final HynixSplit hynixSplit = (HynixSplit) split;
    final InputFormat format = ReflectionUtils.newInstance(
        conf.getClass(HynixPathSpec.INPUT_FORMAT, TextInputFormat.class, InputFormat.class), conf
    );
    return new RecordReader()
    {
      private int index;
      private final String dataSource = hynixSplit.dataSource;
      private final List<FileSplit> splits = hynixSplit.splits;

      private RecordReader reader;
      private long progress;
      private long totalLength;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        context.getConfiguration().set(HynixPathSpec.CURRENT_DATASOURCE, dataSource);
        totalLength = split.getLength();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        CURRENT_DATASOURCE.set(dataSource);
        while (reader == null || !reader.nextKeyValue()) {
          if (!initNextRecordReader()) {
            return false;
          }
        }
        return true;
      }

      private boolean initNextRecordReader() throws IOException, InterruptedException
      {
        if (reader != null) {
          progress += splits.get(index - 1).getLength();    // done processing so far
          reader.close();
          reader = null;
        }
        if (index == splits.size()) {
          return false;
        }
        FileSplit split = splits.get(index++);
        reader = format.createRecordReader(split, context);
        reader.initialize(split, context);
        return true;
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException
      {
        return reader.getCurrentKey();
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException
      {
        return reader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        long subProgress = 0;
        if (reader != null) {
          subProgress = (long) (reader.getProgress() * splits.get(index - 1).getLength());
        }
        return Math.min(1.0f, (progress + subProgress) / (float) totalLength);
      }

      @Override
      public void close() throws IOException
      {
        if (reader != null) {
          reader.close();
          reader = null;
        }
      }
    };
  }
}
