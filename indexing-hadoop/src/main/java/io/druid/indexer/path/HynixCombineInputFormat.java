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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class HynixCombineInputFormat extends FileInputFormat
{
  private static final Logger log = new Logger(HynixCombineInputFormat.class);
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

    // splitSize < 0 : no combine, splitSize == 0 : combine per elements
    final int splitSize = conf.getInt(HynixPathSpec.SPLIT_SIZE, 0);

    List<InputSplit> result = Lists.newArrayList();

    Job cloned = new Job(conf);
    int currentSize = 0;
    Map<String, List<FileSplit>> combined = Maps.newHashMap();
    for (String specString : conf.get(HynixPathSpec.PATH_SPECS).split(",")) {
      String[] spec = specString.split(";");
      String dataSource = spec[0];
      FileInputFormat.setInputPaths(cloned, StringUtils.join(",", HadoopGlobPathSplitter.splitGlob(spec[1])));
      List<FileSplit> splits = super.getSplits(cloned);
      if (splitSize == 0) {
        result.add(new HynixSplit(ImmutableMap.of(dataSource, splits)));
        continue;
      }
      for (FileSplit split : splits) {
        List<FileSplit> splitList = combined.get(dataSource);
        if (splitList == null) {
          combined.put(dataSource, splitList = Lists.newArrayList());
        }
        splitList.add(split);
        currentSize += split.getLength();
        if (currentSize > splitSize) {
          result.add(new HynixSplit(combined));
          combined = Maps.newHashMap();
          currentSize = 0;
        }
      }
    }
    if (!combined.isEmpty()) {
      result.add(new HynixSplit(combined));
    }

    for (int i = 0; i < result.size(); i++) {
      log.info("Split-[%04d] : [%s]", i, ((HynixSplit)result.get(i)).splits);
    }
    return result;
  }

  public static class HynixSplit extends InputSplit implements Writable
  {
    private final Map<String, List<FileSplit>> splits;

    public HynixSplit()
    {
      splits = Maps.newHashMap();
    }

    public HynixSplit(Map<String, List<FileSplit>> splits)
    {
      this.splits = splits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      long length = 0;
      for (List<FileSplit> splitList : splits.values()) {
        for (FileSplit split : splitList) {
          length += split.getLength();
        }
      }
      return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      List<String> hosts = Lists.newArrayList();
      for (List<FileSplit> splitList : splits.values()) {
        for (FileSplit split : splitList) {
          hosts.addAll(Arrays.asList(split.getLocations()));
        }
      }
      return DatasourceInputFormat.getFrequentLocations(hosts, 3);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(splits.size());
      for (Map.Entry<String, List<FileSplit>> entry : splits.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeInt(entry.getValue().size());
        for (FileSplit split : entry.getValue()) {
          split.write(out);
        }
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      int dataSourceLength = in.readInt();
      for (int i = 0; i < dataSourceLength; i++) {
        String dataSource = in.readUTF();
        int splitLength = in.readInt();
        List<FileSplit> splitList = Lists.newArrayListWithCapacity(splitLength);
        for (int j = 0; j < splitLength; j++) {
          FileSplit split = new FileSplit();
          split.readFields(in);
          splitList.add(split);
        }
        splits.put(dataSource, splitList);
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
      private final Iterator<Map.Entry<String, List<FileSplit>>> iterator = hynixSplit.splits.entrySet().iterator();
      private List<FileSplit> splits;

      private RecordReader reader;
      private long progress;
      private long totalLength;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        totalLength = split.getLength();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
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
        if (splits == null || index == splits.size()) {
          if (!iterator.hasNext()) {
            return false;
          }
          Map.Entry<String, List<FileSplit>> next = iterator.next();
          CURRENT_DATASOURCE.set(next.getKey());
          splits = next.getValue();
          index = 0;
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
