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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.metamx.common.logger.Logger;
import io.druid.common.Progressing;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@JsonTypeName("hadoop")
public class HadoopInputUtils
{
  private static final Logger LOG = new Logger(HadoopInputUtils.class);

  private static void setupHadoopAwareParser(InputRowParser parser, MapperContext context) throws IOException
  {
    if (parser instanceof HadoopAwareParser) {
      ((HadoopAwareParser) parser).setup(context);
    }
    if (parser instanceof InputRowParser.Delegated) {
      setupHadoopAwareParser(((InputRowParser.Delegated) parser).getDelegate(), context);
    }
  }

  public static RecordReader<Object, InputRow> toRecordReader(final InputRowParser parser, final Job job)
      throws IOException, InterruptedException
  {
    return toRecordReader(makeInputFormat(job), parser, job);
  }

  // todo : cannot extract datasource / partition columns (see HadoopCombineInputFormat)
  public static RecordReader<Object, InputRow> toRecordReader(
      final InputFormat<?, ?> format,
      final InputRowParser parser,
      final Job job
  )
      throws IOException, InterruptedException
  {

    final Configuration configuration = job.getConfiguration();
    final TaskID taskID = new TaskID("dummy-job", 0, TaskType.MAP, 0);

    final List<InputSplit> splits = format.getSplits(new JobContextImpl(configuration, new JobID("test", 0)));
    long total = 0;
    final float[] thresholds = new float[splits.size() + 1];
    for (int i = 0; i < splits.size(); i++) {
      thresholds[i] = total;
      total += Math.max(10, splits.get(i).getLength());
    }
    thresholds[splits.size()] = total;
    final Iterator<RecordReader> readers = Iterators.transform(
        splits.iterator(),
        new Function<InputSplit, RecordReader>()
        {
          @Override
          public RecordReader apply(InputSplit split)
          {
            final TaskAttemptContextImpl context = new TaskAttemptContextImpl(
                configuration,
                new TaskAttemptID(taskID, splits.indexOf(split))
            );
            try {
              setupHadoopAwareParser(parser, new MapperContext(configuration, split));
              RecordReader reader = format.createRecordReader(split, context);
              reader.initialize(split, context);
              return reader;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    return new RecordReader<Object, InputRow>()
    {
      private int index = -1;
      private RecordReader reader;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        // noop
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        while (reader == null || !reader.nextKeyValue()) {
          IOUtils.closeQuietly(reader);
          if (!readers.hasNext()) {
            return false;
          }
          reader = readers.next();
          index++;
        }
        return reader != null;
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException
      {
        return reader == null ? null : reader.getCurrentKey();
      }

      @Override
      @SuppressWarnings("unchecked")
      public InputRow getCurrentValue() throws IOException, InterruptedException
      {
        return reader == null ? null : parser.parse(reader.getCurrentValue());
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        if (index <= 0) {
          return reader == null ? 0f : reader.getProgress();
        }
        if (index < thresholds.length) {
          final float delta = (thresholds[index] - thresholds[index - 1]) * reader.getProgress();
          return (thresholds[index - 1] + delta) / thresholds[thresholds.length - 1];
        }
        return 1f;
      }

      @Override
      public void close() throws IOException
      {
        IOUtils.closeQuietly(reader);
      }
    };
  }

  public static InputFormat<?, ?> makeInputFormat(Job job)
  {
    try {
      return ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <V> Iterator<V> toIterator(final RecordReader<?, ? extends V> reader)
  {
    return new Progressing.OnIterator<V>()
    {
      private boolean hasMore;

      {
        try {
          hasMore = reader.nextKeyValue();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        reader.close();
      }

      @Override
      public float progress()
      {
        try {
          return reader.getProgress();
        }
        catch (Exception e) {
          return 0.5f;
        }
      }

      @Override
      public boolean hasNext()
      {
        return hasMore;
      }

      @Override
      public V next()
      {
        if (!hasMore) {
          throw new NoSuchElementException();
        }
        try {
          final V value = reader.getCurrentValue();
          hasMore = reader.nextKeyValue();
          return value;
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private static class MapperContext implements HadoopInputContext
  {
    private final Configuration configuration;
    private final InputSplit current;

    private MapperContext(Configuration configuration, InputSplit current)
    {
      this.configuration = configuration;
      this.current = current;
    }

    @Override
    public Configuration getConfiguration()
    {
      return configuration;
    }

    @Override
    public InputSplit getInputSplit()
    {
      return current;
    }

    @Override
    public <T> T unwrap(Class<T> clazz)
    {
      return null;
    }
  }
}
