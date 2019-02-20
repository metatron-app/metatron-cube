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

package io.druid.firehose.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.utils.Runnables;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.druid.indexer.HadoopDruidIndexerConfig.JSON_MAPPER;

@JsonTypeName("hadoop")
public class HadoopFirehoseFactory implements FirehoseFactory
{
  private static final Logger LOG = new Logger(HadoopFirehoseFactory.class);

  private final String extension;
  private final Map<String, Object> pathSpec;

  @JsonCreator
  public HadoopFirehoseFactory(
      @JsonProperty("pathSpec") Map<String, Object> pathSpec,
      @JsonProperty("extension") String extension
  )
  {
    this.extension = extension;
    this.pathSpec = Preconditions.checkNotNull(pathSpec, "'pathSpec' should not be null");
  }

  @JsonProperty
  public Map<String, Object> getPathSpec()
  {
    return pathSpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExtension()
  {
    return extension;
  }

  @Override
  public Firehose connect(final InputRowParser parser) throws IOException, ParseException
  {
    LOG.info("Loading from path spec %s", pathSpec);
    final Job spec = configureJob();
    final Configuration configuration = spec.getConfiguration();
    final TaskAttemptContextImpl context = new TaskAttemptContextImpl(
        configuration,
        new TaskAttemptID("test", 0, TaskType.MAP, 0, 0)
    );
    final Iterator<RecordReader> readers;
    try {
      final InputFormat<?, ?> format = ReflectionUtils.newInstance(spec.getInputFormatClass(), configuration);
      final List<InputSplit> splits = format.getSplits(new JobContextImpl(configuration, new JobID("test", 0)));
      readers = Iterators.transform(
          splits.iterator(),
          new Function<InputSplit, RecordReader>()
          {
            @Override
            public RecordReader apply(InputSplit split)
            {
              try {
                return format.createRecordReader(split, context);
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new Firehose()
    {
      private RecordReader reader;
      private boolean hasMore;

      private RecordReader reader() throws IOException, InterruptedException
      {
        for (;!hasMore; hasMore = reader.nextKeyValue()) {
          IOUtils.closeQuietly(reader);
          if (!readers.hasNext()) {
            return null;
          }
          reader = readers.next();
        }
        return reader;
      }

      @Override
      public boolean hasMore()
      {
        try {
          return reader() != null;
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      @SuppressWarnings("unchecked")
      public InputRow nextRow()
      {
        try {
          RecordReader current = reader();
          Object value = current.getCurrentValue();
          hasMore = current.nextKeyValue();
          return parser.parse(value);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public Runnable commit()
      {
        return Runnables.getNoopRunnable();
      }

      @Override
      public void close() throws IOException
      {
        IOUtils.closeQuietly(reader);
      }
    };
  }

  private Job configureJob() throws IOException
  {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = HadoopFirehoseFactory.class.getClassLoader();
    if (extension != null) {
      loader = Initialization.getClassLoaderForExtension(extension);
    }
    Thread.currentThread().setContextClassLoader(loader);
    try {
      final PathSpec pathSpec = JSON_MAPPER.convertValue(this.pathSpec, PathSpec.class);
      return pathSpec.addInputPaths(null, Job.getInstance());
    } catch (NoClassDefFoundError e) {
      LOG.info("Cannot find class.. use 'extension' for accessing classes in other extension");
      throw e;
    } finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }
}
