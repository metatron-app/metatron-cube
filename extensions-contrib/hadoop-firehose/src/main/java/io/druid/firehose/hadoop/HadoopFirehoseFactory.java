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

package io.druid.firehose.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.hadoop.HadoopInputUtils;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.utils.Runnables;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.druid.indexer.HadoopDruidIndexerConfig.JSON_MAPPER;

@JsonTypeName("hadoop")
public class HadoopFirehoseFactory implements FirehoseFactory
{
  private static final Logger LOG = new Logger(HadoopFirehoseFactory.class);

  private final String extension;   // for path-spec
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
    try {
      return toFirehose(parser, configureJobFromPathSpec());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static Firehose toFirehose(final InputRowParser parser, final Job job) throws Exception
  {
    final RecordReader<Object, InputRow> reader = HadoopInputUtils.toRecordReader(parser, job);

    return new Firehose()
    {
      private boolean hasMore = reader.nextKeyValue();

      @Override
      public boolean hasMore()
      {
        return hasMore;
      }

      @Override
      public InputRow nextRow()
      {
        if (!hasMore) {
          throw new NoSuchElementException();
        }
        try {
          final InputRow value = reader.getCurrentValue();
          hasMore = reader.nextKeyValue();
          return value;
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

  private Job configureJobFromPathSpec() throws IOException
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
    }
    catch (NoClassDefFoundError e) {
      LOG.info(e, "Cannot find class.. use 'extension' for accessing classes in other extension");
      throw e;
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }
}
