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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.CombinePerPathInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;


public class StaticPathSpec implements PathSpec
{
  private static final Logger log = new Logger(StaticPathSpec.class);

  private final String paths;
  private final String inputFormat;
  private final boolean combinePerPath;

  private final Map<String, Object> properties;

  @JsonCreator
  public StaticPathSpec(
      @JsonProperty("paths") String paths,
      @JsonProperty("combinePerPath") boolean combinePerPath,
      @JsonProperty("inputFormat") String inputFormat,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    this.paths = Preconditions.checkNotNull(paths);
    this.combinePerPath = combinePerPath;
    this.inputFormat = inputFormat;
    this.properties = properties;
    Preconditions.checkArgument(
        !combinePerPath ||
        inputFormat == null ||
        inputFormat.equals(TextInputFormat.class.getName()));
  }

  public StaticPathSpec(String paths, String inputFormat)
  {
    this(paths, false, inputFormat, null);
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    log.info("Adding paths[%s]", paths);
    if (properties != null && !properties.isEmpty()) {
      HadoopPathSpec.configure(job, properties);
    }
    Class<? extends InputFormat> inputFormatClass = loadFormatterClass();

    addToMultipleInputs(config, job, paths, inputFormatClass);
    if (combinePerPath) {
      job.setInputFormatClass(CombinePerPathInputFormat.class);
    }

    return job;
  }

  private Class<? extends InputFormat> loadFormatterClass()
  {
    if (inputFormat == null) {
      return TextInputFormat.class;
    }
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    if (loader == null) {
      loader = StaticPathSpec.class.getClassLoader();
    }
    try {
      return (Class<? extends InputFormat>) loader.loadClass(inputFormat);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @JsonProperty
  public String getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public String getPaths()
  {
    return paths;
  }

  @JsonProperty
  public boolean isCombinePerPath()
  {
    return combinePerPath;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  public static void addToMultipleInputs(
      HadoopDruidIndexerConfig config,
      Job job,
      String path,
      Class<? extends InputFormat> inputFormatClass
  )
  {
    if (path != null) {
      addToMultipleInputs(config, job, ImmutableSet.of(path), inputFormatClass);
    }
  }

  public static void addToMultipleInputs(
      HadoopDruidIndexerConfig config,
      Job job,
      Set<String> paths,
      Class<? extends InputFormat> inputFormatClass
  )
  {
    if (paths == null || paths.isEmpty()) {
      return;
    }

    Class<? extends InputFormat> inputFormatClassToUse = inputFormatClass;
    if (inputFormatClassToUse == null) {
      if (config.isCombineText()) {
        inputFormatClassToUse = CombineTextInputFormat.class;
      } else {
        inputFormatClassToUse = TextInputFormat.class;
      }
    }

    // Due to https://issues.apache.org/jira/browse/MAPREDUCE-5061 we can't directly do
    // MultipleInputs.addInputPath(job, path, inputFormatClassToUse)
    // but have to handle hadoop glob path ourselves correctly
    // This change and HadoopGlobPathSplitter.java can be removed once the hadoop issue is fixed
    Set<String> pathStrings = Sets.newLinkedHashSet();
    for (String path : paths) {
      Iterables.addAll(pathStrings, HadoopGlobPathSplitter.splitGlob(path));
    }
    if (!pathStrings.isEmpty()) {
      addInputPath(job, pathStrings, inputFormatClassToUse);
    }
  }

  // copied from MultipleInputs.addInputPath with slight modifications
  public static void addInputPath(Job job, Iterable<String> pathStrings, Class<? extends InputFormat> inputFormatClass)
  {
    Configuration conf = job.getConfiguration();
    StringBuilder inputFormats = new StringBuilder(Strings.nullToEmpty(conf.get(MultipleInputs.DIR_FORMATS)));

    String[] paths = Iterables.toArray(pathStrings, String.class);
    for (int i = 0; i < paths.length - 1; i++) {
      if (inputFormats.length() > 0) {
        inputFormats.append(',');
      }
      inputFormats.append(paths[i]).append(';').append(inputFormatClass.getName());
    }
    if (inputFormats.length() > 0) {
      conf.set(MultipleInputs.DIR_FORMATS, inputFormats.toString());
    }
    // add last one separately for possible initialization in MultipleInputs
    MultipleInputs.addInputPath(job, new Path(paths[paths.length - 1]), inputFormatClass);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticPathSpec that = (StaticPathSpec) o;

    if (paths != null ? !paths.equals(that.paths) : that.paths != null) {
      return false;
    }
    if (combinePerPath != that.combinePerPath) {
      return false;
    }
    return !(inputFormat != null ? !inputFormat.equals(that.inputFormat) : that.inputFormat != null);

  }

  @Override
  public int hashCode()
  {
    int result = paths != null ? paths.hashCode() : 0;
    result = 31 * result + (combinePerPath ? 1 : 0);
    result = 31 * result + (inputFormat != null ? inputFormat.hashCode() : 0);
    return result;
  }
}
