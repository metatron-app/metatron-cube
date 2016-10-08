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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;

/**
 */
public class HynixPathSpec implements PathSpec
{
  public static final String PATH_SPECS = "hynix.input.path.specs";
  public static final String INPUT_FORMAT = "hynix.input.path.specs.format";
  public static final String SPLIT_SIZE = "hynix.input.path.specs.split.size";

  private final List<HynixPathSpecElement> elements;
  private final Class<? extends InputFormat> inputFormat;
  private final int splitSize;

  @JsonCreator
  public HynixPathSpec(
      @JsonProperty("elements") List<HynixPathSpecElement> elements,
      @JsonProperty("inputFormat") Class<? extends InputFormat> inputFormat,
      @JsonProperty("splitSize") int splitSize
  )
  {
    this.elements = Preconditions.checkNotNull(elements);
    this.inputFormat = inputFormat == null ? TextInputFormat.class : inputFormat;
    this.splitSize = splitSize;
    Preconditions.checkArgument(!elements.isEmpty());
  }


  @JsonProperty
  public List<HynixPathSpecElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public int getSplitSize()
  {
    return splitSize;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    List<String> paths = Lists.newArrayList();
    StringBuilder builder = new StringBuilder();
    for (HynixPathSpecElement element : elements) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      paths.addAll(HadoopGlobPathSplitter.splitGlob(element.getPaths()));
      builder.append(element.getDataSource()).append(';').append(element.getPaths());
    }
    job.getConfiguration().set(PATH_SPECS, builder.toString());
    job.getConfiguration().setClass(INPUT_FORMAT, inputFormat, InputFormat.class);
    job.getConfiguration().setInt(SPLIT_SIZE, splitSize);

    // used for sized partition spec
    StaticPathSpec.addInputPath(job, paths, HynixCombineInputFormat.class);

    // should overwrite
    job.setInputFormatClass(HynixCombineInputFormat.class);

    return job;
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

    HynixPathSpec that = (HynixPathSpec) o;

    if (!inputFormat.equals(that.inputFormat)) {
      return false;
    }
    if (!elements.equals(that.elements)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = elements.hashCode();
    result = 31 * result + inputFormat.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "HynixPathSpec{" +
           "elements=" + elements +
           ", inputFormat=" + inputFormat +
           ", splitSize=" + splitSize +
           '}';
  }
}
