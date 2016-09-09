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
  public static final String COMBINE_PER_PATH = "hynix.input.path.specs.combine";
  public static final String CURRENT_DATASOURCE = "hynix.input.path.specs.current.datasource";

  private final List<HynixPathSpecElement> elements;
  private final Class<? extends InputFormat> inputFormat;
  private final boolean combineElement;

  @JsonCreator
  public HynixPathSpec(
      @JsonProperty("elements") List<HynixPathSpecElement> elements,
      @JsonProperty("combineElement") boolean combineElement,
      @JsonProperty("inputFormat") Class<? extends InputFormat> inputFormat
  )
  {
    this.elements = Preconditions.checkNotNull(elements);
    this.combineElement = combineElement;
    this.inputFormat = inputFormat == null ? TextInputFormat.class : inputFormat;
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
  public boolean isCombineElement()
  {
    return combineElement;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    StringBuilder builder = new StringBuilder();
    for (HynixPathSpecElement element : elements) {
      if (builder.length() > 0) {
        builder.append(',');
      }
      builder.append(element.getDataSource()).append(';').append(element.getPaths());
    }
    job.getConfiguration().set(PATH_SPECS, builder.toString());
    job.getConfiguration().setBoolean(COMBINE_PER_PATH, combineElement);
    job.getConfiguration().setClass(INPUT_FORMAT, inputFormat, InputFormat.class);

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

    if (combineElement != that.combineElement) {
      return false;
    }
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
    result = 31 * result + (combineElement ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "HynixPathSpec{" +
           "elements=" + elements +
           ", inputFormat=" + inputFormat +
           ", combinePerPath=" + combineElement +
           '}';
  }
}
