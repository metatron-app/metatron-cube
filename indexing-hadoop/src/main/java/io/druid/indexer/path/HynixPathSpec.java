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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 */
public class HynixPathSpec implements PathSpec
{
  private static final int DEFAULT_SPLIT_SIZE = 0;

  public static final String PATH_SPECS = "hynix.input.path.specs";
  public static final String INPUT_FORMAT = "hynix.input.path.specs.format";
  public static final String SPLIT_SIZE = "hynix.input.path.specs.split.size";

  public static final String FIND_RECURSIVE = FileInputFormat.INPUT_DIR_RECURSIVE;
  public static final String EXTRACT_PARTITION = "hynix.input.path.extract.partition";

  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<HynixPathSpecElement> elements;
  private final Class<? extends InputFormat> inputFormat;
  private final int splitSize;
  private final boolean findRecursive;
  private final boolean extractPartition;

  @JsonCreator
  public HynixPathSpec(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("elements") List<HynixPathSpecElement> elements,
      @JsonProperty("inputFormat") Class<? extends InputFormat> inputFormat,
      @JsonProperty("splitSize") String splitSize,
      @JsonProperty("findRecursive") boolean findRecursive,
      @JsonProperty("extractPartition") boolean extractPartition
  )
  {
    this.basePath = basePath;
    this.elements = Preconditions.checkNotNull(elements);
    this.inputFormat = inputFormat == null ? TextInputFormat.class : inputFormat;
    this.splitSize = Ints.checkedCast(StringUtils.parseKMGT(splitSize, DEFAULT_SPLIT_SIZE));
    this.findRecursive = findRecursive;
    this.extractPartition = extractPartition;

    Preconditions.checkArgument(!elements.isEmpty());
    Preconditions.checkArgument(basePath == null || new Path(basePath).isAbsolute());
    for (HynixPathSpecElement element : elements) {
      for (String path : HadoopGlobPathSplitter.splitGlob(element.getPaths())) {
        if (basePath == null) {
          Preconditions.checkArgument(new Path(path).isAbsolute());
        } else {
          Preconditions.checkArgument(!new Path(path).isAbsolute());
        }
      }
    }
  }

  @JsonProperty
  public String getBasePath()
  {
    return basePath;
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

  @JsonProperty
  public boolean isExtractPartition()
  {
    return extractPartition;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    String schemaDataSource = config.getDataSource();
    List<String> paths = Lists.newArrayList();
    StringBuilder builder = new StringBuilder();
    for (HynixPathSpecElement element : elements) {
      String dataSource = Optional.fromNullable(element.getDataSource()).or(schemaDataSource);
      for (String path : HadoopGlobPathSplitter.splitGlob(element.getPaths())) {
        if (builder.length() > 0) {
          builder.append(',');
        }
        if (basePath != null) {
          path = basePath + "/" + path;
        }
        builder.append(dataSource).append(';').append(path);
        paths.add(path);
      }
    }
    // ds1;path1,ds1;path2,ds2;path3
    job.getConfiguration().set(PATH_SPECS, builder.toString());
    job.getConfiguration().setClass(INPUT_FORMAT, inputFormat, InputFormat.class);
    job.getConfiguration().setInt(SPLIT_SIZE, splitSize);

    job.getConfiguration().setBoolean(FIND_RECURSIVE, findRecursive);
    job.getConfiguration().setBoolean(EXTRACT_PARTITION, extractPartition);

    // used for sized partition spec
    // path1;format1,path2;format2
    StaticPathSpec.addInputPath(job, paths, HynixCombineInputFormat.class);

    // should overwrite (DelegatingInputFormat is set in MultipleInputs.addInputPath)
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

    if (!Objects.equals(basePath, that.basePath)) {
      return false;
    }
    if (!elements.equals(that.elements)) {
      return false;
    }
    if (!inputFormat.equals(that.inputFormat)) {
      return false;
    }
    if (splitSize != that.splitSize) {
      return false;
    }
    if (findRecursive != that.findRecursive) {
      return false;
    }
    if (extractPartition != that.extractPartition) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(basePath, elements, inputFormat, splitSize, findRecursive, extractPartition);
  }

  @Override
  public String toString()
  {
    return "HynixPathSpec{" +
           "basePath=" + basePath +
           ", elements=" + elements +
           ", inputFormat=" + inputFormat +
           ", splitSize=" + splitSize +
           ", findRecursive=" + findRecursive +
           ", extractPartition=" + extractPartition +
           '}';
  }
}
