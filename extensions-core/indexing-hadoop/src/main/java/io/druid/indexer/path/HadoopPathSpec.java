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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.SkippingTextInputFormat;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.logger.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("hadoop")
public class HadoopPathSpec implements PathSpec
{
  public static final int DEFAULT_SPLIT_SIZE = HadoopCombineInputFormat.COMBINE_PER_ELEMENT;

  public static final String PATH_ELEMENTS_JSON = "druid.hadoop.input.path.elements";
  public static final String INPUT_FORMAT_OLD = "druid.hadoop.input.path.specs.format.old";
  public static final String INPUT_FORMAT_NEW = "druid.hadoop.input.path.specs.format.new";
  public static final String SPLIT_SIZE = "druid.hadoop.input.path.specs.split.size";

  public static final String FIND_RECURSIVE = FileInputFormat.INPUT_DIR_RECURSIVE;
  public static final String EXTRACT_PARTITION = "druid.hadoop.input.path.extract.partition";
  public static final String EXTRACT_PARTITION_REGEX = "druid.hadoop.input.path.extract.partition.regex";

  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)

  private final List<PathSpecElement> elements;
  private final Class inputFormat;
  private final int skipHeader;
  private final long splitSize;
  private final boolean findRecursive;
  private final boolean extractPartition;
  private final String extractPartitionRegex;
  private final Map<String, Object> properties;

  @JsonCreator
  public HadoopPathSpec(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("elements") List<PathSpecElement> elements,
      @JsonProperty("inputFormat") Class inputFormat,
      @JsonProperty("skipHeader") int skipHeader,
      @JsonProperty("splitSize") String splitSize,
      @JsonProperty("findRecursive") boolean findRecursive,
      @JsonProperty("extractPartition") boolean extractPartition,
      @JsonProperty("extractPartitionRegex") String extractPartitionRegex,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    this.basePath = basePath;
    this.elements = Preconditions.checkNotNull(elements);
    if (skipHeader > 0) {
      Preconditions.checkArgument(inputFormat == null || inputFormat == SkippingTextInputFormat.class);
      inputFormat = SkippingTextInputFormat.class;
    }
    this.skipHeader = skipHeader;
    this.inputFormat = inputFormat == null ? TextInputFormat.class : inputFormat;
    this.splitSize = StringUtils.parseKMGT(splitSize, DEFAULT_SPLIT_SIZE);
    this.findRecursive = findRecursive;
    this.extractPartition = extractPartition;
    this.extractPartitionRegex = extractPartition ? extractPartitionRegex : null;
    this.properties = properties;
    Preconditions.checkArgument(!elements.isEmpty());
    Preconditions.checkArgument(basePath == null || new Path(basePath).isAbsolute());
    for (PathSpecElement element : elements) {
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getBasePath()
  {
    return basePath;
  }

  @JsonProperty
  public List<PathSpecElement> getElements()
  {
    return elements;
  }

  @JsonProperty
  public Class getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public long getSplitSize()
  {
    return splitSize;
  }

  @JsonProperty
  public boolean isExtractPartition()
  {
    return extractPartition;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExtractPartitionRegex()
  {
    return extractPartitionRegex;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    if (properties != null && !properties.isEmpty()) {
      configure(job, properties);
    }

    String dataSource = config.getDataSource();

    List<String> paths = Lists.newArrayList();  // needed for some reason but I've forgot
    List<PathSpecElement> rewritten = Lists.newArrayList();
    for (PathSpecElement element : elements) {
      StringBuilder elementPaths = new StringBuilder();
      for (String path : HadoopGlobPathSplitter.splitGlob(element.getPaths())) {
        path = basePath == null ? path : basePath + "/" + path;
        if (elementPaths.length() > 0) {
          elementPaths.append(',');
        }
        elementPaths.append(path);
        paths.add(path);
      }
      element = element.withPaths(elementPaths.toString());
      if (element.getDataSource() == null) {
        element = element.withDataSource(dataSource);
      }
      rewritten.add(element);
    }
    job.getConfiguration().set(
        PATH_ELEMENTS_JSON,
        ObjectMappers.excludeNulls(HadoopDruidIndexerConfig.JSON_MAPPER).writeValueAsString(rewritten)
    );
    if (InputFormat.class.isAssignableFrom(inputFormat)) {
      job.getConfiguration().setClass(INPUT_FORMAT_NEW, inputFormat, InputFormat.class);
    } else if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormat)) {
      job.getConfiguration().setClass(INPUT_FORMAT_OLD, inputFormat, org.apache.hadoop.mapred.InputFormat.class);
    } else {
      throw new IllegalArgumentException("invalid format " + inputFormat);
    }
    if (skipHeader > 0) {
      job.getConfiguration().setInt(SkippingTextInputFormat.SKIP_ROW_NUM, skipHeader);
    }
    job.getConfiguration().setLong(SPLIT_SIZE, splitSize);

    job.getConfiguration().setBoolean(FIND_RECURSIVE, findRecursive);
    job.getConfiguration().setBoolean(EXTRACT_PARTITION, extractPartition);
    if (!Strings.isNullOrEmpty(extractPartitionRegex)) {
      job.getConfiguration().set(EXTRACT_PARTITION_REGEX, extractPartitionRegex);
    }

    // used for sized partition spec
    // path1;format1,path2;format2
    StaticPathSpec.addInputPath(job, paths, HadoopCombineInputFormat.class);

    // should overwrite (DelegatingInputFormat is set in MultipleInputs.addInputPath)
    job.setInputFormatClass(HadoopCombineInputFormat.class);

    return job;
  }

  static void configure(Job job, Map<String, Object> properties)
  {
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      Object value = entry.getValue();
      if (value == null) {
        job.getConfiguration().unset(entry.getKey());
        continue;
      }
      if (value instanceof String) {
        job.getConfiguration().set(entry.getKey(), (String) value);
      } else if (value instanceof Integer) {
        job.getConfiguration().setInt(entry.getKey(), (Integer) value);
      } else if (value instanceof Long) {
        job.getConfiguration().setLong(entry.getKey(), (Long) value);
      } else if (value instanceof Float) {
        job.getConfiguration().setFloat(entry.getKey(), (Float) value);
      } else if (value instanceof Double) {
        job.getConfiguration().setDouble(entry.getKey(), (Double) value);
      } else if (value instanceof Boolean) {
        job.getConfiguration().setBoolean(entry.getKey(), (Boolean) value);
      } else if (value instanceof List) {
        List<String> casted = GuavaUtils.cast((List<?>) value);
        job.getConfiguration().setStrings(entry.getKey(), casted.toArray(new String[] {}));
      } else {
        new Logger(HadoopPathSpec.class).warn("Invalid type value %s (%s).. ignoring", value, value.getClass());
      }
    }
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

    HadoopPathSpec that = (HadoopPathSpec) o;

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
    if (!Objects.equals(properties, that.properties)) {
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
    return "HadoopPathSpec{" +
           "basePath=" + basePath +
           ", elements=" + elements +
           ", inputFormat=" + inputFormat +
           ", splitSize=" + splitSize +
           ", findRecursive=" + findRecursive +
           ", extractPartition=" + extractPartition +
           ", properties=" + properties +
           '}';
  }
}
